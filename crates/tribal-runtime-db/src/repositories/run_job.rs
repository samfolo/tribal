//! The run-job repository: enqueue, the two-step admission claim, the
//! claim-token-fenced lifecycle, and orphan reclaim.
//!
//! Uses runtime `sqlx::query` rather than the compile-time macros: the workspace
//! sqlx check types every macro query against the control-plane `DATABASE_URL`,
//! and `run_job`/`tenant_slot` live only in this crate's own database — a macro
//! here would fail that check against a schema that does not hold the tables.

use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use sqlx::{Connection, PgConnection, Postgres, Row, Transaction};
use tribal_domain::RunJobId;
use uuid::Uuid;

use crate::RuntimeDbError;

/// The claimed-job column list, interpolated into the claim's select so the
/// query and its row mapping never drift.
const CLAIM_COLUMNS: ClaimColumns = ClaimColumns;

/// Displays the claimed-job column list — the single source [`CLAIM_COLUMNS`].
struct ClaimColumns;

impl fmt::Display for ClaimColumns {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("id, kind, payload, priority")
    }
}

// ---------------------------------------------------------------------------
// State and row shapes
// ---------------------------------------------------------------------------

/// The lifecycle state of a job. A queued job is claimable; a running job holds
/// a tenant slot until it leaves the running state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunJobState {
    /// Awaiting a claim.
    Queued,
    /// Claimed and executing under a live lease and claim token.
    Running,
    /// Parked awaiting a signal or a wake.
    Suspended,
    /// Work done, holds resolving.
    Settling,
    /// Terminal: completed.
    Done,
    /// Terminal: cancelled.
    Cancelled,
}

impl RunJobState {
    fn from_db(value: &str) -> Result<Self, RuntimeDbError> {
        match value {
            "queued" => Ok(RunJobState::Queued),
            "running" => Ok(RunJobState::Running),
            "suspended" => Ok(RunJobState::Suspended),
            "settling" => Ok(RunJobState::Settling),
            "done" => Ok(RunJobState::Done),
            "cancelled" => Ok(RunJobState::Cancelled),
            other => Err(RuntimeDbError::Malformed {
                context: format!("unknown run_job.state '{other}'"),
            }),
        }
    }
}

/// A state a running job may move to. A job leaves the running state exactly
/// once, and its tenant slot is released in the same transaction — so the exit
/// targets are the states that are not themselves running or re-queued (reclaim
/// owns the return to queued).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostRunningState {
    /// Parked awaiting a signal.
    Suspended,
    /// Work done, holds resolving.
    Settling,
    /// Terminal: completed.
    Done,
    /// Terminal: cancelled.
    Cancelled,
}

impl PostRunningState {
    fn as_str(self) -> &'static str {
        match self {
            PostRunningState::Suspended => "suspended",
            PostRunningState::Settling => "settling",
            PostRunningState::Done => "done",
            PostRunningState::Cancelled => "cancelled",
        }
    }
}

/// A job to enqueue. `kind` is one of the closed job kinds the schema enforces.
pub struct NewRunJob {
    /// The tenant the job runs for — an opaque platform account reference.
    pub account_id: String,
    /// The job kind: `consolidate`, `cron`, or `probe`.
    pub kind: String,
    /// The opaque, task-generic payload.
    pub payload: serde_json::Value,
    /// The caller-supplied key that dedups a re-enqueue.
    pub idempotency_key: String,
    /// Ordering within the tenant only.
    pub priority: i16,
}

/// The result of an enqueue: a fresh job, or the existing one a re-enqueue with
/// an already-seen idempotency key resolves to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// A new job was created.
    Enqueued(RunJobId),
    /// The idempotency key was already present; the existing job is returned.
    Deduplicated(RunJobId),
}

impl EnqueueOutcome {
    /// The job id, whichever arm resolved.
    #[must_use]
    pub fn id(self) -> RunJobId {
        match self {
            EnqueueOutcome::Enqueued(id) | EnqueueOutcome::Deduplicated(id) => id,
        }
    }
}

/// A claimed job and the token every subsequent write must present.
pub struct ClaimedJob {
    /// The claimed job's id.
    pub id: RunJobId,
    /// The tenant it runs for.
    pub account_id: String,
    /// The job kind.
    pub kind: String,
    /// The task-generic payload.
    pub payload: serde_json::Value,
    /// Within-tenant priority.
    pub priority: i16,
    /// The token every subsequent write compares against.
    pub claim_token: Uuid,
    /// When the lease expires unless renewed.
    pub lease_expires_at: DateTime<Utc>,
}

/// Whether a claim-token-fenced write applied, or was refused because the lease
/// was lost to a reclaim or a concurrent runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The write applied under a live, matching claim token.
    Applied,
    /// The claim token no longer matches a running job; the write was refused
    /// and nothing changed.
    OwnershipLost,
}

// ---------------------------------------------------------------------------
// Repository
// ---------------------------------------------------------------------------

/// Enqueue, claim, and the claim-token-fenced lifecycle over `run_job`.
#[async_trait]
pub trait RunJobRepository {
    /// Enqueues a job, deduping on `(account_id, idempotency_key)`.
    async fn enqueue(
        &self,
        conn: &mut PgConnection,
        job: NewRunJob,
    ) -> Result<EnqueueOutcome, RuntimeDbError>;

    /// Claims one job under per-tenant admission: the claimer walks candidate
    /// tenants in globally-oldest-queued-job order, takes the first tenant's
    /// slot via the single-row compare-and-swap, and claims that tenant's
    /// highest-priority oldest job — advancing to the next tenant on saturation.
    /// Returns `None` when nothing is claimable. A missing tenant slot is
    /// upserted at `default_cap`.
    async fn claim(
        &self,
        conn: &mut PgConnection,
        default_cap: i32,
        lease: Duration,
    ) -> Result<Option<ClaimedJob>, RuntimeDbError>;

    /// Renews a running job's lease. Refused as `OwnershipLost` if the token no
    /// longer matches a running job.
    async fn heartbeat(
        &self,
        conn: &mut PgConnection,
        id: RunJobId,
        claim_token: Uuid,
        lease: Duration,
    ) -> Result<WriteOutcome, RuntimeDbError>;

    /// Moves a running job out of the running state and releases its tenant slot
    /// in the same transaction. Refused as `OwnershipLost` if the token no
    /// longer matches a running job.
    async fn leave_running(
        &self,
        conn: &mut PgConnection,
        id: RunJobId,
        claim_token: Uuid,
        to: PostRunningState,
    ) -> Result<WriteOutcome, RuntimeDbError>;

    /// Reclaims running jobs whose lease has expired, returning each to queued
    /// and releasing its slot in one transaction. Returns the reclaimed ids.
    async fn reclaim_expired(
        &self,
        conn: &mut PgConnection,
        limit: i64,
    ) -> Result<Vec<RunJobId>, RuntimeDbError>;

    /// Records a durable cancel intent on a job, whatever its state — no claim
    /// token required. Returns whether the job exists.
    async fn request_cancel(
        &self,
        conn: &mut PgConnection,
        id: RunJobId,
    ) -> Result<bool, RuntimeDbError>;

    /// Wakes a suspended job back to `queued`, so a fresh claim resumes it under
    /// a new lease. The resolving signal — a period rollover, a top-up, a backoff
    /// timer — drives this; a job not currently suspended is left untouched (a
    /// suspended job holds no tenant slot, so waking it takes none). Returns
    /// whether it woke.
    async fn wake(&self, conn: &mut PgConnection, id: RunJobId) -> Result<bool, RuntimeDbError>;

    /// Reads a job's current state, if it exists.
    async fn state_of(
        &self,
        conn: &mut PgConnection,
        id: RunJobId,
    ) -> Result<Option<RunJobState>, RuntimeDbError>;
}

/// Postgres implementation of [`RunJobRepository`].
pub struct PgRunJobRepository;

#[async_trait]
impl RunJobRepository for PgRunJobRepository {
    async fn enqueue(
        &self,
        conn: &mut PgConnection,
        job: NewRunJob,
    ) -> Result<EnqueueOutcome, RuntimeDbError> {
        let id = RunJobId::new();
        let inserted: Option<String> = sqlx::query_scalar(
            "INSERT INTO run_job (id, account_id, kind, payload, idempotency_key, priority, state)
             VALUES ($1, $2, $3, $4, $5, $6, 'queued')
             ON CONFLICT (account_id, idempotency_key) DO NOTHING
             RETURNING id",
        )
        .bind(id.to_string())
        .bind(&job.account_id)
        .bind(&job.kind)
        .bind(&job.payload)
        .bind(&job.idempotency_key)
        .bind(job.priority)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "enqueuing a job".to_owned(),
            source,
        })?;

        if inserted.is_some() {
            return Ok(EnqueueOutcome::Enqueued(id));
        }

        // The idempotency key was already present: return the existing job.
        let existing: String = sqlx::query_scalar(
            "SELECT id FROM run_job WHERE account_id = $1 AND idempotency_key = $2",
        )
        .bind(&job.account_id)
        .bind(&job.idempotency_key)
        .fetch_one(&mut *conn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "reading a deduplicated job".to_owned(),
            source,
        })?;
        Ok(EnqueueOutcome::Deduplicated(parse_id(&existing)?))
    }

    async fn claim(
        &self,
        conn: &mut PgConnection,
        default_cap: i32,
        lease: Duration,
    ) -> Result<Option<ClaimedJob>, RuntimeDbError> {
        let mut txn = conn
            .begin()
            .await
            .map_err(|source| RuntimeDbError::QueryFailed {
                context: "opening the claim transaction".to_owned(),
                source,
            })?;

        if let Some(job) = claim_in_txn(&mut txn, default_cap, lease).await? {
            txn.commit()
                .await
                .map_err(|source| RuntimeDbError::QueryFailed {
                    context: "committing a claim".to_owned(),
                    source,
                })?;
            Ok(Some(job))
        } else {
            // Nothing was claimed; discard the walk's idempotent slot upserts.
            txn.rollback()
                .await
                .map_err(|source| RuntimeDbError::QueryFailed {
                    context: "rolling back an empty claim".to_owned(),
                    source,
                })?;
            Ok(None)
        }
    }

    async fn heartbeat(
        &self,
        conn: &mut PgConnection,
        id: RunJobId,
        claim_token: Uuid,
        lease: Duration,
    ) -> Result<WriteOutcome, RuntimeDbError> {
        let renewed: Option<String> = sqlx::query_scalar(
            "UPDATE run_job
             SET lease_expires_at = now() + make_interval(secs => $3::double precision), updated_at = now()
             WHERE id = $1 AND claim_token = $2 AND state = 'running'
             RETURNING id",
        )
        .bind(id.to_string())
        .bind(claim_token)
        .bind(lease_seconds(lease))
        .fetch_optional(&mut *conn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "renewing a lease".to_owned(),
            source,
        })?;
        Ok(write_outcome(renewed.is_some()))
    }

    async fn leave_running(
        &self,
        conn: &mut PgConnection,
        id: RunJobId,
        claim_token: Uuid,
        to: PostRunningState,
    ) -> Result<WriteOutcome, RuntimeDbError> {
        let mut txn = conn
            .begin()
            .await
            .map_err(|source| RuntimeDbError::QueryFailed {
                context: "opening the state-transition transaction".to_owned(),
                source,
            })?;

        // Move the job out of running under the claim-token fence, clearing the
        // lease. A stale runner's token no longer matches, so nothing changes.
        let account_id: Option<String> = sqlx::query_scalar(
            "UPDATE run_job
             SET state = $3, claim_token = NULL, lease_expires_at = NULL, updated_at = now()
             WHERE id = $1 AND claim_token = $2 AND state = 'running'
             RETURNING account_id",
        )
        .bind(id.to_string())
        .bind(claim_token)
        .bind(to.as_str())
        .fetch_optional(&mut *txn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "leaving the running state".to_owned(),
            source,
        })?;

        let Some(account_id) = account_id else {
            txn.rollback()
                .await
                .map_err(|source| RuntimeDbError::QueryFailed {
                    context: "rolling back a lost transition".to_owned(),
                    source,
                })?;
            return Ok(WriteOutcome::OwnershipLost);
        };

        // Release the slot in the same transaction that moved the job out of
        // running, so a running count can never leak.
        release_slot(&mut txn, &account_id).await?;

        txn.commit()
            .await
            .map_err(|source| RuntimeDbError::QueryFailed {
                context: "committing a state transition".to_owned(),
                source,
            })?;
        Ok(WriteOutcome::Applied)
    }

    async fn reclaim_expired(
        &self,
        conn: &mut PgConnection,
        limit: i64,
    ) -> Result<Vec<RunJobId>, RuntimeDbError> {
        // One transaction: requeue every stale running job and release its
        // tenant slot, so the slot is freed in the same transaction that moves
        // the job out of running.
        let rows = sqlx::query(
            "WITH stale AS (
                 SELECT id, account_id FROM run_job
                 WHERE state = 'running' AND lease_expires_at < now()
                 ORDER BY lease_expires_at
                 LIMIT $1
                 FOR UPDATE SKIP LOCKED
             ),
             requeued AS (
                 UPDATE run_job
                 SET state = 'queued', claim_token = NULL, lease_expires_at = NULL, updated_at = now()
                 FROM stale WHERE run_job.id = stale.id
                 RETURNING run_job.id, run_job.account_id
             ),
             released AS (
                 UPDATE tenant_slot ts
                 SET running = ts.running - decr.n
                 FROM (SELECT account_id, count(*)::int AS n FROM requeued GROUP BY account_id) decr
                 WHERE ts.account_id = decr.account_id AND ts.running >= decr.n
             )
             SELECT id FROM requeued",
        )
        .bind(limit)
        .fetch_all(&mut *conn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "reclaiming expired jobs".to_owned(),
            source,
        })?;

        rows.into_iter()
            .map(|row| parse_id(row.get::<String, _>("id").as_str()))
            .collect()
    }

    async fn request_cancel(
        &self,
        conn: &mut PgConnection,
        id: RunJobId,
    ) -> Result<bool, RuntimeDbError> {
        let updated: Option<String> = sqlx::query_scalar(
            "UPDATE run_job SET cancel_requested = TRUE, updated_at = now()
             WHERE id = $1 RETURNING id",
        )
        .bind(id.to_string())
        .fetch_optional(&mut *conn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "recording a cancel intent".to_owned(),
            source,
        })?;
        Ok(updated.is_some())
    }

    async fn wake(&self, conn: &mut PgConnection, id: RunJobId) -> Result<bool, RuntimeDbError> {
        let woken: Option<String> = sqlx::query_scalar(
            "UPDATE run_job SET state = 'queued', updated_at = now()
             WHERE id = $1 AND state = 'suspended'
             RETURNING id",
        )
        .bind(id.to_string())
        .fetch_optional(&mut *conn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "waking a suspended job".to_owned(),
            source,
        })?;
        Ok(woken.is_some())
    }

    async fn state_of(
        &self,
        conn: &mut PgConnection,
        id: RunJobId,
    ) -> Result<Option<RunJobState>, RuntimeDbError> {
        let state: Option<String> = sqlx::query_scalar("SELECT state FROM run_job WHERE id = $1")
            .bind(id.to_string())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|source| RuntimeDbError::QueryFailed {
                context: "reading a job state".to_owned(),
                source,
            })?;
        state.map(|value| RunJobState::from_db(&value)).transpose()
    }
}

// ---------------------------------------------------------------------------
// Admission
// ---------------------------------------------------------------------------

/// The tenant-walk-by-age admission loop, inside one transaction. Candidate
/// tenants are visited in globally-oldest-queued-job order; the first tenant
/// with slot headroom whose job can be locked yields the claim.
async fn claim_in_txn(
    txn: &mut Transaction<'_, Postgres>,
    default_cap: i32,
    lease: Duration,
) -> Result<Option<ClaimedJob>, RuntimeDbError> {
    let mut exhausted: Vec<String> = Vec::new();

    loop {
        // The tenant of the globally-oldest queued job not yet ruled out.
        let candidate: Option<String> = sqlx::query_scalar(
            "SELECT account_id FROM run_job
             WHERE state = 'queued' AND NOT (account_id = ANY($1::text[]))
             ORDER BY created_at
             LIMIT 1",
        )
        .bind(&exhausted)
        .fetch_optional(&mut **txn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "selecting a candidate tenant".to_owned(),
            source,
        })?;

        let Some(tenant) = candidate else {
            return Ok(None);
        };

        // A missing slot admits at the conservative default rather than refusing.
        sqlx::query(
            "INSERT INTO tenant_slot (account_id, running, cap) VALUES ($1, 0, $2)
             ON CONFLICT (account_id) DO NOTHING",
        )
        .bind(&tenant)
        .bind(default_cap)
        .execute(&mut **txn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "ensuring a tenant slot".to_owned(),
            source,
        })?;

        // The single-row compare-and-swap: over-admission is unrepresentable.
        let took_slot: Option<i32> = sqlx::query_scalar(
            "UPDATE tenant_slot SET running = running + 1
             WHERE account_id = $1 AND running < cap
             RETURNING running",
        )
        .bind(&tenant)
        .fetch_optional(&mut **txn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "taking a tenant slot".to_owned(),
            source,
        })?;

        if took_slot.is_none() {
            exhausted.push(tenant);
            continue;
        }

        // The tenant's highest-priority oldest queued job, skip-locked.
        let job = sqlx::query(&format!(
            "SELECT {CLAIM_COLUMNS} FROM run_job
             WHERE state = 'queued' AND account_id = $1
             ORDER BY priority DESC, created_at
             FOR UPDATE SKIP LOCKED
             LIMIT 1"
        ))
        .bind(&tenant)
        .fetch_optional(&mut **txn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "locking the tenant's oldest job".to_owned(),
            source,
        })?;

        let Some(job) = job else {
            // A rival claimer took this tenant's last queued job between the
            // candidate read and the lock: release the slot and move on.
            release_slot(txn, &tenant).await?;
            exhausted.push(tenant);
            continue;
        };

        let job_id: String = job.get("id");
        let claim_token = Uuid::new_v4();
        let lease_expires_at: DateTime<Utc> = sqlx::query_scalar(
            "UPDATE run_job
             SET state = 'running', claim_token = $2,
                 lease_expires_at = now() + make_interval(secs => $3::double precision), updated_at = now()
             WHERE id = $1
             RETURNING lease_expires_at",
        )
        .bind(&job_id)
        .bind(claim_token)
        .bind(lease_seconds(lease))
        .fetch_one(&mut **txn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "marking a job running".to_owned(),
            source,
        })?;

        return Ok(Some(ClaimedJob {
            id: parse_id(&job_id)?,
            account_id: tenant,
            kind: job.get("kind"),
            payload: job.get("payload"),
            priority: job.get("priority"),
            claim_token,
            lease_expires_at,
        }));
    }
}

/// Decrements a tenant's running count, never below zero.
async fn release_slot(
    txn: &mut Transaction<'_, Postgres>,
    account_id: &str,
) -> Result<(), RuntimeDbError> {
    sqlx::query(
        "UPDATE tenant_slot SET running = running - 1 WHERE account_id = $1 AND running > 0",
    )
    .bind(account_id)
    .execute(&mut **txn)
    .await
    .map_err(|source| RuntimeDbError::QueryFailed {
        context: "releasing a tenant slot".to_owned(),
        source,
    })?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn write_outcome(applied: bool) -> WriteOutcome {
    if applied {
        WriteOutcome::Applied
    } else {
        WriteOutcome::OwnershipLost
    }
}

/// The lease duration as whole seconds, cast to `double precision` in the query
/// for `make_interval`.
fn lease_seconds(lease: Duration) -> i64 {
    lease.num_seconds()
}

fn parse_id(raw: &str) -> Result<RunJobId, RuntimeDbError> {
    raw.parse().map_err(|_| RuntimeDbError::Malformed {
        context: format!("run_job id '{raw}'"),
    })
}
