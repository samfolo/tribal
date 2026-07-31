//! The manager's local graph-transition barrier and the one authoritative
//! transition assessment.
//!
//! The barrier's local half lives here as a typed occupancy: a pending
//! transition excludes new manager-owned migrations, runtime lifecycle
//! operations, and sibling transitions, each refused with the pending
//! transition's identity before any command or effect begins. The rule is
//! asymmetric by design — migration and lifecycle admissions exclude only a
//! transition, never each other, so the lifecycle owner keeps adjudicating
//! lifecycle-versus-lifecycle exactly as it does today.

use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use sqlx::{Connection as _, PgConnection};
use tribal_db::{
    AdvisoryLockRepository, GraphIdentityRepository, PgAdvisoryLockRepository,
    PgGraphIdentityRepository, PgReindexRunRepository, PgReindexTaskRepository,
    ReindexRunRepository, ReindexTaskRepository, advisory_locks,
};
use tribal_domain::{ReindexRunState, StorageTransitionId};
use tribal_wire::management::{
    BlockingResolution, ConfigRevision, DatabaseTargetReceipt, DatabaseTargetState, GraphActivity,
    GraphActivityKind, GraphActivityStatus, Revisioned, RuntimeIdentity, SecretLiteral,
    StorageSwitchAbortRequest, StorageSwitchAbortResult, StorageSwitchContinueRequest,
    StorageSwitchContinueResult, StorageSwitchForceStopRequest, StorageSwitchForceStopResult,
    StorageSwitchRequest, StorageSwitchResult, StorageTransitionAssessment,
    StorageTransitionVerdict,
};

use super::{
    super::{
        lifecycle::{
            LifecycleController, TransitionObserveOutcome, TransitionSource, TransitionStopOutcome,
        },
        product::database_endpoint,
    },
    database::{
        CANDIDATE_IO_WINDOW, DatabaseAccess, DatabaseAccessError, inspect_target_state,
        parse_candidate_url,
    },
    operation::OperationContext,
};
use crate::management::observed_at_unix_ms;

/// The manager's observation margin past the runtime's graceful deadline.
const TRANSITION_OBSERVATION_MARGIN: Duration = Duration::from_secs(5);
/// One bound for every lease operation — connect, lock acquisition, and the
/// custody proof. A stalled peer must settle as a typed refusal or a custody
/// loss well inside the management request budget, never hang the call.
const LEASE_IO_WINDOW: Duration = CANDIDATE_IO_WINDOW;
/// The whole request's budget: every bounded step a transition operation
/// takes — the leases, the reinspection, the custody proofs, and the
/// observation window itself — is drawn from this one clock, so the typed
/// answer always reaches the caller inside the transport's request timeout.
const TRANSITION_REQUEST_BUDGET: Duration = Duration::from_secs(90);
/// What a lease's failures name in the error chain.
const LEASE_SESSION_CONTEXT: &str = "a dedicated transition lock session";

// ---------------------------------------------------------------------------
// Gate
// ---------------------------------------------------------------------------

/// The shared local barrier and assessment service; one instance per manager.
#[derive(Clone, Debug)]
pub(in crate::management) struct StorageTransitionGate {
    occupancy: Arc<Mutex<Occupancy>>,
    pending: Arc<tokio::sync::Mutex<Option<PendingTransition>>>,
}

// ---------------------------------------------------------------------------
// Pending transition and lock leases
// ---------------------------------------------------------------------------

/// One manager-held transition: its identity, the recorded source posture,
/// the reinspected target, and the two lock leases whose live custody every
/// quiescence answer is fenced on.
struct PendingTransition {
    transition_id: StorageTransitionId,
    source: TransitionSource,
    target_url: SecretLiteral,
    graceful: Duration,
    source_lease: TransitionLockLease,
    target_lease: TransitionLockLease,
}

impl PendingTransition {
    /// Whether this transition was admitted against exactly this runtime.
    fn records_runtime(&self, runtime: &RuntimeIdentity) -> bool {
        matches!(&self.source, TransitionSource::AttachedRuntime(recorded) if recorded == runtime)
    }

    /// Proves both leases still hold. Every answer that asserts custody —
    /// the parked ones that claim it is retained as much as the quiescent
    /// ones that claim it may now be released — passes through here, so no
    /// outcome can describe a barrier that Postgres has already dropped.
    async fn prove_custody(&mut self) -> Result<(), DatabaseAccessError> {
        if self.source_lease.prove().await && self.target_lease.prove().await {
            Ok(())
        } else {
            Err(custody_lost_error())
        }
    }
}

impl std::fmt::Debug for PendingTransition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingTransition")
            .field("transition_id", &self.transition_id)
            .finish_non_exhaustive()
    }
}

/// A dedicated non-pool session holding one exclusive advisory lock; the lock
/// lives and dies with this connection, so dropping the lease releases it.
struct TransitionLockLease {
    connection: PgConnection,
}

impl TransitionLockLease {
    /// Connects a dedicated session and try-acquires the exclusive lock.
    /// `Ok(None)` is contention; the session closes with the lease.
    async fn acquire(url: &str, lock_id: i64) -> Result<Option<Self>, DatabaseAccessError> {
        let options = parse_candidate_url(url).map_err(|_| {
            lease_failure(
                LEASE_SESSION_CONTEXT,
                sqlx::Error::Configuration("the lock session URL was refused".into()),
            )
        })?;
        tokio::time::timeout(LEASE_IO_WINDOW, async {
            let mut connection = PgConnection::connect_with(&options)
                .await
                .map_err(|source| lease_failure(LEASE_SESSION_CONTEXT, source))?;
            let granted = match PgAdvisoryLockRepository
                .try_acquire_exclusive(&mut connection, lock_id)
                .await
            {
                Ok(granted) => granted,
                Err(source) => {
                    let _ = connection.close().await;
                    return Err(DatabaseAccessError::Connection { source });
                }
            };
            if granted {
                Ok(Some(Self { connection }))
            } else {
                let _ = connection.close().await;
                Ok(None)
            }
        })
        .await
        .unwrap_or_else(|_elapsed| {
            Err(lease_failure(
                "the transition lock session did not settle",
                sqlx::Error::PoolTimedOut,
            ))
        })
    }

    /// Proves the session still holds custody: the lock is session-bound, so
    /// a live round-trip is the evidence no quiescence answer may skip.
    async fn prove(&mut self) -> bool {
        // A stalled session is not a live one: an unanswered probe fails
        // custody rather than holding the caller past its budget.
        tokio::time::timeout(
            LEASE_IO_WINDOW,
            PgAdvisoryLockRepository.ping(&mut self.connection),
        )
        .await
        .is_ok_and(|result| result.is_ok())
    }
}

/// Why a switch attempt could not take the transition slot.
#[derive(Debug)]
enum TransitionRefusal {
    /// A sibling transition already holds it.
    Sibling(StorageTransitionId),
    /// Admitted migration or lifecycle work holds the local barrier; the
    /// kinds are captured under the same lock so the blocked answer cannot
    /// contradict the refusal.
    LocalActivity { migration: bool, lifecycle: bool },
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Forces the activities that refused admission into the assessment, so a
/// refusal observed under the barrier is never reported alongside a `Ready`
/// verdict computed after the blocker settled.
fn force_refusing_activities(
    assessment: &mut StorageTransitionAssessment,
    migration: bool,
    lifecycle: bool,
) {
    if migration
        && !assessment
            .activities
            .iter()
            .any(|activity| activity.kind == GraphActivityKind::DatabaseMigration)
    {
        assessment.activities.push(GraphActivity {
            kind: GraphActivityKind::DatabaseMigration,
            status: GraphActivityStatus::Running,
            resolution: BlockingResolution::Wait,
        });
    }
    if lifecycle
        && !assessment
            .activities
            .iter()
            .any(|activity| activity.kind == GraphActivityKind::RuntimeLifecycle)
    {
        assessment.activities.push(GraphActivity {
            kind: GraphActivityKind::RuntimeLifecycle,
            status: GraphActivityStatus::Running,
            resolution: BlockingResolution::WaitForLifecycle,
        });
    }
    assessment.verdict = StorageTransitionVerdict::Blocked;
}

/// What is left of the request's budget for one observation, after the work
/// already done and with room kept for the custody proofs and reinspection
/// that follow it. Never longer than the source's own graceful deadline.
fn observation_window(started: tokio::time::Instant, graceful: Duration) -> Duration {
    let spent = started.elapsed();
    let reserved = LEASE_IO_WINDOW * 3;
    TRANSITION_REQUEST_BUDGET
        .saturating_sub(spent)
        .saturating_sub(reserved)
        .min(graceful)
}

/// The reinspection receipt for one target, at this instant.
async fn target_receipt(raw: &str) -> DatabaseTargetReceipt {
    DatabaseTargetReceipt {
        endpoint: database_endpoint(raw),
        state: inspect_target_state(raw).await,
        observed_at_unix_ms: observed_at_unix_ms(),
    }
}

/// Lost custody of a transition lock session mid-operation; the answer is an
/// error, never a quiescence result.
/// Every way a lock lease can fail its caller: the session would not open,
/// did not settle in its window, or no longer holds what it promised. All
/// three are the same answer — this manager cannot act on that lock.
fn lease_failure(context: &str, source: sqlx::Error) -> DatabaseAccessError {
    DatabaseAccessError::Connection {
        source: tribal_db::DbError::QueryFailed {
            context: context.to_owned(),
            source,
        },
    }
}

/// The lease no longer holds what it promised; no quiescence may be reported
/// past this point.
fn custody_lost_error() -> DatabaseAccessError {
    lease_failure("transition lock custody was lost", sqlx::Error::PoolClosed)
}

/// What currently occupies the local barrier.
#[derive(Debug, Default)]
struct Occupancy {
    transition: Option<StorageTransitionId>,
    lifecycle_count: usize,
    migration_count: usize,
}

/// What a released guard gives back.
#[derive(Debug, Clone, Copy)]
enum AdmittedKind {
    Lifecycle,
    Migration,
    Transition,
}

/// One admitted occupancy, held until its operation's terminal reply;
/// dropping it frees the slot. A transition that parks past its graceful
/// window keeps the barrier instead, through [`LocalAdmissionGuard::retain`].
#[derive(Debug)]
pub(in crate::management) struct LocalAdmissionGuard {
    occupancy: Arc<Mutex<Occupancy>>,
    kind: AdmittedKind,
    release: bool,
}

impl LocalAdmissionGuard {
    /// Keeps the occupancy past this guard's life: the pending transition
    /// owns the barrier now, and only its own settlement frees it.
    fn retain(mut self) {
        self.release = false;
    }
}

impl Drop for LocalAdmissionGuard {
    fn drop(&mut self) {
        if !self.release {
            return;
        }
        let mut occupancy = lock(&self.occupancy);
        match self.kind {
            AdmittedKind::Lifecycle => {
                occupancy.lifecycle_count = occupancy.lifecycle_count.saturating_sub(1);
            }
            AdmittedKind::Migration => {
                occupancy.migration_count = occupancy.migration_count.saturating_sub(1);
            }
            AdmittedKind::Transition => occupancy.transition = None,
        }
    }
}

// ---------------------------------------------------------------------------
// The barrier and the transition protocol
// ---------------------------------------------------------------------------

impl StorageTransitionGate {
    pub(in crate::management) fn new() -> Self {
        Self {
            occupancy: Arc::new(Mutex::new(Occupancy::default())),
            pending: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Admits a manager-owned migration, or reports the pending transition
    /// that refuses it.
    pub(in crate::management) fn admit_migration(
        &self,
    ) -> Result<LocalAdmissionGuard, StorageTransitionId> {
        let mut occupancy = lock(&self.occupancy);
        if let Some(transition_id) = occupancy.transition {
            return Err(transition_id);
        }
        occupancy.migration_count += 1;
        Ok(LocalAdmissionGuard {
            occupancy: Arc::clone(&self.occupancy),
            kind: AdmittedKind::Migration,
            release: true,
        })
    }

    /// Admits a runtime lifecycle operation, or reports the pending
    /// transition that refuses it. Concurrent lifecycle operations all admit;
    /// the lifecycle owner adjudicates between them.
    pub(in crate::management) fn admit_lifecycle(
        &self,
    ) -> Result<LocalAdmissionGuard, StorageTransitionId> {
        let mut occupancy = lock(&self.occupancy);
        if let Some(transition_id) = occupancy.transition {
            return Err(transition_id);
        }
        occupancy.lifecycle_count += 1;
        Ok(LocalAdmissionGuard {
            occupancy: Arc::clone(&self.occupancy),
            kind: AdmittedKind::Lifecycle,
            release: true,
        })
    }

    /// Assembles the transition assessment without taking any barrier: the
    /// durable live reindex run (with its retry wait), this manager's
    /// admitted migration, and its live lifecycle operations.
    pub(in crate::management) async fn assess(
        &self,
        operation: &OperationContext,
        database: &DatabaseAccess,
        expected_revision: Option<&ConfigRevision>,
    ) -> Result<Revisioned<StorageTransitionAssessment>, DatabaseAccessError> {
        self.assess_excluding(operation, database, expected_revision, None)
            .await
    }

    /// The assessment, with the caller's own pending transition excluded —
    /// the switch's under-barrier recompute must not block on itself, while
    /// every other reader sees a pending transition as lifecycle occupancy.
    async fn assess_excluding(
        &self,
        operation: &OperationContext,
        database: &DatabaseAccess,
        expected_revision: Option<&ConfigRevision>,
        exclude: Option<StorageTransitionId>,
    ) -> Result<Revisioned<StorageTransitionAssessment>, DatabaseAccessError> {
        let session = database.session(operation, expected_revision).await?;
        let mut conn = session
            .pool
            .acquire()
            .await
            .map_err(assessment_connection_error)?;
        let source_graph_id = PgGraphIdentityRepository
            .get(&mut conn)
            .await
            .map_err(|source| DatabaseAccessError::Connection { source })?;

        let mut activities = Vec::new();
        if let Some(run) = PgReindexRunRepository
            .find_live(&mut conn)
            .await
            .map_err(|source| DatabaseAccessError::Connection { source })?
        {
            let status = if run.state() == ReindexRunState::Queued {
                GraphActivityStatus::Queued
            } else {
                match PgReindexTaskRepository
                    .retry_wait(&mut conn, run.id())
                    .await
                    .map_err(|source| DatabaseAccessError::Connection { source })?
                {
                    Some(resume_at) => GraphActivityStatus::WaitingForRetry {
                        resume_at_unix_ms: u64::try_from(resume_at.timestamp_millis()).ok(),
                    },
                    None => GraphActivityStatus::Running,
                }
            };
            activities.push(GraphActivity {
                kind: GraphActivityKind::EmbeddingReindex { run_id: run.id() },
                status,
                resolution: BlockingResolution::WaitOrCancelReindex,
            });
        }

        let (migrations, lifecycle, pending_transition) = {
            let occupancy = lock(&self.occupancy);
            let pending = occupancy
                .transition
                .filter(|id| exclude.is_none_or(|own| own != *id));
            (
                occupancy.migration_count,
                occupancy.lifecycle_count,
                pending,
            )
        };
        if migrations > 0 {
            activities.push(GraphActivity {
                kind: GraphActivityKind::DatabaseMigration,
                status: GraphActivityStatus::Running,
                resolution: BlockingResolution::Wait,
            });
        }
        if lifecycle > 0 || pending_transition.is_some() {
            activities.push(GraphActivity {
                kind: GraphActivityKind::RuntimeLifecycle,
                status: GraphActivityStatus::Running,
                resolution: BlockingResolution::WaitForLifecycle,
            });
        }

        let verdict = if activities.is_empty() {
            StorageTransitionVerdict::Ready
        } else {
            StorageTransitionVerdict::Blocked
        };
        Ok(session.revisioned(StorageTransitionAssessment {
            source_graph_id,
            verdict,
            activities,
        }))
    }

    /// Drives one switch attempt end to end, in the design's order: local
    /// admission, the runtime fence, the source identity, the target
    /// reservation, the source transition lock, reinspection and
    /// reassessment under both leases, then the confirmation-required stop.
    pub(in crate::management) async fn switch(
        &self,
        operation: &OperationContext,
        database: &DatabaseAccess,
        lifecycle: &LifecycleController,
        request: StorageSwitchRequest,
    ) -> Result<StorageSwitchResult, DatabaseAccessError> {
        let started = tokio::time::Instant::now();
        let transition_id = StorageTransitionId::new();
        let admission = match self.admit_transition(transition_id) {
            Ok(admission) => admission,
            Err(TransitionRefusal::Sibling(transition_id)) => {
                return Ok(StorageSwitchResult::TransitionInProgress { transition_id });
            }
            Err(TransitionRefusal::LocalActivity {
                migration,
                lifecycle,
            }) => {
                let mut assessment = self.assess(operation, database, None).await?.value;
                force_refusing_activities(&mut assessment, migration, lifecycle);
                return Ok(StorageSwitchResult::Blocked { assessment });
            }
        };

        let mut pending_slot = self.pending.lock().await;
        let session = database
            .session(operation, Some(&request.expected_revision))
            .await?;

        // The runtime fence comes before any database work, so a changed or
        // absent runtime answers `RuntimeChanged` rather than surfacing as
        // contention or an unready target. The owner re-checks it
        // authoritatively before it sends any signal.
        let attached = lifecycle.attached_runtime();
        if attached != request.expected_runtime {
            return Ok(StorageSwitchResult::RuntimeChanged { observed: attached });
        }
        let source = request
            .expected_runtime
            .clone()
            .map_or(TransitionSource::CleanNoRuntime, |expected| {
                TransitionSource::AttachedRuntime(expected)
            });

        // The source identity, before any dedicated connection opens.
        let mut conn =
            session
                .pool
                .acquire()
                .await
                .map_err(|source| DatabaseAccessError::Connection {
                    source: tribal_db::DbError::QueryFailed {
                        context: "acquiring the source identity connection".to_owned(),
                        source,
                    },
                })?;
        let observed_source = PgGraphIdentityRepository
            .get(&mut conn)
            .await
            .map_err(|source| DatabaseAccessError::Connection { source })?;
        drop(conn);
        if observed_source != request.source_graph_id {
            return Ok(StorageSwitchResult::SourceChanged {
                observed_graph_id: observed_source,
            });
        }

        // Target reservation before the source transition lock.
        let target_url = request.target_url.expose_secret().to_owned();
        let target_lease = match TransitionLockLease::acquire(
            &target_url,
            advisory_locks::TARGET_MIGRATION_RESERVATION,
        )
        .await
        {
            Ok(Some(lease)) => lease,
            Ok(None) => return Ok(StorageSwitchResult::AdmissionContended),
            // A candidate that admits no session is an unready target, not a
            // management error; the receipt classifies it.
            Err(_) => {
                return Ok(StorageSwitchResult::TargetNotReady {
                    target: target_receipt(&target_url).await,
                });
            }
        };
        let source_url = session.config.database.url.clone();
        let Some(source_lease) =
            TransitionLockLease::acquire(&source_url, advisory_locks::GRAPH_TRANSITION).await?
        else {
            return Ok(StorageSwitchResult::AdmissionContended);
        };

        // Reinspect the target and recompute the assessment under both leases.
        let target_receipt = target_receipt(&target_url).await;
        match &target_receipt.state {
            DatabaseTargetState::Ready { graph_id } if *graph_id == request.target_graph_id => {}
            DatabaseTargetState::Ready { graph_id } => {
                return Ok(StorageSwitchResult::TargetChanged {
                    observed_graph_id: *graph_id,
                });
            }
            _ => {
                return Ok(StorageSwitchResult::TargetNotReady {
                    target: target_receipt,
                });
            }
        }
        let assessment = self
            .assess_excluding(
                operation,
                database,
                Some(&request.expected_revision),
                Some(transition_id),
            )
            .await?
            .value;
        if assessment.verdict == StorageTransitionVerdict::Blocked {
            return Ok(StorageSwitchResult::Blocked { assessment });
        }

        // The confirmation-required stop, fenced on live lock custody.
        // The source's own drain contract, margin included; each request
        // derives its observation from this and from what its budget allows.
        let graceful = Duration::from_millis(session.config.server.shutdown_deadline_ms)
            + TRANSITION_OBSERVATION_MARGIN;
        let mut pending = PendingTransition {
            transition_id,
            source: source.clone(),
            target_url: request.target_url,
            graceful,
            source_lease,
            target_lease,
        };
        let outcome = lifecycle
            .transition_stop_for(
                operation,
                transition_id,
                source,
                observation_window(started, graceful),
            )
            .await?;
        match outcome {
            Some(TransitionStopOutcome::SourceQuiesced) => {
                pending.prove_custody().await?;
                Ok(StorageSwitchResult::SourceQuiesced {
                    transition_id,
                    target: target_receipt,
                })
            }
            Some(TransitionStopOutcome::StopTimedOut { runtime }) => {
                // Parking claims the barrier is retained, so prove it is.
                pending.prove_custody().await?;
                // The parked transition owns the barrier from here.
                admission.retain();
                *pending_slot = Some(pending);
                Ok(StorageSwitchResult::StopTimedOut {
                    transition_id,
                    runtime,
                })
            }
            Some(TransitionStopOutcome::RuntimeChanged { observed }) => {
                Ok(StorageSwitchResult::RuntimeChanged { observed })
            }
            None => Err(custody_lost_error()),
        }
    }

    /// Continues a timed-out transition: another bounded observation of the
    /// same already-signalled process, with no further signal.
    pub(in crate::management) async fn continue_switch(
        &self,
        operation: &OperationContext,
        lifecycle: &LifecycleController,
        request: StorageSwitchContinueRequest,
    ) -> Result<StorageSwitchContinueResult, DatabaseAccessError> {
        let started = tokio::time::Instant::now();
        let mut pending_slot = self.pending.lock().await;
        let Some(pending) = pending_slot.as_mut() else {
            return Ok(StorageSwitchContinueResult::NoPendingTransition);
        };
        if pending.transition_id != request.transition_id {
            return Ok(StorageSwitchContinueResult::NoPendingTransition);
        }
        // The recorded runtime is the fence: once the child has exited the
        // owner answers any identity, so a mismatch must be refused here.
        if !pending.records_runtime(&request.expected_runtime) {
            return Ok(StorageSwitchContinueResult::RuntimeChanged);
        }
        let outcome = lifecycle
            .transition_observe_for(
                operation,
                request.expected_runtime,
                observation_window(started, pending.graceful),
                false,
            )
            .await?;
        match outcome {
            Some(TransitionObserveOutcome::Exited) => {
                let transition_id = pending.transition_id;
                let target = target_receipt(pending.target_url.expose_secret()).await;
                pending.prove_custody().await?;
                self.release_pending(&mut pending_slot);
                Ok(StorageSwitchContinueResult::SourceQuiesced {
                    transition_id,
                    target,
                })
            }
            Some(TransitionObserveOutcome::StillPresent { runtime, .. }) => {
                pending.prove_custody().await?;
                Ok(StorageSwitchContinueResult::StopTimedOut {
                    transition_id: pending.transition_id,
                    runtime,
                })
            }
            Some(
                TransitionObserveOutcome::RuntimeChanged { .. }
                | TransitionObserveOutcome::NotStopping,
            ) => Ok(StorageSwitchContinueResult::RuntimeChanged),
            None => Err(custody_lost_error()),
        }
    }

    /// Force-stops a timed-out transition: the kill is sent under the same
    /// identity fence and quiescence still waits for exact exit.
    pub(in crate::management) async fn force_stop(
        &self,
        operation: &OperationContext,
        lifecycle: &LifecycleController,
        request: StorageSwitchForceStopRequest,
    ) -> Result<StorageSwitchForceStopResult, DatabaseAccessError> {
        let started = tokio::time::Instant::now();
        let mut pending_slot = self.pending.lock().await;
        let Some(pending) = pending_slot.as_mut() else {
            return Ok(StorageSwitchForceStopResult::NoPendingTransition);
        };
        if pending.transition_id != request.transition_id {
            return Ok(StorageSwitchForceStopResult::NoPendingTransition);
        }
        if !pending.records_runtime(&request.expected_runtime) {
            return Ok(StorageSwitchForceStopResult::RuntimeChanged);
        }
        // The kill is irreversible: prove both barriers still hold before
        // sending it.
        pending.prove_custody().await?;
        let outcome = lifecycle
            .transition_observe_for(
                operation,
                request.expected_runtime,
                observation_window(started, pending.graceful),
                true,
            )
            .await?;
        match outcome {
            Some(TransitionObserveOutcome::Exited) => {
                let transition_id = pending.transition_id;
                let target = target_receipt(pending.target_url.expose_secret()).await;
                pending.prove_custody().await?;
                self.release_pending(&mut pending_slot);
                Ok(StorageSwitchForceStopResult::SourceQuiesced {
                    transition_id,
                    target,
                })
            }
            Some(TransitionObserveOutcome::StillPresent { failure, .. }) => {
                pending.prove_custody().await?;
                Ok(StorageSwitchForceStopResult::RuntimeStillPresent { failure })
            }
            Some(
                TransitionObserveOutcome::RuntimeChanged { .. }
                | TransitionObserveOutcome::NotStopping,
            ) => Ok(StorageSwitchForceStopResult::RuntimeChanged),
            None => Err(custody_lost_error()),
        }
    }

    /// Aborts the transition, restoring the source's pre-transition posture:
    /// a clean no-runtime origin stays stopped, an attached origin restarts
    /// only after its exact exit, and custody is retained while recovery is
    /// not yet safe.
    pub(in crate::management) async fn abort(
        &self,
        operation: &OperationContext,
        lifecycle: &LifecycleController,
        request: StorageSwitchAbortRequest,
    ) -> Result<StorageSwitchAbortResult, DatabaseAccessError> {
        let started = tokio::time::Instant::now();
        let mut pending_slot = self.pending.lock().await;
        let Some(pending) = pending_slot.as_mut() else {
            return Ok(StorageSwitchAbortResult::NoPendingTransition);
        };
        if pending.transition_id != request.transition_id {
            return Ok(StorageSwitchAbortResult::NoPendingTransition);
        }
        let recorded = match &pending.source {
            TransitionSource::CleanNoRuntime => {
                self.release_pending(&mut pending_slot);
                return Ok(StorageSwitchAbortResult::SourceRemainedStopped);
            }
            TransitionSource::AttachedRuntime(runtime) => runtime.clone(),
        };
        let outcome = lifecycle
            .transition_observe_for(
                operation,
                recorded,
                observation_window(started, pending.graceful),
                false,
            )
            .await?;
        match outcome {
            Some(TransitionObserveOutcome::Exited) => {
                let start = lifecycle.start_for(operation).await?;
                self.release_pending(&mut pending_slot);
                match start {
                    Some(tribal_wire::management::RuntimeStartResult::Started { .. }) => {
                        Ok(StorageSwitchAbortResult::SourceRestarted)
                    }
                    Some(result) => Ok(StorageSwitchAbortResult::SourceStillStopped {
                        result: Box::new(result),
                    }),
                    None => Err(custody_lost_error()),
                }
            }
            Some(TransitionObserveOutcome::StillPresent { runtime, .. }) => {
                pending.prove_custody().await?;
                Ok(StorageSwitchAbortResult::SourceRecoveryPending { runtime })
            }
            Some(TransitionObserveOutcome::RuntimeChanged { observed }) => {
                pending.prove_custody().await?;
                Ok(StorageSwitchAbortResult::RuntimeChanged { observed })
            }
            Some(TransitionObserveOutcome::NotStopping) => {
                Ok(StorageSwitchAbortResult::RuntimeChanged { observed: None })
            }
            None => Err(custody_lost_error()),
        }
    }

    /// Reserves the transition slot, or names what refuses it. The guard
    /// frees the slot on every return path until the transition parks.
    fn admit_transition(
        &self,
        transition_id: StorageTransitionId,
    ) -> Result<LocalAdmissionGuard, TransitionRefusal> {
        let mut occupancy = lock(&self.occupancy);
        if let Some(existing) = occupancy.transition {
            return Err(TransitionRefusal::Sibling(existing));
        }
        if occupancy.lifecycle_count > 0 || occupancy.migration_count > 0 {
            return Err(TransitionRefusal::LocalActivity {
                migration: occupancy.migration_count > 0,
                lifecycle: occupancy.lifecycle_count > 0,
            });
        }
        occupancy.transition = Some(transition_id);
        Ok(LocalAdmissionGuard {
            occupancy: Arc::clone(&self.occupancy),
            kind: AdmittedKind::Transition,
            release: true,
        })
    }

    /// Drops the pending record — closing both lease sessions releases their
    /// locks — and clears the transition occupancy.
    fn release_pending(&self, slot: &mut Option<PendingTransition>) {
        *slot = None;
        lock(&self.occupancy).transition = None;
    }

    /// Installs a pending record over real lock leases, so the follow-up
    /// operations can be exercised without driving a full switch.
    #[cfg(test)]
    async fn install_pending_for_test(
        &self,
        transition_id: StorageTransitionId,
        source: TransitionSource,
        source_url: &str,
        target_url: &str,
    ) {
        let source_lease =
            TransitionLockLease::acquire(source_url, advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("source lease connects")
                .expect("source lock uncontended");
        let target_lease =
            TransitionLockLease::acquire(target_url, advisory_locks::TARGET_MIGRATION_RESERVATION)
                .await
                .expect("target lease connects")
                .expect("target lock uncontended");
        lock(&self.occupancy).transition = Some(transition_id);
        *self.pending.lock().await = Some(PendingTransition {
            transition_id,
            source,
            target_url: SecretLiteral::try_from(target_url.to_owned()).expect("target url"),
            graceful: Duration::from_secs(1),
            source_lease,
            target_lease,
        });
    }

    #[cfg(test)]
    pub(in crate::management) fn occupy_for_test(&self, transition_id: StorageTransitionId) {
        lock(&self.occupancy).transition = Some(transition_id);
    }

    #[cfg(test)]
    pub(in crate::management) fn release_for_test(&self) {
        lock(&self.occupancy).transition = None;
    }
}

/// A poisoned barrier would fail every admission closed forever; the state is
/// three plain integers, valid under any panic, so recover the guard.
fn lock(occupancy: &Arc<Mutex<Occupancy>>) -> MutexGuard<'_, Occupancy> {
    occupancy
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn assessment_connection_error(source: sqlx::Error) -> DatabaseAccessError {
    DatabaseAccessError::Connection {
        source: tribal_db::DbError::QueryFailed {
            context: "acquiring an assessment connection".to_owned(),
            source,
        },
    }
}

#[cfg(test)]
mod tests {
    use tribal_config::TribalConfig;
    use tribal_db::{NewReindexRun, PgPrincipalRepository, PrincipalRepository};
    use tribal_domain::{GraphId, StorageTransitionId};
    use tribal_test_utils::{TestDb, a_new_principal, ensure_genesis_profile};
    use tribal_wire::management::ConfigDigest;

    use super::*;
    use crate::management::{configuration::ConfigAuthority, worker};

    fn operation() -> OperationContext {
        OperationContext::new(tokio_util::sync::CancellationToken::new())
    }

    fn database_access(
        database_url: &str,
    ) -> (
        tempfile::TempDir,
        DatabaseAccess,
        worker::ConfigWorkerClient,
        worker::ConfigWorkerRuntime,
    ) {
        let temp = tempfile::tempdir().expect("temporary config root");
        let path = temp.path().join("tribal.yaml");
        let config = TribalConfig::minimum_valid(database_url);
        std::fs::write(&path, serde_yaml::to_string(&config).unwrap()).unwrap();
        let (worker, runtime) = worker::spawn(ConfigAuthority::new(path)).unwrap();
        (temp, DatabaseAccess::new(worker.clone()), worker, runtime)
    }

    #[tokio::test]
    async fn test_assessment_is_ready_with_the_source_identity_when_nothing_is_live() {
        let ctx = TestDb::new().await;
        let (_temp, database, _worker, _runtime) = database_access(ctx.database_url());
        let gate = StorageTransitionGate::new();

        let assessment = gate
            .assess(&operation(), &database, None)
            .await
            .expect("assess");

        assert_eq!(assessment.value.verdict, StorageTransitionVerdict::Ready);
        assert!(assessment.value.activities.is_empty());
        assert!(
            assessment
                .value
                .source_graph_id
                .to_string()
                .starts_with("graph_")
        );
    }

    #[tokio::test]
    async fn test_assessment_refuses_a_stale_revision() {
        let ctx = TestDb::new().await;
        let (_temp, database, _worker, _runtime) = database_access(ctx.database_url());
        let gate = StorageTransitionGate::new();
        let stale = ConfigRevision::from_digest(&ConfigDigest::from_bytes(b"stale"));

        let error = gate
            .assess(&operation(), &database, Some(&stale))
            .await
            .expect_err("a stale revision is refused");
        assert!(matches!(
            error,
            DatabaseAccessError::RevisionConflict { .. }
        ));
    }

    #[tokio::test]
    async fn test_assessment_projects_a_live_reindex_and_its_retry_wait() {
        let ctx = TestDb::new().await;
        let (_temp, database, _worker, _runtime) = database_access(ctx.database_url());
        let gate = StorageTransitionGate::new();

        let mut conn = ctx.raw_connection().await.expect("seed connection");
        let principal = PgPrincipalRepository
            .insert(
                &mut conn,
                &a_new_principal()
                    .principal_key("user:assessment".to_owned())
                    .build(),
            )
            .await
            .expect("insert principal");
        let profile = ensure_genesis_profile(&mut conn, "test-model", 768).await;
        let run = PgReindexRunRepository
            .insert(
                &mut conn,
                &NewReindexRun::builder()
                    .target_profile_id(profile.id())
                    .epoch(profile.epoch())
                    .initiated_by_principal_id(principal.id())
                    .build(),
            )
            .await
            .expect("insert run");

        let assessment = gate
            .assess(&operation(), &database, None)
            .await
            .expect("assess")
            .value;
        assert_eq!(assessment.verdict, StorageTransitionVerdict::Blocked);
        assert_eq!(assessment.activities.len(), 1);
        let activity = &assessment.activities[0];
        assert_eq!(
            activity.kind,
            GraphActivityKind::EmbeddingReindex { run_id: run.id() }
        );
        assert_eq!(activity.status, GraphActivityStatus::Queued);
        assert_eq!(activity.resolution, BlockingResolution::WaitOrCancelReindex);
    }

    #[test]
    fn test_the_transition_slot_refuses_and_is_refused_by_local_activity() {
        let gate = StorageTransitionGate::new();

        // Lifecycle-first: the transition is refused while lifecycle work is
        // admitted, and admits once it releases.
        let lifecycle = gate.admit_lifecycle().expect("admit lifecycle");
        assert!(matches!(
            gate.admit_transition(StorageTransitionId::new()),
            Err(TransitionRefusal::LocalActivity { .. })
        ));
        drop(lifecycle);

        // Transition-first: both local kinds are refused with the pending id,
        // and a sibling transition names it.
        let pending = StorageTransitionId::new();
        let held = gate.admit_transition(pending).expect("transition admits");
        assert_eq!(
            gate.admit_migration().expect_err("migration refused"),
            pending
        );
        assert_eq!(
            gate.admit_lifecycle().expect_err("lifecycle refused"),
            pending
        );
        assert!(matches!(
            gate.admit_transition(StorageTransitionId::new()),
            Err(TransitionRefusal::Sibling(existing)) if existing == pending
        ));

        // Dropping the admission frees the slot; retaining it is what a
        // parked transition does instead.
        drop(held);
        drop(
            gate.admit_migration()
                .expect("a dropped admission frees the slot"),
        );
        gate.admit_transition(StorageTransitionId::new())
            .expect("the slot is free")
            .retain();
        assert!(matches!(
            gate.admit_transition(StorageTransitionId::new()),
            Err(TransitionRefusal::Sibling(_))
        ));
        gate.release_for_test();
    }

    /// Everything two switch tests share: a ready source and target, the
    /// gate, and a live controller over the source configuration.
    struct SwitchHarness {
        source: TestDb,
        ready_target: TestDb,
        database: DatabaseAccess,
        revision: ConfigRevision,
        lifecycle: LifecycleController,
        gate: StorageTransitionGate,
        source_id: GraphId,
        target_id: GraphId,
        _config_temp: tempfile::TempDir,
        _authority_temp: tempfile::TempDir,
        _worker_runtime: worker::ConfigWorkerRuntime,
        _lifecycle_runtime: worker::ConfigWorkerRuntime,
        _lifecycle_task: tokio::task::JoinHandle<crate::management::lifecycle::LifecycleExit>,
    }

    async fn a_switch_harness() -> SwitchHarness {
        let source = TestDb::new().await;
        let ready_target = TestDb::new().await;
        let (config_temp, database, worker, worker_runtime) =
            database_access(source.database_url());
        let revision = worker.resolved_snapshot().await.unwrap().revision;
        let (authority_temp, lifecycle, lifecycle_task, lifecycle_runtime) =
            crate::management::lifecycle::controller_for_test(source.database_url()).await;
        let mut conn = source.raw_connection().await.expect("source identity");
        let source_id = PgGraphIdentityRepository
            .get(&mut conn)
            .await
            .expect("source id");
        let mut conn = ready_target
            .raw_connection()
            .await
            .expect("target identity");
        let target_id = PgGraphIdentityRepository
            .get(&mut conn)
            .await
            .expect("target id");
        SwitchHarness {
            source,
            ready_target,
            database,
            revision,
            lifecycle,
            gate: StorageTransitionGate::new(),
            source_id,
            target_id,
            _config_temp: config_temp,
            _authority_temp: authority_temp,
            _worker_runtime: worker_runtime,
            _lifecycle_runtime: lifecycle_runtime,
            _lifecycle_task: lifecycle_task,
        }
    }

    impl SwitchHarness {
        fn request(
            &self,
            source_graph_id: GraphId,
            target_graph_id: GraphId,
            url: &str,
        ) -> StorageSwitchRequest {
            StorageSwitchRequest {
                expected_revision: self.revision.clone(),
                expected_runtime: None,
                source_graph_id,
                target_graph_id,
                target_url: SecretLiteral::try_from(url.to_owned()).expect("target url"),
            }
        }

        async fn switch(&self, request: StorageSwitchRequest) -> StorageSwitchResult {
            self.gate
                .switch(&operation(), &self.database, &self.lifecycle, request)
                .await
                .expect("switch runs")
        }
    }

    #[tokio::test]
    async fn test_switch_checks_the_source_fence_before_the_target_reservation() {
        let h = a_switch_harness().await;
        let bare_target = TestDb::new_unmigrated().await;

        // A changed source returns before the candidate is ever touched: a
        // shared hold on the target reservation would turn any reservation
        // attempt into contention, and the result is still SourceChanged.
        let mut target_probe = h.ready_target.raw_connection().await.expect("probe");
        assert!(
            PgAdvisoryLockRepository
                .try_acquire_shared(
                    &mut target_probe,
                    advisory_locks::TARGET_MIGRATION_RESERVATION
                )
                .await
                .expect("hold shared reservation"),
        );
        let result = h
            .switch(h.request(GraphId::new(), h.target_id, h.ready_target.database_url()))
            .await;
        assert!(
            matches!(result, StorageSwitchResult::SourceChanged { observed_graph_id } if observed_graph_id == h.source_id),
        );
        assert!(
            PgAdvisoryLockRepository
                .release_shared(
                    &mut target_probe,
                    advisory_locks::TARGET_MIGRATION_RESERVATION
                )
                .await
                .expect("release shared reservation"),
        );

        // An unready target ends the attempt with the reinspection receipt.
        let result = h
            .switch(h.request(h.source_id, h.target_id, bare_target.database_url()))
            .await;
        assert!(matches!(
            result,
            StorageSwitchResult::TargetNotReady { target } if matches!(target.state, DatabaseTargetState::Uninitialised)
        ));

        // A ready target whose identity is not the requested one is refused.
        let result = h
            .switch(h.request(h.source_id, GraphId::new(), h.ready_target.database_url()))
            .await;
        assert!(
            matches!(result, StorageSwitchResult::TargetChanged { observed_graph_id } if observed_graph_id == h.target_id),
        );

        // An exclusively held reservation is admission contention.
        assert!(
            PgAdvisoryLockRepository
                .try_acquire_exclusive(
                    &mut target_probe,
                    advisory_locks::TARGET_MIGRATION_RESERVATION
                )
                .await
                .expect("hold the reservation"),
        );
        let result = h
            .switch(h.request(h.source_id, h.target_id, h.ready_target.database_url()))
            .await;
        assert!(matches!(result, StorageSwitchResult::AdmissionContended));
        assert!(
            PgAdvisoryLockRepository
                .release_exclusive(
                    &mut target_probe,
                    advisory_locks::TARGET_MIGRATION_RESERVATION
                )
                .await
                .expect("release the reservation"),
        );
    }

    #[tokio::test]
    async fn test_switch_quiesces_a_clean_no_runtime_source_and_strands_no_lock() {
        let h = a_switch_harness().await;

        // The clean-ready no-runtime source is already quiescent, so the
        // switch walks admission, both leases, reinspection, and assessment
        // to a proven quiescence — no signal sent, custody live throughout.
        let result = h
            .switch(h.request(h.source_id, h.target_id, h.ready_target.database_url()))
            .await;
        assert!(
            matches!(
                &result,
                StorageSwitchResult::SourceQuiesced { target, .. }
                    if matches!(target.state, DatabaseTargetState::Ready { graph_id } if graph_id == h.target_id)
            ),
            "a clean no-runtime source quiesces through the full path, got {result:?}",
        );
        let mut source_probe = h.source.raw_connection().await.expect("source probe");
        assert!(
            PgAdvisoryLockRepository
                .try_acquire_exclusive(&mut source_probe, advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("probe the source lock"),
            "a settled switch leaves no lock stranded",
        );
        assert!(
            PgAdvisoryLockRepository
                .release_exclusive(&mut source_probe, advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("release the source probe"),
        );

        // Without a pending transition, the follow-up operations are typed
        // refusals rather than effects.
        let continued = h
            .gate
            .continue_switch(
                &operation(),
                &h.lifecycle,
                StorageSwitchContinueRequest {
                    transition_id: StorageTransitionId::new(),
                    expected_runtime: a_runtime_identity(),
                },
            )
            .await
            .expect("continue runs");
        assert!(matches!(
            continued,
            StorageSwitchContinueResult::NoPendingTransition
        ));
        let forced = h
            .gate
            .force_stop(
                &operation(),
                &h.lifecycle,
                StorageSwitchForceStopRequest {
                    transition_id: StorageTransitionId::new(),
                    expected_runtime: a_runtime_identity(),
                },
            )
            .await
            .expect("force runs");
        assert!(matches!(
            forced,
            StorageSwitchForceStopResult::NoPendingTransition
        ));
        let aborted = h
            .gate
            .abort(
                &operation(),
                &h.lifecycle,
                StorageSwitchAbortRequest {
                    transition_id: StorageTransitionId::new(),
                },
            )
            .await
            .expect("abort runs");
        assert!(matches!(
            aborted,
            StorageSwitchAbortResult::NoPendingTransition
        ));
    }

    #[tokio::test]
    async fn test_switch_contends_on_the_source_lock_and_frees_the_reservation() {
        let h = a_switch_harness().await;

        // Another manager's transition holds the source; the reservation was
        // taken first and must be released with the refusal.
        let mut source_holder = h.source.raw_connection().await.expect("holder");
        assert!(
            PgAdvisoryLockRepository
                .try_acquire_exclusive(&mut source_holder, advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("hold the source lock"),
        );
        let result = h
            .switch(h.request(h.source_id, h.target_id, h.ready_target.database_url()))
            .await;
        assert!(matches!(result, StorageSwitchResult::AdmissionContended));

        let mut target_probe = h.ready_target.raw_connection().await.expect("probe");
        assert!(
            PgAdvisoryLockRepository
                .try_acquire_exclusive(
                    &mut target_probe,
                    advisory_locks::TARGET_MIGRATION_RESERVATION
                )
                .await
                .expect("probe the reservation"),
            "a refused switch releases its partial target reservation",
        );
        assert!(
            PgAdvisoryLockRepository
                .release_exclusive(
                    &mut target_probe,
                    advisory_locks::TARGET_MIGRATION_RESERVATION
                )
                .await
                .expect("release the probe"),
        );
        assert!(
            PgAdvisoryLockRepository
                .release_exclusive(&mut source_holder, advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("release the holder"),
        );
    }

    #[tokio::test]
    async fn test_a_failed_revision_fence_frees_the_transition_slot() {
        let h = a_switch_harness().await;
        let stale = ConfigRevision::from_digest(&ConfigDigest::from_bytes(b"stale"));

        let refused = h
            .gate
            .switch(
                &operation(),
                &h.database,
                &h.lifecycle,
                StorageSwitchRequest {
                    expected_revision: stale,
                    expected_runtime: None,
                    source_graph_id: h.source_id,
                    target_graph_id: h.target_id,
                    target_url: SecretLiteral::try_from(h.ready_target.database_url().to_owned())
                        .expect("target url"),
                },
            )
            .await
            .expect_err("a stale revision is refused");
        assert!(matches!(
            refused,
            DatabaseAccessError::RevisionConflict { .. }
        ));

        // The slot must be free again: a refused switch cannot strand the
        // barrier against every later admission.
        drop(
            h.gate
                .admit_migration()
                .expect("migration admits after a refused switch"),
        );
        drop(
            h.gate
                .admit_lifecycle()
                .expect("lifecycle admits after a refused switch"),
        );
        let result = h
            .switch(h.request(h.source_id, h.target_id, h.ready_target.database_url()))
            .await;
        assert!(
            matches!(result, StorageSwitchResult::SourceQuiesced { .. }),
            "a later switch still admits, got {result:?}",
        );
    }

    #[tokio::test]
    async fn test_a_settled_blocker_still_names_the_activity_that_refused() {
        let h = a_switch_harness().await;

        // The blocker drops between the refusal and the assessment read, so
        // the assessment alone would report Ready; the answer must still
        // name the activity that actually refused admission.
        let mut assessment = h
            .gate
            .assess(&operation(), &h.database, None)
            .await
            .expect("assess")
            .value;
        assert_eq!(assessment.verdict, StorageTransitionVerdict::Ready);
        force_refusing_activities(&mut assessment, true, false);
        assert_eq!(assessment.verdict, StorageTransitionVerdict::Blocked);
        assert!(
            assessment
                .activities
                .iter()
                .any(|activity| activity.kind == GraphActivityKind::DatabaseMigration),
        );
    }

    /// The whole operator journey through the gate: a switch that parks on a
    /// timeout, a continue that observes again, and an abort that recovers —
    /// each step's custody and release asserted, with the owner's answers
    /// scripted so the gate's own composition is what is under test.
    #[tokio::test]
    async fn test_a_parked_switch_continues_and_then_aborts_to_recovery() {
        let source = TestDb::new().await;
        let ready_target = TestDb::new().await;
        let (_config_temp, database, worker, _worker_runtime) =
            database_access(source.database_url());
        let revision = worker.resolved_snapshot().await.unwrap().revision;
        let runtime = a_runtime_identity();
        let lifecycle = crate::management::lifecycle::controller_answering_transitions(
            Some(runtime.clone()),
            crate::management::lifecycle::TransitionStopOutcome::StopTimedOut {
                runtime: runtime.clone(),
            },
            vec![
                crate::management::lifecycle::TransitionObserveOutcome::StillPresent {
                    runtime: runtime.clone(),
                    failure: tribal_wire::management::RuntimeStopTimedOutFailure {
                        presentation: tribal_wire::management::FailurePresentation {
                            message: "the runtime is still draining".to_owned(),
                            remediation: None,
                        },
                    },
                },
                crate::management::lifecycle::TransitionObserveOutcome::Exited,
            ],
        );
        let gate = StorageTransitionGate::new();

        let mut conn = source.raw_connection().await.expect("source identity");
        let source_id = PgGraphIdentityRepository
            .get(&mut conn)
            .await
            .expect("source id");
        let mut conn = ready_target
            .raw_connection()
            .await
            .expect("target identity");
        let target_id = PgGraphIdentityRepository
            .get(&mut conn)
            .await
            .expect("target id");

        // The switch parks: the source did not exit inside its window.
        let parked = gate
            .switch(
                &operation(),
                &database,
                &lifecycle,
                StorageSwitchRequest {
                    expected_revision: revision,
                    expected_runtime: Some(runtime.clone()),
                    source_graph_id: source_id,
                    target_graph_id: target_id,
                    target_url: SecretLiteral::try_from(ready_target.database_url().to_owned())
                        .expect("target url"),
                },
            )
            .await
            .expect("switch runs");
        let StorageSwitchResult::StopTimedOut { transition_id, .. } = parked else {
            panic!("the scripted timeout parks the transition, got {parked:?}");
        };

        // Parked means custody retained and every sibling surface refused.
        let mut probe = source.raw_connection().await.expect("probe");
        assert!(
            !PgAdvisoryLockRepository
                .try_acquire_exclusive(&mut probe, advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("probe the source lock"),
            "a parked transition keeps its source lock",
        );
        assert_eq!(
            gate.admit_lifecycle().expect_err("lifecycle refused"),
            transition_id,
        );

        // Continuing observes again and finds the source still present.
        let continued = gate
            .continue_switch(
                &operation(),
                &lifecycle,
                StorageSwitchContinueRequest {
                    transition_id,
                    expected_runtime: runtime.clone(),
                },
            )
            .await
            .expect("continue runs");
        assert!(matches!(
            continued,
            StorageSwitchContinueResult::StopTimedOut { .. }
        ));
        assert!(
            !PgAdvisoryLockRepository
                .try_acquire_exclusive(&mut probe, advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("probe the source lock"),
            "a repeated timeout still keeps custody",
        );

        // Aborting sees the exit and attempts recovery; this scripted owner
        // cannot restart, so the abort completes on that failure and
        // releases its barriers for ordinary lifecycle recovery.
        let aborted = gate
            .abort(
                &operation(),
                &lifecycle,
                StorageSwitchAbortRequest { transition_id },
            )
            .await
            .expect("abort runs");
        assert!(
            matches!(aborted, StorageSwitchAbortResult::SourceStillStopped { .. }),
            "a failed restart after exact exit completes the abort, got {aborted:?}",
        );
        await_lock_released(&source, advisory_locks::GRAPH_TRANSITION).await;
        await_lock_released(&ready_target, advisory_locks::TARGET_MIGRATION_RESERVATION).await;
        drop(gate.admit_lifecycle().expect("the barrier is free again"));
    }

    #[tokio::test]
    async fn test_the_runtime_fence_answers_before_any_database_work() {
        let h = a_switch_harness().await;
        let expected = a_runtime_identity();

        // This manager has no runtime attached, so an attached expectation
        // is refused — and refused ahead of the reservation, proven by
        // holding that lock exclusively: contention would otherwise be the
        // answer.
        let mut reservation_holder = h.ready_target.raw_connection().await.expect("holder");
        assert!(
            PgAdvisoryLockRepository
                .try_acquire_exclusive(
                    &mut reservation_holder,
                    advisory_locks::TARGET_MIGRATION_RESERVATION
                )
                .await
                .expect("hold the reservation"),
        );
        let result = h
            .switch(StorageSwitchRequest {
                expected_revision: h.revision.clone(),
                expected_runtime: Some(expected),
                source_graph_id: h.source_id,
                target_graph_id: h.target_id,
                target_url: SecretLiteral::try_from(h.ready_target.database_url().to_owned())
                    .expect("target url"),
            })
            .await;
        assert!(
            matches!(
                result,
                StorageSwitchResult::RuntimeChanged { observed: None }
            ),
            "an absent runtime is refused before the reservation, got {result:?}",
        );
        assert!(
            PgAdvisoryLockRepository
                .release_exclusive(
                    &mut reservation_holder,
                    advisory_locks::TARGET_MIGRATION_RESERVATION
                )
                .await
                .expect("release the reservation"),
        );

        // The fence also refuses the reverse expectation: a clean-no-runtime
        // switch is only for a source with nothing attached, which this one
        // is — so it proceeds past the fence.
        let proceeded = h
            .switch(h.request(h.source_id, h.target_id, h.ready_target.database_url()))
            .await;
        assert!(
            !matches!(proceeded, StorageSwitchResult::RuntimeChanged { .. }),
            "a matching no-runtime expectation passes the fence, got {proceeded:?}",
        );
    }

    #[tokio::test]
    async fn test_abort_reports_a_restart_failure_only_after_exact_exit() {
        let h = a_switch_harness().await;
        let transition_id = StorageTransitionId::new();
        h.gate
            .install_pending_for_test(
                transition_id,
                TransitionSource::AttachedRuntime(a_runtime_identity()),
                h.source.database_url(),
                h.ready_target.database_url(),
            )
            .await;

        // The source has exited (the harness's manager holds no runtime), so
        // abort attempts the restart. This manager cannot launch a runtime,
        // so the attempt fails — and the abort completes on that failure
        // rather than reporting a recovery it did not achieve.
        let result = h
            .gate
            .abort(
                &operation(),
                &h.lifecycle,
                StorageSwitchAbortRequest { transition_id },
            )
            .await
            .expect("abort runs");
        assert!(
            matches!(result, StorageSwitchAbortResult::SourceStillStopped { .. }),
            "a failed restart after exact exit completes the abort, got {result:?}",
        );

        // The abort completed, so its barriers released for ordinary
        // lifecycle recovery.
        await_lock_released(&h.source, advisory_locks::GRAPH_TRANSITION).await;
    }

    #[tokio::test]
    async fn test_switch_types_an_unreachable_target_as_not_ready() {
        let h = a_switch_harness().await;

        let result = h
            .switch(h.request(
                h.source_id,
                h.target_id,
                "postgres://tribal:tribal@127.0.0.1:1/absent",
            ))
            .await;
        assert!(
            matches!(
                &result,
                StorageSwitchResult::TargetNotReady { target } if matches!(
                    &target.state,
                    DatabaseTargetState::Unavailable { failure }
                        if failure.kind == tribal_wire::management::DatabaseTargetFailureKind::HostUnreachable
                )
            ),
            "an unreachable candidate is a typed receipt, got {result:?}",
        );
    }

    #[tokio::test]
    async fn test_a_blocked_switch_names_the_refusing_activity() {
        let h = a_switch_harness().await;

        let admitted = h.gate.admit_lifecycle().expect("admit lifecycle");
        let result = h
            .switch(h.request(h.source_id, h.target_id, h.ready_target.database_url()))
            .await;
        drop(admitted);
        let StorageSwitchResult::Blocked { assessment } = result else {
            panic!("lifecycle-first admission blocks the switch, got {result:?}");
        };
        assert_eq!(assessment.verdict, StorageTransitionVerdict::Blocked);
        assert!(
            assessment
                .activities
                .iter()
                .any(|activity| activity.kind == GraphActivityKind::RuntimeLifecycle),
            "the blocked answer names the refusing activity",
        );
    }

    #[tokio::test]
    async fn test_a_stalled_lease_peer_settles_rather_than_hanging() {
        // A target that accepts no connection settles inside the lease
        // window as a typed error, never an unbounded wait.
        let started = tokio::time::Instant::now();
        let outcome =
            TransitionLockLease::acquire("postgres://tribal:tribal@127.0.0.1:1/absent", 1).await;
        assert!(
            matches!(outcome, Err(DatabaseAccessError::Connection { .. })),
            "an unreachable peer is refused rather than awaited",
        );
        assert!(
            started.elapsed() < LEASE_IO_WINDOW * 2,
            "the lease settles inside its own bound",
        );
    }

    #[tokio::test]
    async fn test_a_parked_transition_blocks_the_public_assessment() {
        let ctx = TestDb::new().await;
        let (_temp, database, _worker, _runtime) = database_access(ctx.database_url());
        let gate = StorageTransitionGate::new();
        gate.occupy_for_test(StorageTransitionId::new());

        let assessment = gate
            .assess(&operation(), &database, None)
            .await
            .expect("assess")
            .value;
        assert_eq!(assessment.verdict, StorageTransitionVerdict::Blocked);
        assert!(
            assessment
                .activities
                .iter()
                .any(|activity| activity.kind == GraphActivityKind::RuntimeLifecycle),
        );
        gate.release_for_test();
    }

    #[tokio::test]
    async fn test_abort_of_a_clean_no_runtime_origin_releases_without_a_start() {
        let h = a_switch_harness().await;
        let transition_id = StorageTransitionId::new();
        h.gate
            .install_pending_for_test(
                transition_id,
                TransitionSource::CleanNoRuntime,
                h.source.database_url(),
                h.ready_target.database_url(),
            )
            .await;

        let result = h
            .gate
            .abort(
                &operation(),
                &h.lifecycle,
                StorageSwitchAbortRequest { transition_id },
            )
            .await
            .expect("abort runs");
        assert!(matches!(
            result,
            StorageSwitchAbortResult::SourceRemainedStopped
        ));

        // The barriers released with the abort: both locks re-acquirable.
        let mut probe = h.source.raw_connection().await.expect("probe");
        assert!(
            PgAdvisoryLockRepository
                .try_acquire_exclusive(&mut probe, advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("probe the source lock"),
        );
        let mut target_probe = h.ready_target.raw_connection().await.expect("target probe");
        assert!(
            PgAdvisoryLockRepository
                .try_acquire_exclusive(
                    &mut target_probe,
                    advisory_locks::TARGET_MIGRATION_RESERVATION
                )
                .await
                .expect("probe the reservation"),
        );
    }

    #[tokio::test]
    async fn test_continue_quiesces_an_exited_source_and_releases_its_custody() {
        let h = a_switch_harness().await;
        let transition_id = StorageTransitionId::new();
        h.gate
            .install_pending_for_test(
                transition_id,
                TransitionSource::AttachedRuntime(a_runtime_identity()),
                h.source.database_url(),
                h.ready_target.database_url(),
            )
            .await;

        // A transition id that is not this one changes nothing and keeps
        // custody with the transition that holds it.
        let stranger = h
            .gate
            .continue_switch(
                &operation(),
                &h.lifecycle,
                StorageSwitchContinueRequest {
                    transition_id: StorageTransitionId::new(),
                    expected_runtime: a_runtime_identity(),
                },
            )
            .await
            .expect("continue runs");
        assert!(matches!(
            stranger,
            StorageSwitchContinueResult::NoPendingTransition
        ));
        let mut probe = h.source.raw_connection().await.expect("probe");
        assert!(
            !PgAdvisoryLockRepository
                .try_acquire_exclusive(&mut probe, advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("probe the source lock"),
            "a refused follow-up retains the transition's custody",
        );

        // The barrier admits no lifecycle operation while a transition is
        // pending, so a no-runtime source under this transition is its exact
        // exit: continue quiesces and releases both leases.
        let continued = h
            .gate
            .continue_switch(
                &operation(),
                &h.lifecycle,
                StorageSwitchContinueRequest {
                    transition_id,
                    expected_runtime: a_runtime_identity(),
                },
            )
            .await
            .expect("continue runs");
        assert!(
            matches!(
                &continued,
                StorageSwitchContinueResult::SourceQuiesced { transition_id: id, .. }
                    if *id == transition_id
            ),
            "an exited source quiesces under its own transition, got {continued:?}",
        );
        await_lock_released(&h.source, advisory_locks::GRAPH_TRANSITION).await;

        // The transition is gone: a repeat is a typed refusal.
        let repeated = h
            .gate
            .continue_switch(
                &operation(),
                &h.lifecycle,
                StorageSwitchContinueRequest {
                    transition_id,
                    expected_runtime: a_runtime_identity(),
                },
            )
            .await
            .expect("continue runs");
        assert!(matches!(
            repeated,
            StorageSwitchContinueResult::NoPendingTransition
        ));
    }

    #[tokio::test]
    async fn test_a_follow_up_with_a_different_runtime_cannot_release_custody() {
        let h = a_switch_harness().await;
        let transition_id = StorageTransitionId::new();
        h.gate
            .install_pending_for_test(
                transition_id,
                TransitionSource::AttachedRuntime(a_runtime_identity()),
                h.source.database_url(),
                h.ready_target.database_url(),
            )
            .await;
        let mut stranger = a_runtime_identity();
        stranger.instance_id = "someone-else".to_owned();

        let continued = h
            .gate
            .continue_switch(
                &operation(),
                &h.lifecycle,
                StorageSwitchContinueRequest {
                    transition_id,
                    expected_runtime: stranger.clone(),
                },
            )
            .await
            .expect("continue runs");
        assert!(matches!(
            continued,
            StorageSwitchContinueResult::RuntimeChanged
        ));

        let forced = h
            .gate
            .force_stop(
                &operation(),
                &h.lifecycle,
                StorageSwitchForceStopRequest {
                    transition_id,
                    expected_runtime: stranger,
                },
            )
            .await
            .expect("force runs");
        assert!(matches!(
            forced,
            StorageSwitchForceStopResult::RuntimeChanged
        ));

        // Custody is retained: neither refusal released the source lock.
        let mut probe = h.source.raw_connection().await.expect("probe");
        assert!(
            !PgAdvisoryLockRepository
                .try_acquire_exclusive(&mut probe, advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("probe the source lock"),
            "a runtime mismatch retains the transition's custody",
        );
    }

    #[tokio::test]
    async fn test_force_stop_quiesces_only_the_transition_that_owns_the_barrier() {
        let h = a_switch_harness().await;
        let transition_id = StorageTransitionId::new();
        h.gate
            .install_pending_for_test(
                transition_id,
                TransitionSource::AttachedRuntime(a_runtime_identity()),
                h.source.database_url(),
                h.ready_target.database_url(),
            )
            .await;

        let stranger = h
            .gate
            .force_stop(
                &operation(),
                &h.lifecycle,
                StorageSwitchForceStopRequest {
                    transition_id: StorageTransitionId::new(),
                    expected_runtime: a_runtime_identity(),
                },
            )
            .await
            .expect("force runs");
        assert!(matches!(
            stranger,
            StorageSwitchForceStopResult::NoPendingTransition
        ));

        let forced = h
            .gate
            .force_stop(
                &operation(),
                &h.lifecycle,
                StorageSwitchForceStopRequest {
                    transition_id,
                    expected_runtime: a_runtime_identity(),
                },
            )
            .await
            .expect("force runs");
        assert!(
            matches!(
                &forced,
                StorageSwitchForceStopResult::SourceQuiesced { transition_id: id, .. }
                    if *id == transition_id
            ),
            "an exited source quiesces under force, got {forced:?}",
        );
        await_lock_released(&h.source, advisory_locks::GRAPH_TRANSITION).await;
    }

    /// Polls until the lock is free: a released lease closes its session, and
    /// Postgres drops the lock when it notices, not when the value drops.
    async fn await_lock_released(ctx: &TestDb, lock_id: i64) {
        let mut probe = ctx.raw_connection().await.expect("probe session");
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if PgAdvisoryLockRepository
                    .try_acquire_exclusive(&mut probe, lock_id)
                    .await
                    .expect("probe the lock")
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a released lease frees its lock");
    }

    fn a_runtime_identity() -> tribal_wire::management::RuntimeIdentity {
        tribal_wire::management::RuntimeIdentity {
            instance_id: "test-runtime".to_owned(),
            pid: 4242,
            binary_version: "test".to_owned(),
            config_path: tribal_wire::management::ConfigFilePath {
                path: "/tmp/tribal.yaml".to_owned(),
            },
        }
    }

    #[tokio::test]
    async fn test_lock_leases_hold_prove_and_release_across_sessions() {
        let ctx = TestDb::new().await;

        let mut lease =
            TransitionLockLease::acquire(ctx.database_url(), advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("acquire")
                .expect("uncontended");
        assert!(lease.prove().await, "a live session proves custody");

        // A second acquisition of the same lock is contention.
        assert!(
            TransitionLockLease::acquire(ctx.database_url(), advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("attempt")
                .is_none(),
            "the lock is held by the first lease",
        );

        // Dropping the lease closes its session and releases the lock: the
        // manager-custody rule — teardown strands nothing.
        drop(lease);
        let mut probe = ctx.raw_connection().await.expect("probe session");
        let released = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if PgAdvisoryLockRepository
                    .try_acquire_exclusive(&mut probe, advisory_locks::GRAPH_TRANSITION)
                    .await
                    .expect("probe acquire")
                {
                    break true;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the dropped lease releases its lock");
        assert!(released);
    }

    #[tokio::test]
    async fn test_assessment_projects_admitted_migration_and_lifecycle_occupancy() {
        let ctx = TestDb::new().await;
        let (_temp, database, _worker, _runtime) = database_access(ctx.database_url());
        let gate = StorageTransitionGate::new();

        let migration = gate.admit_migration().expect("admit migration");
        let lifecycle = gate.admit_lifecycle().expect("admit lifecycle");
        let assessment = gate
            .assess(&operation(), &database, None)
            .await
            .expect("assess")
            .value;
        drop(migration);
        drop(lifecycle);

        assert_eq!(assessment.verdict, StorageTransitionVerdict::Blocked);
        let kinds: Vec<_> = assessment
            .activities
            .iter()
            .map(|activity| activity.kind.clone())
            .collect();
        assert!(kinds.contains(&GraphActivityKind::DatabaseMigration));
        assert!(kinds.contains(&GraphActivityKind::RuntimeLifecycle));

        let clear = gate
            .assess(&operation(), &database, None)
            .await
            .expect("assess after release")
            .value;
        assert_eq!(clear.verdict, StorageTransitionVerdict::Ready);
    }

    #[test]
    fn test_lifecycle_and_migration_admissions_coexist() {
        let gate = StorageTransitionGate::new();

        let first_lifecycle = gate.admit_lifecycle().expect("first lifecycle admits");
        let second_lifecycle = gate
            .admit_lifecycle()
            .expect("a concurrent lifecycle operation also admits; the owner adjudicates");
        let migration = gate
            .admit_migration()
            .expect("migration and lifecycle never exclude each other");

        drop(first_lifecycle);
        drop(second_lifecycle);
        drop(migration);
    }

    #[test]
    fn test_a_pending_transition_refuses_migration_and_lifecycle_with_its_id() {
        let gate = StorageTransitionGate::new();
        let pending = StorageTransitionId::new();
        gate.occupy_for_test(pending);

        assert_eq!(
            gate.admit_migration().expect_err("migration refused"),
            pending
        );
        assert_eq!(
            gate.admit_lifecycle().expect_err("lifecycle refused"),
            pending
        );

        gate.release_for_test();
        drop(
            gate.admit_migration()
                .expect("admission resumes on release"),
        );
        drop(
            gate.admit_lifecycle()
                .expect("admission resumes on release"),
        );
    }

    #[test]
    fn test_a_dropped_guard_releases_its_occupancy() {
        let gate = StorageTransitionGate::new();
        let guard = gate.admit_migration().expect("admit");
        drop(guard);
        // The slot is empty again: a fresh admission and a clean drop.
        drop(gate.admit_migration().expect("the dropped guard released"));
    }
}
