//! The managed-run drive: the job-plane sibling of the product driver.
//!
//! A managed run is anchored to a `run_job` and driven as an agent thread with
//! no product stage. The drive claims off the job plane (the loop), then walks
//! the run's [`ProbeSpec`] over the seam's guarded primitives: adopt-or-create
//! the thread under the fence, re-derive progress from the committed tail, make
//! the metered calls through the Platform bracket, park on a durable wait,
//! commit the artifact, and terminalize by the two-plane choreography — the
//! thread's terminal, then the job's settle-or-release teardown.
//!
//! Resume is re-derive: every claim goes back through the fence, counts what
//! the log already holds, and continues from there, so a crash or a reclaim
//! never repeats a committed call.

use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Postgres, pool::PoolConnection};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tribal_agent_runtime::{
    AgentRuntimeError, DrivingClaim, ManagedRunDisposition, commit_artifact_record,
    commit_managed_terminal, commit_model_call, ensure_managed_thread, resolve_stage_thread,
    suspend_managed_thread,
};
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, PgAgentThreadRecordRepository,
    PgAgentThreadRepository,
};
use tribal_domain::{
    AgentThread, AgentThreadRecord, AgentThreadRecordKind, AgentThreadStatus,
    AgentThreadSuspension, CompletionResponse, RunJobId,
};
use tribal_inference::{
    CallContext, CompletionRequest, InferenceError, InferenceProvider, Message, collect_completion,
};
use tribal_runtime_db::{
    CapBehaviour, ClaimedJob, MeteringGateway, PgRunJobRepository, PollBudget, PostRunningState,
    RunDisposition, RunJobRepository, RuntimeDbError, TeardownError, TeardownOutcome,
    TeardownTarget, WriteOutcome, cancel_teardown, cap_disposition, mint_grant,
};
use tribal_wire::gateway::{GatewayError, GrantSet, PositionKey};

use super::probe::ProbeSpec;

/// The reserved signal key a money-driven cap requeue parks on, distinct from a
/// probe's own durable-wait signal so the re-derive tells the two apart.
const CAP_WAIT_SIGNAL: &str = "cortex.managed.cap-wait";

/// The timing a managed drive runs under.
#[derive(Debug, Clone)]
pub struct ManagedConfig {
    /// The job lease each heartbeat renews.
    pub lease: Duration,
    /// The interval between lease heartbeats.
    pub heartbeat_interval: StdDuration,
    /// The tokens a probe call requests; the gateway sizes the hold from it.
    pub call_max_tokens: u32,
    /// The teardown's holds-report poll budget.
    pub teardown_budget: PollBudget,
    /// How long a hard-stopped run waits for credit before it gives up.
    pub cap_give_up_after: Duration,
    /// How long a throttled run backs off before it retries — recomputed fresh
    /// on each re-suspend, never carried.
    pub cap_backoff: Duration,
}

/// What a single drive attempt concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedRunOutcome {
    /// The run reached its terminal and the job settled to `done`.
    Done,
    /// The run parked on a wait — its durable signal or a money-driven cap
    /// requeue; the job left `running` as `suspended` with its slot released.
    Suspended,
    /// An observed cancel tore the run down; the job settled to `cancelled`.
    Cancelled,
    /// A money refusal no wait could clear failed the run: its thread is
    /// `Failed` and the job settled to `cancelled` with its holds resolved.
    Failed,
    /// The teardown could not resolve the run's holds within its budget;
    /// nothing settled, and a later attempt retries.
    HoldsStillLive,
    /// The lease was lost to a reclaim mid-drive; the rival owns the run now.
    OwnershipLost,
}

/// Why a managed run's cap wait woke: a hard-stop's give-up deadline fired, or a
/// throttle's backoff elapsed. Reconstructed from the account's [`CapBehaviour`]
/// at resolve and recorded in the resolution, so a crash after the resolve
/// re-derives it and never re-suspends a spent give-up bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WakeCause {
    /// The hard-stop deadline fired: a renewed refusal fails the run.
    GiveUp,
    /// The throttle backoff elapsed: a renewed refusal re-suspends afresh.
    Backoff,
}

impl WakeCause {
    /// The cause a cap wait resolves under, per the account's cap setting.
    fn of(behaviour: CapBehaviour) -> Self {
        match behaviour {
            CapBehaviour::HardStop => WakeCause::GiveUp,
            CapBehaviour::ThrottleQueue => WakeCause::Backoff,
        }
    }

    /// Reads the cause a resolution record carries, if it is a cap wake.
    fn from_resolution(content: &serde_json::Value) -> Option<Self> {
        serde_json::from_value(content.get("managed_wake")?.clone()).ok()
    }

    /// Renders the resolution record a cap wake commits.
    fn resolution(self) -> serde_json::Value {
        serde_json::json!({ "managed_wake": self })
    }
}

/// A fault that ends a drive attempt without settling the run.
#[derive(Debug, thiserror::Error)]
pub enum ManagedRunError {
    /// The job's payload was not a probe spec.
    #[error("the probe payload was not a probe spec: {0}")]
    Payload(#[source] serde_json::Error),
    /// A guarded thread operation failed.
    #[error("a managed thread operation failed: {0}")]
    Thread(#[from] AgentRuntimeError),
    /// A job-plane operation failed.
    #[error("a job-plane operation failed: {0}")]
    Job(#[from] RuntimeDbError),
    /// A metered call failed for a reason that is not a gateway refusal.
    #[error("a metered call failed: {0}")]
    Inference(#[source] InferenceError),
    /// The settle-or-release teardown crossing failed.
    #[error("the teardown crossing failed: {0}")]
    Teardown(#[from] TeardownError),
    /// Acquiring a database connection failed.
    #[error("acquiring a {plane} connection failed: {source}")]
    Pool {
        /// Which pool the acquire hit.
        plane: &'static str,
        /// The underlying pool error.
        #[source]
        source: sqlx::Error,
    },
}

/// The progress a run's committed log already holds, re-derived on every claim.
#[derive(Debug, Clone, Copy)]
struct Progress {
    /// How many metered calls have committed their assistant record.
    calls_done: u32,
    /// Whether the probe's durable wait — its own signal, not a cap requeue —
    /// is committed.
    waited: bool,
    /// Whether the artifact record is committed.
    artifact_committed: bool,
    /// The cap wake cause a resolved-but-uncommitted re-attempt runs under: set
    /// by the last cap resolution the log holds, cleared by the call it commits.
    /// A crash between the resolve and the call re-derives it here.
    pending_cap_cause: Option<WakeCause>,
}

/// Drives one claimed managed run to a settled outcome, adopting its thread
/// through the fence and re-deriving from the committed tail.
///
/// # Errors
///
/// Returns [`ManagedRunError`] on a malformed payload, a database fault, or a
/// non-refusal call failure; a lost lease resolves to
/// [`ManagedRunOutcome::OwnershipLost`], not an error.
pub async fn drive_managed_run(
    core_pool: &PgPool,
    runtime_pool: &PgPool,
    provider: &dyn InferenceProvider,
    gateway: &impl MeteringGateway,
    claimed: &ClaimedJob,
    config: &ManagedConfig,
) -> Result<ManagedRunOutcome, ManagedRunError> {
    let spec =
        ProbeSpec::from_payload(claimed.payload.clone()).map_err(ManagedRunError::Payload)?;
    let run_key = claimed.id;
    let claim_token = claimed.claim_token;
    let grant = mint_grant(claimed);
    let claim = DrivingClaim::managed(run_key, claim_token);

    let mut core = acquire(core_pool, "core").await?;
    let mut runtime = acquire(runtime_pool, "runtime").await?;

    // Adopt-or-create under the fence; the token this claim presents becomes
    // the thread's lease.
    let thread =
        ensure_managed_thread(&mut core, run_key, claim_token, claimed.payload.clone()).await?;
    let progress = re_derive(&mut core, &thread, &spec).await?;

    // A run cancelled while suspended tears down on adoption rather than
    // resuming — the same flag the heartbeat polls, read once up front.
    if PgRunJobRepository
        .cancel_requested(&mut runtime, run_key)
        .await?
    {
        return teardown(
            &mut runtime,
            gateway,
            run_key,
            claim_token,
            &position_keys(run_key, progress.calls_done),
            PostRunningState::Cancelled,
            ManagedRunOutcome::Cancelled,
            config,
        )
        .await;
    }

    let resumed_cap = match adopt(
        &mut core,
        &mut runtime,
        &thread,
        &spec,
        run_key,
        claim_token,
        progress,
    )
    .await?
    {
        Adoption::Terminal => {
            return teardown(
                &mut runtime,
                gateway,
                run_key,
                claim_token,
                &position_keys(run_key, progress.calls_done),
                PostRunningState::Done,
                ManagedRunOutcome::Done,
                config,
            )
            .await;
        }
        Adoption::NotReady => return Ok(ManagedRunOutcome::Suspended),
        Adoption::Running { resumed_cap } => resumed_cap,
    };

    let cancel_watch = CancellationToken::new();
    let heartbeat = spawn_heartbeat(
        runtime_pool.clone(),
        run_key,
        claim_token,
        config.lease,
        config.heartbeat_interval,
        cancel_watch.clone(),
    );

    let outcome = walk(
        &mut core,
        &mut runtime,
        provider,
        gateway,
        &thread,
        &spec,
        &grant,
        &claim,
        &cancel_watch,
        run_key,
        claim_token,
        progress,
        resumed_cap,
        config,
    )
    .await;

    // Await the abort so the heartbeat's lease connection is released before the
    // drive returns, rather than on a task that outlives it.
    heartbeat.abort();
    let _ = heartbeat.await;
    outcome
}

/// What the adoption of a claimed run's thread resolved to.
enum Adoption {
    /// The thread is running; drive it. `resumed_cap` carries the cause of a
    /// cap wait this adoption resolved, so the re-attempted call fails or
    /// re-suspends on a renewed refusal rather than parking afresh.
    Running {
        /// The cap wake cause under which the re-attempt runs, if this run
        /// resumed a money-driven cap wait.
        resumed_cap: Option<WakeCause>,
    },
    /// The thread was already terminal; settle the job to converge.
    Terminal,
    /// The thread is suspended and its wait is unsatisfied; re-park and yield.
    NotReady,
}

/// Brings an adopted thread to a drivable state, or reports why it cannot be.
async fn adopt(
    core: &mut PgConnection,
    runtime: &mut PgConnection,
    thread: &AgentThread,
    spec: &ProbeSpec,
    run_key: RunJobId,
    claim_token: uuid::Uuid,
    progress: Progress,
) -> Result<Adoption, ManagedRunError> {
    match thread.status() {
        AgentThreadStatus::Suspended => {
            // The worker owns the resume, never the sweep: resolve only a
            // satisfied wait, and re-park a still-waiting one.
            let ready = thread.wake_at().is_some_and(|at| at <= Utc::now());
            if !ready {
                PgRunJobRepository
                    .leave_running(runtime, run_key, claim_token, PostRunningState::Suspended)
                    .await?;
                return Ok(Adoption::NotReady);
            }
            // A cap wait resolves under a wake cause reconstructed from the
            // account's cap setting, recorded before `mark_running` nulls the
            // deadline so a crash after the resolve re-derives it; the probe's
            // own durable wait resolves plainly.
            let resumed_cap = matches!(
                thread.suspension(),
                Some(AgentThreadSuspension::Signal { key }) if key == CAP_WAIT_SIGNAL
            )
            .then(|| WakeCause::of(spec.cap_behaviour));
            let resolution = match resumed_cap {
                Some(cause) => cause.resolution(),
                None => serde_json::json!({ "resolved": "signal" }),
            };
            resolve_stage_thread(core, thread.id(), &resolution).await?;
            Ok(Adoption::Running { resumed_cap })
        }
        AgentThreadStatus::Queued => PgAgentThreadRepository
            .mark_running(core, thread.id(), AgentThreadStatus::Queued)
            .await
            .map_err(|source| AgentRuntimeError::database("marking the run running", source).into())
            .map(|_| Adoption::Running { resumed_cap: None }),
        // Already running (a crash mid-drive left it so); the re-attempt runs
        // under whatever cap cause the log's last resolution recorded.
        AgentThreadStatus::Running => Ok(Adoption::Running {
            resumed_cap: progress.pending_cap_cause,
        }),
        // Completed, Failed, Cancelled, DeadLetter: the run already terminated.
        _ => Ok(Adoption::Terminal),
    }
}

/// Walks the probe over the seam primitives from the re-derived position.
#[allow(clippy::too_many_arguments)]
async fn walk(
    core: &mut PgConnection,
    runtime: &mut PgConnection,
    provider: &dyn InferenceProvider,
    gateway: &impl MeteringGateway,
    thread: &AgentThread,
    spec: &ProbeSpec,
    grant: &GrantSet,
    claim: &DrivingClaim,
    cancel_watch: &CancellationToken,
    run_key: RunJobId,
    claim_token: uuid::Uuid,
    progress: Progress,
    resumed_cap: Option<WakeCause>,
    config: &ManagedConfig,
) -> Result<ManagedRunOutcome, ManagedRunError> {
    // A cap resume's cause governs the re-attempted call alone; a committed
    // call spends it, and later calls refuse fresh.
    let mut resumed_cap = resumed_cap;
    // The metered calls, resuming from the committed count.
    for index in progress.calls_done..spec.calls {
        if cancel_watch.is_cancelled() {
            return teardown(
                runtime,
                gateway,
                run_key,
                claim_token,
                &position_keys(run_key, index),
                PostRunningState::Cancelled,
                ManagedRunOutcome::Cancelled,
                config,
            )
            .await;
        }
        let context = CallContext {
            position_key: Some(position_key(run_key, index)),
            grant: Some(grant.clone()),
        };
        let call = tokio::select! {
            () = cancel_watch.cancelled() => continue,
            result = drive_call(provider, probe_request(spec.system.clone(), config.call_max_tokens), &context) => result,
        };
        let response = match call {
            Ok(response) => response,
            // A gateway refusal is money, not a fault: dispose it under the
            // account's cap setting rather than ending the drive in error.
            Err(InferenceError::GatewayRefused { error }) => {
                return dispose_cap_refusal(
                    core,
                    runtime,
                    gateway,
                    thread,
                    spec,
                    claim,
                    &error,
                    resumed_cap,
                    index,
                    run_key,
                    claim_token,
                    config,
                )
                .await;
            }
            Err(source) => return Err(ManagedRunError::Inference(source)),
        };
        match commit_model_call(core, thread, claim, &response).await {
            Ok(_) => resumed_cap = None,
            Err(AgentRuntimeError::LeaseLost { .. }) => {
                return Ok(ManagedRunOutcome::OwnershipLost);
            }
            Err(source) => return Err(source.into()),
        }
    }

    // The durable wait, once, parking the run until its signal.
    let pending_wait = if progress.waited {
        None
    } else {
        spec.wait_signal.clone()
    };
    if let Some(key) = pending_wait {
        let cause = AgentThreadSuspension::Signal { key };
        match suspend_managed_thread(core, thread, claim, &cause, None).await {
            Ok(()) => {}
            Err(AgentRuntimeError::LeaseLost { .. }) => {
                return Ok(ManagedRunOutcome::OwnershipLost);
            }
            Err(source) => return Err(source.into()),
        }
        PgRunJobRepository
            .leave_running(runtime, run_key, claim_token, PostRunningState::Suspended)
            .await?;
        return Ok(ManagedRunOutcome::Suspended);
    }

    // The artifact, once.
    if !progress.artifact_committed {
        let artifact = serde_json::json!({ "note": spec.artifact_note });
        match commit_artifact_record(core, thread, *claim, &artifact).await {
            Ok(_) => {}
            Err(AgentRuntimeError::LeaseLost { .. }) => {
                return Ok(ManagedRunOutcome::OwnershipLost);
            }
            Err(source) => return Err(source.into()),
        }
    }

    // Terminalize: the thread's terminal, then the job's settle. A missed CAS
    // means a rival already settled the thread — converge on the job all the
    // same.
    match commit_managed_terminal(core, thread, claim, ManagedRunDisposition::Completed).await {
        Ok(()) | Err(AgentRuntimeError::StatusCasMissed { .. }) => {}
        Err(AgentRuntimeError::LeaseLost { .. }) => return Ok(ManagedRunOutcome::OwnershipLost),
        Err(source) => return Err(source.into()),
    }
    teardown(
        runtime,
        gateway,
        run_key,
        claim_token,
        &position_keys(run_key, spec.calls),
        PostRunningState::Done,
        ManagedRunOutcome::Done,
        config,
    )
    .await
}

/// Disposes a metered call's gateway refusal under the account's cap setting:
/// a hard-stop suspends to its give-up deadline and fails once that deadline's
/// wake still finds it short; a throttle re-suspends at a fresh backoff; an
/// unpriceable or not-entitled refusal fails on first sight.
#[allow(clippy::too_many_arguments)]
async fn dispose_cap_refusal(
    core: &mut PgConnection,
    runtime: &mut PgConnection,
    gateway: &impl MeteringGateway,
    thread: &AgentThread,
    spec: &ProbeSpec,
    claim: &DrivingClaim,
    error: &GatewayError,
    resumed_cap: Option<WakeCause>,
    index: u32,
    run_key: RunJobId,
    claim_token: uuid::Uuid,
    config: &ManagedConfig,
) -> Result<ManagedRunOutcome, ManagedRunError> {
    let now = Utc::now();
    match cap_disposition(error, spec.cap_behaviour, now, config.cap_give_up_after) {
        // A fresh hard-stop parks to its give-up deadline; a resumed one whose
        // deadline already fired fails rather than stretching the bound afresh.
        RunDisposition::Suspend { give_up_at } if resumed_cap.is_none() => {
            suspend_cap(
                core,
                runtime,
                thread,
                claim,
                give_up_at,
                run_key,
                claim_token,
            )
            .await
        }
        RunDisposition::Requeue => {
            let wake_at = now + config.cap_backoff;
            suspend_cap(core, runtime, thread, claim, wake_at, run_key, claim_token).await
        }
        // A resumed hard-stop whose deadline fired, or a refusal no wait can
        // clear, fails the run cleanly.
        RunDisposition::Suspend { .. } | RunDisposition::Fail => {
            fail_run(
                core,
                runtime,
                gateway,
                thread,
                claim,
                index,
                run_key,
                claim_token,
                config,
            )
            .await
        }
        // The bracket owns `InFlight`/`Failed`; reaching the run's mapping is an
        // unmodelled refusal, surfaced rather than silently parked.
        RunDisposition::Bracket => {
            Err(ManagedRunError::Inference(InferenceError::GatewayRefused {
                error: *error,
            }))
        }
    }
}

/// Parks the run on the reserved cap-wait signal until `wake_at`, thread first,
/// then leaves the job `suspended`.
async fn suspend_cap(
    core: &mut PgConnection,
    runtime: &mut PgConnection,
    thread: &AgentThread,
    claim: &DrivingClaim,
    wake_at: DateTime<Utc>,
    run_key: RunJobId,
    claim_token: uuid::Uuid,
) -> Result<ManagedRunOutcome, ManagedRunError> {
    let cause = AgentThreadSuspension::Signal {
        key: CAP_WAIT_SIGNAL.to_owned(),
    };
    match suspend_managed_thread(core, thread, claim, &cause, Some(wake_at)).await {
        Ok(()) => {}
        Err(AgentRuntimeError::LeaseLost { .. }) => return Ok(ManagedRunOutcome::OwnershipLost),
        Err(source) => return Err(source.into()),
    }
    PgRunJobRepository
        .leave_running(runtime, run_key, claim_token, PostRunningState::Suspended)
        .await?;
    Ok(ManagedRunOutcome::Suspended)
}

/// Fails the run cleanly: the thread's `Failed` terminal, then the job settled
/// to `cancelled` with its holds resolved.
#[allow(clippy::too_many_arguments)]
async fn fail_run(
    core: &mut PgConnection,
    runtime: &mut PgConnection,
    gateway: &impl MeteringGateway,
    thread: &AgentThread,
    claim: &DrivingClaim,
    index: u32,
    run_key: RunJobId,
    claim_token: uuid::Uuid,
    config: &ManagedConfig,
) -> Result<ManagedRunOutcome, ManagedRunError> {
    match commit_managed_terminal(core, thread, claim, ManagedRunDisposition::Failed).await {
        Ok(()) | Err(AgentRuntimeError::StatusCasMissed { .. }) => {}
        Err(AgentRuntimeError::LeaseLost { .. }) => return Ok(ManagedRunOutcome::OwnershipLost),
        Err(source) => return Err(source.into()),
    }
    teardown(
        runtime,
        gateway,
        run_key,
        claim_token,
        &position_keys(run_key, index),
        PostRunningState::Cancelled,
        ManagedRunOutcome::Failed,
        config,
    )
    .await
}

/// Makes one metered call and folds its stream to a response.
async fn drive_call(
    provider: &dyn InferenceProvider,
    request: CompletionRequest,
    context: &CallContext,
) -> Result<CompletionResponse, InferenceError> {
    let stream = provider.complete_stream(request, context).await?;
    collect_completion(stream).await
}

/// Re-derives the run's committed progress from its thread log.
async fn re_derive(
    core: &mut PgConnection,
    thread: &AgentThread,
    spec: &ProbeSpec,
) -> Result<Progress, ManagedRunError> {
    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(core, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("reading the thread log", source))?;

    let mut calls_done: u32 = 0;
    let mut waited = false;
    let mut artifact_committed = false;
    // The cap cause of the last resolution not yet spent by a committed call;
    // an assistant record after it means the re-attempt succeeded.
    let mut pending_cap_cause = None;
    for record in &records {
        match record.kind() {
            AgentThreadRecordKind::AssistantMessage => {
                calls_done = calls_done.saturating_add(1);
                pending_cap_cause = None;
            }
            AgentThreadRecordKind::AppendArtifact => artifact_committed = true,
            AgentThreadRecordKind::Suspension => {
                if is_durable_wait(record, spec) {
                    waited = true;
                }
            }
            AgentThreadRecordKind::Input => {
                if let Some(cause) = WakeCause::from_resolution(record.content()) {
                    pending_cap_cause = Some(cause);
                }
            }
            _ => {}
        }
    }
    Ok(Progress {
        calls_done,
        waited,
        artifact_committed,
        pending_cap_cause,
    })
}

/// Whether a suspension record is the probe's own durable wait, told from a
/// money-driven cap requeue by its signal key.
fn is_durable_wait(record: &AgentThreadRecord, spec: &ProbeSpec) -> bool {
    let Some(signal) = spec.wait_signal.as_deref() else {
        return false;
    };
    matches!(
        serde_json::from_value::<AgentThreadSuspension>(record.content().clone()),
        Ok(AgentThreadSuspension::Signal { key }) if key == signal
    )
}

/// Settles the job to its terminal through the poll-and-ack teardown, reporting
/// `settled` once its holds resolve. `terminal` is the job state (a failed run
/// settles the job to `cancelled`); `settled` is the outcome the drive reports.
#[allow(clippy::too_many_arguments)]
async fn teardown(
    runtime: &mut PgConnection,
    gateway: &impl MeteringGateway,
    run_key: RunJobId,
    claim_token: uuid::Uuid,
    position_keys: &[PositionKey],
    terminal: PostRunningState,
    settled: ManagedRunOutcome,
    config: &ManagedConfig,
) -> Result<ManagedRunOutcome, ManagedRunError> {
    let target = TeardownTarget {
        id: run_key,
        claim_token,
        run_key: run_key.to_string(),
        position_keys: position_keys.to_vec(),
        terminal,
    };
    let outcome = cancel_teardown(runtime, gateway, &target, &config.teardown_budget).await?;
    Ok(match outcome {
        TeardownOutcome::ToreDown => settled,
        TeardownOutcome::HoldsStillLive => ManagedRunOutcome::HoldsStillLive,
        TeardownOutcome::OwnershipLost => ManagedRunOutcome::OwnershipLost,
    })
}

/// The position keys a run's calls carry — `{run_key}:{index}` for each, keyed
/// on the run's job-plane anchor so the holds the gateway files under a position
/// key's run-key prefix are the holds the teardown reconciles by that same key.
/// The idempotency key the gateway meters against, stable across resumes.
fn position_keys(run_key: RunJobId, count: u32) -> Vec<PositionKey> {
    (0..count)
        .map(|index| position_key(run_key, index))
        .collect()
}

fn position_key(run_key: RunJobId, index: u32) -> PositionKey {
    PositionKey::new(format!("{run_key}:{index}"))
}

/// A probe's metered call: one user turn under the probe's steering system
/// prompt, a bounded generation so the gateway can size its hold.
fn probe_request(system: Option<String>, max_tokens: u32) -> CompletionRequest {
    CompletionRequest {
        system,
        messages: vec![Message::User {
            content: "probe".to_owned(),
        }],
        tools: vec![],
        temperature: None,
        max_tokens: Some(max_tokens),
        response_format: None,
    }
}

/// Renews the job lease on a timer, firing the cancel watch when the lease is
/// lost or a cancel intent is observed, so an in-flight call drops promptly.
fn spawn_heartbeat(
    runtime_pool: PgPool,
    run_key: RunJobId,
    claim_token: uuid::Uuid,
    lease: Duration,
    interval: StdDuration,
    cancel_watch: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            tokio::select! {
                () = cancel_watch.cancelled() => return,
                _ = ticker.tick() => {}
            }
            let Ok(mut conn) = runtime_pool.acquire().await else {
                continue;
            };
            let renewed = PgRunJobRepository
                .heartbeat(&mut conn, run_key, claim_token, lease)
                .await;
            if !matches!(renewed, Ok(WriteOutcome::Applied)) {
                cancel_watch.cancel();
                return;
            }
            if PgRunJobRepository
                .cancel_requested(&mut conn, run_key)
                .await
                .unwrap_or(false)
            {
                cancel_watch.cancel();
                return;
            }
        }
    })
}

async fn acquire(
    pool: &PgPool,
    plane: &'static str,
) -> Result<PoolConnection<Postgres>, ManagedRunError> {
    pool.acquire()
        .await
        .map_err(|source| ManagedRunError::Pool { plane, source })
}
