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

use chrono::{Duration, Utc};
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
    AgentThread, AgentThreadRecordKind, AgentThreadStatus, AgentThreadSuspension,
    CompletionResponse, RunJobId,
};
use tribal_inference::{
    CallContext, CompletionRequest, InferenceError, InferenceProvider, Message, collect_completion,
};
use tribal_runtime_db::{
    ClaimedJob, MeteringGateway, PgRunJobRepository, PollBudget, PostRunningState,
    RunJobRepository, RuntimeDbError, TeardownError, TeardownOutcome, TeardownTarget, WriteOutcome,
    cancel_teardown, mint_grant,
};
use tribal_wire::gateway::{GrantSet, PositionKey};

use super::probe::ProbeSpec;

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
}

/// What a single drive attempt concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedRunOutcome {
    /// The run reached its terminal and the job settled to `done`.
    Done,
    /// The run parked on its durable wait; the job left `running` as
    /// `suspended` with its slot released.
    Suspended,
    /// An observed cancel tore the run down; the job settled to `cancelled`.
    Cancelled,
    /// The teardown could not resolve the run's holds within its budget;
    /// nothing settled, and a later attempt retries.
    HoldsStillLive,
    /// The lease was lost to a reclaim mid-drive; the rival owns the run now.
    OwnershipLost,
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
    /// Whether the durable wait's suspension record is committed.
    waited: bool,
    /// Whether the artifact record is committed.
    artifact_committed: bool,
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
    let progress = re_derive(&mut core, &thread).await?;

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
            &position_keys(&thread, progress.calls_done),
            PostRunningState::Cancelled,
            config,
        )
        .await;
    }

    match adopt(&mut core, &mut runtime, &thread, run_key, claim_token).await? {
        Adoption::Terminal => {
            return teardown(
                &mut runtime,
                gateway,
                run_key,
                claim_token,
                &position_keys(&thread, progress.calls_done),
                PostRunningState::Done,
                config,
            )
            .await;
        }
        Adoption::NotReady => return Ok(ManagedRunOutcome::Suspended),
        Adoption::Running => {}
    }

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
    /// The thread is running; drive it.
    Running,
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
    run_key: RunJobId,
    claim_token: uuid::Uuid,
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
            let resolution = serde_json::json!({ "resolved": "signal" });
            resolve_stage_thread(core, thread.id(), &resolution).await?;
            Ok(Adoption::Running)
        }
        AgentThreadStatus::Queued => PgAgentThreadRepository
            .mark_running(core, thread.id(), AgentThreadStatus::Queued)
            .await
            .map_err(|source| AgentRuntimeError::database("marking the run running", source).into())
            .map(|_| Adoption::Running),
        // Already running (a crash mid-drive left it so); drive on.
        AgentThreadStatus::Running => Ok(Adoption::Running),
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
    config: &ManagedConfig,
) -> Result<ManagedRunOutcome, ManagedRunError> {
    // The metered calls, resuming from the committed count.
    for index in progress.calls_done..spec.calls {
        if cancel_watch.is_cancelled() {
            return teardown(
                runtime,
                gateway,
                run_key,
                claim_token,
                &position_keys(thread, index),
                PostRunningState::Cancelled,
                config,
            )
            .await;
        }
        let context = CallContext {
            position_key: Some(position_key(thread, index)),
            grant: Some(grant.clone()),
        };
        let response = tokio::select! {
            () = cancel_watch.cancelled() => continue,
            result = drive_call(provider, probe_request(config.call_max_tokens), &context) => result?,
        };
        match commit_model_call(core, thread, claim, &response).await {
            Ok(_) => {}
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
        &position_keys(thread, spec.calls),
        PostRunningState::Done,
        config,
    )
    .await
}

/// Makes one metered call and folds its stream to a response.
async fn drive_call(
    provider: &dyn InferenceProvider,
    request: CompletionRequest,
    context: &CallContext,
) -> Result<CompletionResponse, ManagedRunError> {
    let stream = provider
        .complete_stream(request, context)
        .await
        .map_err(ManagedRunError::Inference)?;
    collect_completion(stream)
        .await
        .map_err(ManagedRunError::Inference)
}

/// Re-derives the run's committed progress from its thread log.
async fn re_derive(
    core: &mut PgConnection,
    thread: &AgentThread,
) -> Result<Progress, ManagedRunError> {
    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(core, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("reading the thread log", source))?;
    let calls_done = records
        .iter()
        .filter(|record| record.kind() == AgentThreadRecordKind::AssistantMessage)
        .count();
    Ok(Progress {
        calls_done: u32::try_from(calls_done).unwrap_or(u32::MAX),
        waited: records
            .iter()
            .any(|record| record.kind() == AgentThreadRecordKind::Suspension),
        artifact_committed: records
            .iter()
            .any(|record| record.kind() == AgentThreadRecordKind::AppendArtifact),
    })
}

/// Settles the job to its terminal through the poll-and-ack teardown.
async fn teardown(
    runtime: &mut PgConnection,
    gateway: &impl MeteringGateway,
    run_key: RunJobId,
    claim_token: uuid::Uuid,
    position_keys: &[PositionKey],
    terminal: PostRunningState,
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
        TeardownOutcome::ToreDown if terminal == PostRunningState::Cancelled => {
            ManagedRunOutcome::Cancelled
        }
        TeardownOutcome::ToreDown => ManagedRunOutcome::Done,
        TeardownOutcome::HoldsStillLive => ManagedRunOutcome::HoldsStillLive,
        TeardownOutcome::OwnershipLost => ManagedRunOutcome::OwnershipLost,
    })
}

/// The position keys a run's calls carry — `{thread}:{index}` for each, the
/// idempotency key the gateway meters against, stable across resumes.
fn position_keys(thread: &AgentThread, count: u32) -> Vec<PositionKey> {
    (0..count)
        .map(|index| position_key(thread, index))
        .collect()
}

fn position_key(thread: &AgentThread, index: u32) -> PositionKey {
    PositionKey::new(format!("{}:{index}", thread.id()))
}

/// A probe's metered call: one user turn, a bounded generation so the gateway
/// can size its hold.
fn probe_request(max_tokens: u32) -> CompletionRequest {
    CompletionRequest {
        system: None,
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
