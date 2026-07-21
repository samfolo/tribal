//! The availability sweep: the universal convergence actor.
//!
//! A composition of named, independently tested predicates over one loop,
//! never one growing query. Each predicate scans with `SKIP LOCKED`, so
//! concurrent serve processes never contend, and each acts through the
//! runtime's guarded transitions. The sweep is the structural half of the
//! no-strand guarantee: every suspended thread has a live resolver, a
//! wake-at deadline this sweep drives, or a terminal outcome whose
//! cancel-fallback this sweep performs.

use tracing::Instrument;
use tribal_agent_runtime::{BUDGET_RECHECK_CAUSE, ResolveOutcome, resolve_stage_thread};
use tribal_config::AgentsConfig;
use tribal_db::{
    AgentBindingVersionRepository, AgentThreadRepository, PgAgentBindingVersionRepository,
    PgAgentThreadRepository,
};
use tribal_domain::{AgentThread, AgentThreadSuspension, StageExecutorKind, span_attrs};
use tribal_runtime_db::{PgRunJobRepository, RunJobRepository};
use tribal_telemetry::MetricsRecorder;

use crate::{
    definition::current_stage_budgets,
    worker::{Worker, coupling},
};

/// How many rows each predicate handles per sweep cycle.
const SWEEP_BATCH: u32 = 32;

/// Counts of what one sweep cycle converged.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ThreadSweepStats {
    /// Suspended threads woken by an elapsed timer.
    pub(crate) timer_wakes: u32,
    /// Orphaned descendants of a terminal parent given the cancellation
    /// intent by the cascade janitor.
    pub(crate) cascaded: u32,
    /// Threads cancelled through the fallback (unclaimed, intent pending).
    pub(crate) cancelled: u32,
    /// Stranded relation threads failed: their job had no live resolver.
    pub(crate) stuck_relating: u32,
    /// Managed runs whose elapsed wake the sweep carried to the job plane.
    pub(crate) managed_wakes: u32,
    /// Suspended managed runs woken to be torn down on a cancel intent.
    pub(crate) managed_cancels: u32,
}

impl Worker {
    /// Runs one availability-sweep cycle: the timer-wake predicate, the
    /// orphan-cascade janitor, the cancel-fallback predicate, then the
    /// stuck-relating predicate. Best-effort like every sweep: a failing
    /// predicate warns and leaves convergence to the next cycle.
    ///
    /// The janitor runs before the cancel fallback so an orphan it marks
    /// this cycle is disposed the same cycle rather than the next.
    pub(crate) async fn run_thread_sweep(&self) -> ThreadSweepStats {
        self.run_thread_sweep_inner()
            .instrument(thread_sweep_span())
            .await
    }

    async fn run_thread_sweep_inner(&self) -> ThreadSweepStats {
        let mut stats = ThreadSweepStats::default();
        let Ok(mut conn) = self.pool().acquire().await else {
            tracing::warn!("pool acquire failed for the thread sweep");
            // The cycle still carries its (zero) counts on the span.
            record_sweep_outcome(self.metrics(), &stats);
            return stats;
        };

        stats.timer_wakes = sweep_timer_wakes(self.agents(), &mut conn).await;
        stats.cascaded = sweep_orphan_cascade(&mut conn).await;
        stats.cancelled = sweep_cancel_fallback(self, &mut conn).await;
        stats.stuck_relating = sweep_stuck_relating(self, &mut conn).await;

        // The managed sweeps run only on a worker that drives the job plane; the
        // wake predicate spans both planes (find on core, wake on runtime), the
        // cancel predicate is job-plane only.
        if let Some(managed) = self.managed() {
            match managed.runtime_pool().acquire().await {
                Ok(mut runtime) => {
                    stats.managed_wakes = sweep_managed_wakes(&mut conn, &mut runtime).await;
                    stats.managed_cancels = sweep_managed_cancels(&mut runtime).await;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "runtime pool acquire failed for the managed sweep");
                }
            }
        }

        record_sweep_outcome(self.metrics(), &stats);
        stats
    }
}

/// The availability sweep's per-cycle span. Its convergence counts are
/// recorded as the cycle completes, so they read off one coherent span.
fn thread_sweep_span() -> tracing::Span {
    tracing::info_span!(
        "tribal.thread_sweep",
        { span_attrs::SWEEP_TIMER_WAKES } = tracing::field::Empty,
        { span_attrs::SWEEP_CASCADED } = tracing::field::Empty,
        { span_attrs::SWEEP_CANCELLED } = tracing::field::Empty,
        { span_attrs::SWEEP_STUCK_RELATING } = tracing::field::Empty,
        { span_attrs::SWEEP_MANAGED_WAKES } = tracing::field::Empty,
        { span_attrs::SWEEP_MANAGED_CANCELS } = tracing::field::Empty,
    )
}

/// Records the cycle's counts on the enclosing sweep span, emits a
/// per-action event, and counts a per-action metric for each predicate
/// that converged anything.
fn record_sweep_outcome(recorder: &dyn MetricsRecorder, stats: &ThreadSweepStats) {
    let span = tracing::Span::current();
    span.record(span_attrs::SWEEP_TIMER_WAKES, stats.timer_wakes);
    span.record(span_attrs::SWEEP_CASCADED, stats.cascaded);
    span.record(span_attrs::SWEEP_CANCELLED, stats.cancelled);
    span.record(span_attrs::SWEEP_STUCK_RELATING, stats.stuck_relating);
    span.record(span_attrs::SWEEP_MANAGED_WAKES, stats.managed_wakes);
    span.record(span_attrs::SWEEP_MANAGED_CANCELS, stats.managed_cancels);
    emit_sweep_action(
        recorder,
        span_attrs::SWEEP_ACTION_TIMER_WAKE,
        stats.timer_wakes,
    );
    emit_sweep_action(
        recorder,
        span_attrs::SWEEP_ACTION_ORPHAN_CASCADE,
        stats.cascaded,
    );
    emit_sweep_action(
        recorder,
        span_attrs::SWEEP_ACTION_CANCEL_FALLBACK,
        stats.cancelled,
    );
    emit_sweep_action(
        recorder,
        span_attrs::SWEEP_ACTION_STUCK_RELATING,
        stats.stuck_relating,
    );
    emit_sweep_action(
        recorder,
        span_attrs::SWEEP_ACTION_MANAGED_WAKE,
        stats.managed_wakes,
    );
    emit_sweep_action(
        recorder,
        span_attrs::SWEEP_ACTION_MANAGED_CANCEL,
        stats.managed_cancels,
    );
}

/// Emits a per-action sweep event and counts the metric when the predicate
/// converged anything.
fn emit_sweep_action(recorder: &dyn MetricsRecorder, action: &'static str, count: u32) {
    if count > 0 {
        tracing::info!(
            { span_attrs::SWEEP_ACTION } = action,
            { span_attrs::SWEEP_ACTION_COUNT } = count,
            "thread sweep converged an action",
        );
        recorder.record_agent_sweep_action(action, u64::from(count));
    }
}

/// The timer-wake predicate: suspended threads whose `wake_at` elapsed
/// get the full resolve transaction (a timer-fired input record, the
/// running status, and the driving task re-queued).
async fn sweep_timer_wakes(agents: &AgentsConfig, conn: &mut sqlx::PgConnection) -> u32 {
    let due = match PgAgentThreadRepository
        .find_due_timer_wakes(conn, SWEEP_BATCH)
        .await
    {
        Ok(due) => due,
        Err(e) => {
            tracing::warn!(error = %e, "timer-wake scan failed");
            return 0;
        }
    };

    let mut woken = 0;
    for thread in due {
        // The resolution payload discriminates its cause: a budget wake
        // carries the suspension's unchanged-recheck count forward (so
        // the resumed admission's accounting is durable rather than
        // worker memory) and the budgets current at the wake as the
        // resumption's admission basis.
        let resolution = match thread.suspension() {
            Some(AgentThreadSuspension::BudgetExhaustion { unchanged_rechecks }) => {
                serde_json::json!({
                    "cause": BUDGET_RECHECK_CAUSE,
                    "fired_at": chrono::Utc::now(),
                    "unchanged_rechecks": unchanged_rechecks,
                    "budgets_basis": wake_budgets_basis(agents, conn, &thread).await,
                })
            }
            _ => serde_json::json!({ "cause": "timer", "fired_at": chrono::Utc::now() }),
        };
        match resolve_stage_thread(conn, thread.id(), &resolution).await {
            Ok(ResolveOutcome::Woken) => woken += 1,
            Ok(outcome) => {
                tracing::debug!(thread_id = %thread.id(), ?outcome, "timer wake skipped");
            }
            Err(e) => {
                tracing::warn!(thread_id = %thread.id(), error = %e, "timer wake failed");
            }
        }
    }
    woken
}

/// The budgets a woken thread's re-admission will run against, as of
/// this wake: re-resolved from the current configuration under the
/// thread's recorded executor kind. Provenance for the resolution
/// record; `null` when the thread carries no product binding or stage.
async fn wake_budgets_basis(
    agents: &AgentsConfig,
    conn: &mut sqlx::PgConnection,
    thread: &AgentThread,
) -> serde_json::Value {
    let (Some(binding_version_id), Some(stage)) =
        (thread.binding_version_id(), thread.stage().product())
    else {
        return serde_json::Value::Null;
    };
    let executor = match PgAgentBindingVersionRepository
        .find_by_id(conn, binding_version_id)
        .await
    {
        Ok(Some(binding)) => binding.definition().executor,
        Ok(None) | Err(_) => StageExecutorKind::OneShot,
    };
    let budgets = current_stage_budgets(stage, executor, agents);
    serde_json::to_value(budgets).unwrap_or(serde_json::Value::Null)
}

/// The orphan-cascade janitor: a non-terminal thread whose parent is
/// terminal receives the cancellation intent, the convergent half of the
/// cancellation cascade. It heals a crash between a parent's terminal
/// commit and its post-commit fast-path cascade, and stands in for that
/// fast path on every terminal transition that does not run it. Writing
/// the intent is all it does; the cancel fallback (next, same cycle)
/// disposes the unclaimed ones, and a claimed child's driver observes the
/// intent at its own boundary.
async fn sweep_orphan_cascade(conn: &mut sqlx::PgConnection) -> u32 {
    match PgAgentThreadRepository
        .cascade_cancel_to_orphans(conn, SWEEP_BATCH, coupling::CANCEL_CASCADE_REQUESTED_BY)
        .await
    {
        Ok(count) => u32::try_from(count).unwrap_or(u32::MAX),
        Err(e) => {
            tracing::warn!(error = %e, "orphan-cascade scan failed");
            0
        }
    }
}

/// The cancel-fallback predicate: live threads carrying a durable intent
/// whose driving task is unclaimed (a suspended thread with no live
/// worker) get the cancel transaction. A claimed task means a live
/// worker will observe the intent at its own boundary, so the fallback
/// skips it. Intents are written by an operator, by a parent's
/// cancellation cascade, or by the orphan janitor above.
async fn sweep_cancel_fallback(worker: &Worker, conn: &mut sqlx::PgConnection) -> u32 {
    let intents = match PgAgentThreadRepository
        .find_cancel_intents(conn, SWEEP_BATCH)
        .await
    {
        Ok(intents) => intents,
        Err(e) => {
            tracing::warn!(error = %e, "cancel-intent scan failed");
            return 0;
        }
    };

    let mut cancelled = 0;
    for thread in intents {
        // A running thread's worker handles the intent itself unless the
        // worker died; the unclaimed guard inside the transaction is the
        // arbiter, so the sweep simply attempts every candidate. Job
        // coupling rides the same transaction through the seam; the owed
        // notification goes out after it commits.
        match coupling::cancel_thread(conn, &thread).await {
            Ok(coupling::CancelThreadOutcome::Cancelled { notification }) => {
                cancelled += 1;
                if let Some(notice) = notification {
                    worker.notify_job_state(notice.job_id, notice.state);
                }
                tracing::info!(
                    thread_id = %thread.id(),
                    status = thread.status().as_str(),
                    "cancel fallback terminated a thread",
                );
            }
            Ok(coupling::CancelThreadOutcome::Skipped) => {
                tracing::debug!(thread_id = %thread.id(), "cancel fallback skipped");
            }
            Err(e) => {
                tracing::warn!(thread_id = %thread.id(), error = %e, "cancel fallback failed");
            }
        }
    }
    cancelled
}

/// The stuck-relating predicate: a suspended relation thread whose job is
/// still relating but whose resolver has died (no live child thread, no
/// live descendant driver task) gets the disposal seam, failing its
/// stranded job. Relation is job-terminal with no fan-in, so this is the
/// one suspension that cannot otherwise converge.
///
/// Convergence is normally the driver family reclaiming the verifier
/// child; this sweep is the authority only when the driver loop itself is
/// gone, which the liveness predicate is what detects. It shares the same
/// transaction-composable seam as the cancel fallback, so the job
/// coupling and its owed notification ride the one disposal.
async fn sweep_stuck_relating(worker: &Worker, conn: &mut sqlx::PgConnection) -> u32 {
    let stuck = match PgAgentThreadRepository
        .find_stuck_relating_threads(conn, SWEEP_BATCH)
        .await
    {
        Ok(stuck) => stuck,
        Err(e) => {
            tracing::warn!(error = %e, "stuck-relating scan failed");
            return 0;
        }
    };

    let mut failed = 0;
    for thread in stuck {
        match coupling::cancel_thread(conn, &thread).await {
            Ok(coupling::CancelThreadOutcome::Cancelled { notification }) => {
                failed += 1;
                if let Some(notice) = notification {
                    worker.notify_job_state(notice.job_id, notice.state);
                }
                tracing::warn!(
                    thread_id = %thread.id(),
                    "stuck-relating sweep failed a stranded job",
                );
            }
            Ok(coupling::CancelThreadOutcome::Skipped) => {
                tracing::debug!(thread_id = %thread.id(), "stuck-relating sweep skipped");
            }
            Err(e) => {
                tracing::warn!(thread_id = %thread.id(), error = %e, "stuck-relating sweep failed");
            }
        }
    }
    failed
}

/// The managed timer-wake predicate: a suspended managed thread whose `wake_at`
/// elapsed has its job woken on the other plane (`suspended`→`queued`), so a
/// worker claims and resolves it. The sweep touches the thread not at all — the
/// worker owns the resume — and re-scans every cycle (level-triggered) until the
/// thread is resolved, so no wake is lost. Best-effort: a failed wake warns and
/// the next cycle re-attempts.
async fn sweep_managed_wakes(
    core: &mut sqlx::PgConnection,
    runtime: &mut sqlx::PgConnection,
) -> u32 {
    let due = match PgAgentThreadRepository
        .find_due_managed_wakes(core, SWEEP_BATCH)
        .await
    {
        Ok(due) => due,
        Err(e) => {
            tracing::warn!(error = %e, "managed-wake scan failed");
            return 0;
        }
    };

    let mut woken = 0;
    for run_key in due {
        match PgRunJobRepository.wake(runtime, run_key).await {
            // The job woke; the thread stays suspended until a worker resolves
            // it, so a later cycle re-finds it and re-wakes a job the worker has
            // not yet claimed — a no-op once it is queued or running.
            Ok(true) => woken += 1,
            Ok(false) => {
                tracing::debug!(%run_key, "managed wake found the job already off suspended");
            }
            Err(e) => tracing::warn!(%run_key, error = %e, "managed wake failed"),
        }
    }
    woken
}

/// The cancel-while-suspended predicate: a suspended job carrying a durable
/// cancel intent is woken so a claiming worker tears it down, rather than
/// leaving it parked until a deadline the cancellation makes moot. Job-plane
/// only — the thread is resolved by the claiming worker's teardown.
async fn sweep_managed_cancels(runtime: &mut sqlx::PgConnection) -> u32 {
    let cancelled = match PgRunJobRepository
        .find_suspended_cancel_requested(runtime, i64::from(SWEEP_BATCH))
        .await
    {
        Ok(cancelled) => cancelled,
        Err(e) => {
            tracing::warn!(error = %e, "suspended-cancel scan failed");
            return 0;
        }
    };

    let mut woken = 0;
    for run_key in cancelled {
        match PgRunJobRepository.wake(runtime, run_key).await {
            Ok(true) => woken += 1,
            Ok(false) => {}
            Err(e) => tracing::warn!(%run_key, error = %e, "cancel wake failed"),
        }
    }
    woken
}

#[cfg(test)]
mod tests {
    use tribal_agent_runtime::{SuspendOutcome, suspend_stage_thread};
    use tribal_db::{
        AgentBindingVersionRepository, AgentThreadRecordRepository, DrivingTaskRef, JobRepository,
        NewAgentBindingVersion, NewAgentThread, PgAgentBindingVersionRepository,
        PgAgentThreadRecordRepository, PgTaskRepository, PrincipalRepository, ProjectRepository,
        TaskRepository,
    };
    use tribal_domain::{
        AGENT_THREAD_FORMAT_VERSION, AgentThreadRecordKind, AgentThreadStage, GitRemote, TaskType,
    };
    use tribal_test_utils::{
        TestDb, TracingCapture, a_new_job, a_new_principal, a_new_project, a_new_prompt_version,
        a_new_system_fingerprint, a_new_task, an_agent_definition, insert_prompt_version,
        upsert_system_fingerprint,
    };

    use super::*;

    /// Seeds a claimed stage task with its thread, ready to suspend.
    async fn a_claimed_thread(
        conn: &mut sqlx::PgConnection,
        suffix: &str,
    ) -> (tribal_domain::Task, tribal_domain::AgentThread) {
        let principal = tribal_db::PgPrincipalRepository
            .insert(
                conn,
                &a_new_principal()
                    .principal_key(format!("user:sweep-{suffix}"))
                    .build(),
            )
            .await
            .expect("insert principal");
        let project = tribal_db::PgProjectRepository
            .insert_git(
                conn,
                &a_new_project()
                    .git_remote(GitRemote::from_parts(
                        "github.com",
                        &format!("test/sweep-{suffix}"),
                        None,
                    ))
                    .build(),
            )
            .await
            .expect("insert project");
        let pv_id = insert_prompt_version(conn, &a_new_prompt_version().build()).await;
        let fingerprint =
            upsert_system_fingerprint(conn, &a_new_system_fingerprint().build()).await;
        let job = tribal_db::PgJobRepository
            .insert(
                conn,
                &a_new_job()
                    .project_id(project.id())
                    .principal_id(principal.id())
                    .extraction_system_prompt_version_id(pv_id)
                    .extraction_user_prompt_version_id(pv_id)
                    .triage_system_prompt_version_id(pv_id)
                    .triage_user_prompt_version_id(pv_id)
                    .relation_system_prompt_version_id(pv_id)
                    .relation_user_prompt_version_id(pv_id)
                    .system_fingerprint_hash(fingerprint)
                    .build(),
            )
            .await
            .expect("insert job");
        PgTaskRepository
            .insert(conn, &a_new_task().job_id(job.id()).build())
            .await
            .expect("insert task");
        let claimed = PgTaskRepository
            .claim(conn, 1, &format!("sweep-{suffix}"))
            .await
            .expect("claim");
        let task = claimed.first().expect("claims").clone();
        let binding = PgAgentBindingVersionRepository
            .record(
                conn,
                &NewAgentBindingVersion::builder()
                    .hash(format!("{:0>64}", suffix.len()))
                    .pipeline_stage(TaskType::Extraction)
                    .definition(an_agent_definition().build())
                    .build(),
            )
            .await
            .expect("record binding");
        let thread = PgAgentThreadRepository
            .insert(
                conn,
                &NewAgentThread::builder()
                    .stage(AgentThreadStage::Product(TaskType::Extraction))
                    .binding_version_id(Some(binding.id()))
                    .driving_task(DrivingTaskRef::Stage(task.id()))
                    .principal_id(Some(principal.id()))
                    .format_version(AGENT_THREAD_FORMAT_VERSION)
                    .build(),
            )
            .await
            .expect("insert thread");
        let moved = PgAgentThreadRepository
            .mark_running(conn, thread.id(), tribal_domain::AgentThreadStatus::Queued)
            .await
            .expect("mark running");
        assert_eq!(moved, 1);
        let thread = PgAgentThreadRepository
            .find_by_id(conn, thread.id())
            .await
            .expect("re-read")
            .expect("present");
        (task, thread)
    }

    /// The timer-wake sweep's resolution payload discriminates its
    /// cause: a budget-exhaustion suspension wakes with the durable
    /// re-check count carried forward; a plain timer stays a timer.
    #[tokio::test]
    async fn test_budget_wakes_carry_the_recheck_count_and_timer_wakes_stay_timers() {
        let ctx = TestDb::new().await;
        let mut conn = ctx.raw_connection().await.expect("raw connection");

        let (budget_task, budget_thread) = a_claimed_thread(&mut conn, "budget").await;
        let outcome = suspend_stage_thread(
            &mut conn,
            &budget_thread,
            budget_task.id(),
            budget_task.claim_token().expect("token"),
            &AgentThreadSuspension::BudgetExhaustion {
                unchanged_rechecks: 2,
            },
            Some(chrono::Utc::now() - chrono::Duration::seconds(1)),
        )
        .await
        .expect("suspend");
        assert!(matches!(outcome, SuspendOutcome::Suspended));

        let (timer_task, timer_thread) = a_claimed_thread(&mut conn, "timer").await;
        let outcome = suspend_stage_thread(
            &mut conn,
            &timer_thread,
            timer_task.id(),
            timer_task.claim_token().expect("token"),
            &AgentThreadSuspension::Timer,
            Some(chrono::Utc::now() - chrono::Duration::seconds(1)),
        )
        .await
        .expect("suspend");
        assert!(matches!(outcome, SuspendOutcome::Suspended));

        let woken = sweep_timer_wakes(&AgentsConfig::default(), &mut conn).await;
        assert_eq!(woken, 2);

        let budget_wake = PgAgentThreadRecordRepository
            .last_record(&mut conn, budget_thread.id())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(budget_wake.kind(), AgentThreadRecordKind::Input);
        assert_eq!(budget_wake.content()["cause"], BUDGET_RECHECK_CAUSE);
        assert_eq!(
            budget_wake.content()["unchanged_rechecks"],
            2,
            "the suspension's count rides the resolution record",
        );

        let timer_wake = PgAgentThreadRecordRepository
            .last_record(&mut conn, timer_thread.id())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(timer_wake.content()["cause"], "timer");
        assert!(timer_wake.content().get("unchanged_rechecks").is_none());
    }

    /// The sweep cycle records its four convergence counts on one
    /// `tribal.thread_sweep` span and emits a per-action event only for the
    /// predicates that converged anything.
    #[tokio::test]
    async fn test_sweep_cycle_records_counts_and_non_zero_actions() {
        let (capture, _capture) = TracingCapture::install();
        let recorder = tribal_telemetry::noop_recorder();
        let stats = ThreadSweepStats {
            timer_wakes: 2,
            cascaded: 0,
            cancelled: 1,
            stuck_relating: 0,
            managed_wakes: 0,
            managed_cancels: 0,
        };
        thread_sweep_span().in_scope(|| record_sweep_outcome(recorder.as_ref(), &stats));

        let span = capture
            .span("tribal.thread_sweep")
            .expect("the sweep opens one span");
        assert_eq!(span.field(span_attrs::SWEEP_TIMER_WAKES), Some("2"));
        assert_eq!(span.field(span_attrs::SWEEP_CASCADED), Some("0"));
        assert_eq!(span.field(span_attrs::SWEEP_CANCELLED), Some("1"));
        assert_eq!(span.field(span_attrs::SWEEP_STUCK_RELATING), Some("0"));

        let actions: Vec<_> = capture
            .events()
            .into_iter()
            .filter(|event| event.message == "thread sweep converged an action")
            .collect();
        assert_eq!(actions.len(), 2, "only non-zero actions emit an event");
        assert!(actions.iter().any(|event| {
            event.field(span_attrs::SWEEP_ACTION) == Some("timer_wake")
                && event.field(span_attrs::SWEEP_ACTION_COUNT) == Some("2")
        }));
        assert!(actions.iter().any(|event| {
            event.field(span_attrs::SWEEP_ACTION) == Some("cancel_fallback")
                && event.field(span_attrs::SWEEP_ACTION_COUNT) == Some("1")
        }));
    }

    // -- Managed sweeps -----------------------------------------------------

    /// Suspends a managed thread in the core plane, anchored to `run_key`, so
    /// the managed-wake scan finds it once `wake_at` elapses.
    async fn suspend_managed_thread_at(
        core: &mut sqlx::PgConnection,
        run_key: tribal_domain::RunJobId,
        wake_at: chrono::DateTime<chrono::Utc>,
    ) {
        let thread = PgAgentThreadRepository
            .insert(
                core,
                &NewAgentThread::builder()
                    .stage(AgentThreadStage::Managed)
                    .driving_task(DrivingTaskRef::Managed(run_key))
                    .run_claim_token(Some(uuid::Uuid::new_v4()))
                    .format_version(AGENT_THREAD_FORMAT_VERSION)
                    .build(),
            )
            .await
            .expect("insert managed thread");
        PgAgentThreadRepository
            .mark_running(core, thread.id(), tribal_domain::AgentThreadStatus::Queued)
            .await
            .expect("running");
        PgAgentThreadRepository
            .suspend(
                core,
                thread.id(),
                &AgentThreadSuspension::Signal {
                    key: "cortex.managed.cap-wait".to_owned(),
                },
                Some(wake_at),
            )
            .await
            .expect("suspend managed");
    }

    /// Enqueues, claims, and leaves a job `suspended` — optionally with a cancel
    /// intent — returning its run key.
    async fn a_suspended_job(
        runtime: &mut sqlx::PgConnection,
        suffix: &str,
        cancel: bool,
    ) -> tribal_domain::RunJobId {
        PgRunJobRepository
            .enqueue(
                runtime,
                tribal_runtime_db::NewRunJob {
                    account_id: format!("acct-{suffix}"),
                    kind: "probe".to_owned(),
                    payload: serde_json::json!({}),
                    idempotency_key: format!("sweep-{suffix}"),
                    priority: 0,
                },
            )
            .await
            .expect("enqueue");
        let claimed = PgRunJobRepository
            .claim(runtime, 4, chrono::Duration::seconds(60))
            .await
            .expect("claim")
            .expect("a job to claim");
        if cancel {
            PgRunJobRepository
                .request_cancel(runtime, claimed.id)
                .await
                .expect("request cancel");
        }
        PgRunJobRepository
            .leave_running(
                runtime,
                claimed.id,
                claimed.claim_token,
                tribal_runtime_db::PostRunningState::Suspended,
            )
            .await
            .expect("suspend the job");
        claimed.id
    }

    async fn job_state(
        runtime: &mut sqlx::PgConnection,
        id: tribal_domain::RunJobId,
    ) -> tribal_runtime_db::RunJobState {
        PgRunJobRepository
            .state_of(runtime, id)
            .await
            .expect("state read")
            .expect("the job exists")
    }

    #[tokio::test]
    async fn test_managed_wake_sweep_wakes_a_due_run_and_leaves_a_pending_one() {
        let ctx = TestDb::new().await;
        let rt = tribal_test_utils::provision_runtime_db().await;
        let mut core = ctx.raw_connection().await.expect("core connection");
        let mut runtime = rt.pool().acquire().await.expect("runtime connection");

        let past = chrono::Utc::now() - chrono::Duration::seconds(5);
        let future = chrono::Utc::now() + chrono::Duration::minutes(10);

        let due = a_suspended_job(&mut runtime, "due", false).await;
        suspend_managed_thread_at(&mut core, due, past).await;
        let pending = a_suspended_job(&mut runtime, "pending", false).await;
        suspend_managed_thread_at(&mut core, pending, future).await;

        let woken = sweep_managed_wakes(&mut core, &mut runtime).await;
        assert_eq!(woken, 1, "only the due run's job woke");
        assert_eq!(
            job_state(&mut runtime, due).await,
            tribal_runtime_db::RunJobState::Queued
        );
        assert_eq!(
            job_state(&mut runtime, pending).await,
            tribal_runtime_db::RunJobState::Suspended,
            "the not-yet-due run stays parked",
        );
    }

    #[tokio::test]
    async fn test_cancel_sweep_wakes_a_suspended_cancelled_run_alone() {
        let rt = tribal_test_utils::provision_runtime_db().await;
        let mut runtime = rt.pool().acquire().await.expect("runtime connection");

        let cancelled = a_suspended_job(&mut runtime, "cancel", true).await;
        let plain = a_suspended_job(&mut runtime, "plain", false).await;

        let woken = sweep_managed_cancels(&mut runtime).await;
        assert_eq!(woken, 1, "only the cancelled suspended run woke");
        assert_eq!(
            job_state(&mut runtime, cancelled).await,
            tribal_runtime_db::RunJobState::Queued,
        );
        assert_eq!(
            job_state(&mut runtime, plain).await,
            tribal_runtime_db::RunJobState::Suspended,
            "an uncancelled suspended run is left parked",
        );
    }
}
