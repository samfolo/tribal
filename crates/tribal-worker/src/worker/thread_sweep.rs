//! The availability sweep: the universal convergence actor.
//!
//! A composition of named, independently tested predicates over one loop,
//! never one growing query. Each predicate scans with `SKIP LOCKED`, so
//! concurrent serve processes never contend, and each acts through the
//! runtime's guarded transitions. The sweep is the structural half of the
//! no-strand guarantee: every suspended thread has a live resolver, a
//! wake-at deadline this sweep drives, or a terminal outcome whose
//! cancel-fallback this sweep performs.

use tribal_agent_runtime::{BUDGET_RECHECK_CAUSE, ResolveOutcome, resolve_stage_thread};
use tribal_db::{AgentThreadRepository, PgAgentThreadRepository};
use tribal_domain::AgentThreadSuspension;

use crate::worker::{Worker, coupling};

/// How many rows each predicate handles per sweep cycle.
const SWEEP_BATCH: u32 = 32;

/// Counts of what one sweep cycle converged.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ThreadSweepStats {
    /// Suspended threads woken by an elapsed timer.
    pub(crate) timer_wakes: u32,
    /// Threads cancelled through the fallback (unclaimed, intent pending).
    pub(crate) cancelled: u32,
}

impl Worker {
    /// Runs one availability-sweep cycle: the timer-wake predicate, then
    /// the cancel-fallback predicate. Best-effort like every sweep — a
    /// failing predicate warns and leaves convergence to the next cycle.
    pub(crate) async fn run_thread_sweep(&self) -> ThreadSweepStats {
        let mut stats = ThreadSweepStats::default();
        let Ok(mut conn) = self.pool().acquire().await else {
            tracing::warn!("pool acquire failed for the thread sweep");
            return stats;
        };

        stats.timer_wakes = sweep_timer_wakes(&mut conn).await;
        stats.cancelled = sweep_cancel_fallback(self, &mut conn).await;
        stats
    }
}

/// The timer-wake predicate: suspended threads whose `wake_at` elapsed
/// get the full resolve transaction — a timer-fired input record, the
/// running status, and the driving task re-queued.
async fn sweep_timer_wakes(conn: &mut sqlx::PgConnection) -> u32 {
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
        // carries the suspension's unchanged-recheck count forward, so
        // the resumed admission's accounting is durable rather than
        // worker memory.
        let resolution = match thread.suspension() {
            Some(AgentThreadSuspension::BudgetExhaustion { unchanged_rechecks }) => {
                serde_json::json!({
                    "cause": BUDGET_RECHECK_CAUSE,
                    "fired_at": chrono::Utc::now(),
                    "unchanged_rechecks": unchanged_rechecks,
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
    if woken > 0 {
        tracing::info!(woken, "timer wakes resolved");
    }
    woken
}

/// The cancel-fallback predicate: live threads carrying a durable intent
/// whose driving task is unclaimed (a suspended thread with no live
/// worker) get the cancel transaction. A claimed task means a live
/// worker will observe the intent at its own boundary, so the fallback
/// skips it. The orphan-spotting janitor that writes intents to
/// abandoned descendants arrives with the first parent-thread producer;
/// until then every intent is operator-written.
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

#[cfg(test)]
mod tests {
    use tribal_agent_runtime::{SuspendOutcome, suspend_stage_thread};
    use tribal_db::{
        AgentBindingVersionRepository, AgentThreadRecordRepository, DrivingTaskRef, JobRepository,
        NewAgentBindingVersion, NewAgentThread, PgAgentBindingVersionRepository,
        PgAgentThreadRecordRepository, PgTaskRepository, PrincipalRepository, ProjectRepository,
        TaskRepository,
    };
    use tribal_domain::{AGENT_THREAD_FORMAT_VERSION, AgentThreadRecordKind, GitRemote, TaskType};
    use tribal_test_utils::{
        a_new_job, a_new_principal, a_new_project, a_new_prompt_version, a_new_system_fingerprint,
        a_new_task, an_agent_definition, insert_prompt_version, serial_lock, test_context,
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
            .insert(
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
                    .pipeline_stage(TaskType::Extraction)
                    .binding_version_id(binding.id())
                    .driving_task(DrivingTaskRef::Stage(task.id()))
                    .principal_id(principal.id())
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
        let _guard = serial_lock().await;
        let ctx = test_context().await;
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

        let woken = sweep_timer_wakes(&mut conn).await;
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

        tribal_test_utils::truncate_all_tables(&mut conn).await;
    }
}
