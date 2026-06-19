//! The driver loop: a sibling worker loop executing the driver family.
//!
//! It claims `Drive` tasks, executes their child threads as one-shots
//! under the driver-claim guard and a driver-task heartbeat, and hands
//! each child's result back to its parent through the child-terminal
//! transaction. A child's permanent failure crosses into the parent as
//! deferred death. The `DeferredTool` kind has no producer in this
//! release: the claim loop matches it exhaustively and dead-letters it
//! loudly, so an unmodelled enqueue can never sit unexecuted in silence.
//!
//! Reclaim mirrors the stage families': a lapsed-heartbeat claim is
//! re-queued within its attempt budget, and the final attempt drives the
//! deferred-death transaction so the child never strands its lineage.

use std::sync::Arc;

use tribal_agent_runtime::{
    AgentRuntimeError, ChildTerminalOutcome, ParentResolution, RenderedConversation,
    adopt_conversation, commit_child_terminal, commit_deferred_death,
};
use tribal_common::clamp_to_i32;
use tribal_db::{
    AgentBindingVersionRepository, AgentDriverTaskRepository, AgentThreadRecordRepository,
    AgentThreadRepository, DbError, PgAgentBindingVersionRepository, PgAgentDriverTaskRepository,
    PgAgentThreadRecordRepository, PgAgentThreadRepository,
};
use tribal_domain::{
    AgentDriverTask, AgentDriverTaskKind, AgentThread, AgentThreadId, AgentThreadRecordKind,
    AgentThreadStatus, AgentThreadSuspension, ErrorOutcome, StageParameters, UsageOwner,
};
use tribal_inference::{
    CompletionRequest, Message, PermitWait, ResponseFormat, Role, UsageAttribution,
};

use crate::{
    error::StageError,
    prompt::narrow_temperature,
    worker::{
        Worker,
        heartbeat::spawn_driver_heartbeat,
        thread::{guard_route, stage_spec},
    },
};

/// How many driver tasks one cycle claims and executes.
const DRIVER_CLAIM_BATCH: u32 = 4;

/// The claimer identity recorded on a driver task's lease.
const DRIVER_WORKER: &str = "driver-loop";

/// The deferred-death message for a child the §10.3 cancellation cascade
/// reached before the driver could run it.
const CHILD_CASCADE_CANCELLED: &str =
    "the parent's cancellation cascaded to this verifier child before it ran";

impl Worker {
    /// Drives the driver family until cancellation: each cycle reclaims
    /// stale leases, then claims and executes a batch of pending tasks.
    /// Sibling to the reclaim and reindex loops; [`Worker::run`] spawns
    /// it, and it is a standalone entry point for tests and tooling.
    pub async fn run_driver_loop(self: &Arc<Self>) {
        let mut ticker = tokio::time::interval(self.config().poll_interval());
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // skip the immediate first tick

        loop {
            tokio::select! {
                () = self.cancellation_token().cancelled() => return,
                _ = ticker.tick() => {}
            }

            self.reclaim_stale_driver_tasks().await;
            self.drive_pending_tasks().await;
        }
    }

    /// Claims one batch of pending driver tasks and drives them with
    /// bounded concurrency.
    ///
    /// The batch runs concurrently, not in series: a single lane would
    /// funnel every stage's verifier child through one model call at a
    /// time, and relation's terminal hand-back, on the job-completion
    /// critical path, would wait behind all of them. The claim count is
    /// the concurrency bound; the gateway's per-provider permits bound the
    /// real inference parallelism beneath it.
    async fn drive_pending_tasks(self: &Arc<Self>) {
        let claimed = {
            let Ok(mut conn) = self.pool().acquire().await else {
                tracing::warn!("driver loop pool acquire failed");
                return;
            };
            match PgAgentDriverTaskRepository
                .claim(&mut conn, DRIVER_CLAIM_BATCH, DRIVER_WORKER)
                .await
            {
                Ok(claimed) => claimed,
                Err(e) => {
                    tracing::warn!(error = %e, "driver task claim failed");
                    return;
                }
            }
        };

        let mut batch = tokio::task::JoinSet::new();
        for task in claimed {
            let worker = Arc::clone(self);
            batch.spawn(async move {
                match task.kind() {
                    AgentDriverTaskKind::Drive => worker.drive_child(&task).await,
                    // No v1 producer enqueues a deferred tool; matching it
                    // exhaustively and dead-lettering loudly means an
                    // unmodelled enqueue surfaces, never sits unexecuted.
                    AgentDriverTaskKind::DeferredTool => {
                        tracing::error!(
                            driver_task_id = %task.id(),
                            "claimed a deferred-tool driver task, which no producer creates in \
                             this release; dead-lettering it",
                        );
                        worker
                            .dispose_driver_task(&task, "deferred-tool execution is unsupported")
                            .await;
                    }
                }
            });
        }
        // Each task handles its own errors and returns unit, so a join
        // error here is a panic or a cancellation: surface it rather than
        // let a crashed driver task vanish silently.
        while let Some(joined) = batch.join_next().await {
            if let Err(error) = joined {
                tracing::error!(error = %error, "a driver batch task failed to complete");
            }
        }
    }

    /// Executes one `Drive` task's child as a one-shot and hands the
    /// result back to its parent, or dispositions a failure.
    async fn drive_child(&self, task: &AgentDriverTask) {
        let Some(claim_token) = task.claim_token() else {
            tracing::error!(driver_task_id = %task.id(), "claimed driver task carries no token");
            return;
        };
        let heartbeat = spawn_driver_heartbeat(
            self.pool().clone(),
            task.id(),
            claim_token,
            self.config().heartbeat_interval(),
            self.cancellation_token().clone(),
        );
        let deadline = tokio::time::Instant::now() + self.config().task_timeout();
        let result = self.execute_child(task, claim_token, deadline).await;
        heartbeat.abort();

        if let Err(error) = result {
            self.dispose_failed_child(task, &error).await;
        }
    }

    /// The child's one-shot: adopt its committed conversation, call the
    /// model under thread-keyed attribution, and commit the child
    /// terminal handing the result back to the parent.
    async fn execute_child(
        &self,
        task: &AgentDriverTask,
        claim_token: uuid::Uuid,
        deadline: tokio::time::Instant,
    ) -> Result<(), StageError> {
        let mut conn = self.acquire_driver_conn().await?;

        let child = self.run_child_thread(&mut conn, task, claim_token).await?;

        // A cancellation intent supersedes the run: the §10.3 cascade (or
        // an operator) marked this child while it queued. Hand the
        // cancellation back through deferred death rather than spend a paid
        // model call; a parent already terminal discards it under the
        // orphan guard.
        if child.cancel_requested_at().is_some() {
            drop(conn);
            self.deferred_death(task, CHILD_CASCADE_CANCELLED).await;
            return Ok(());
        }

        let parent_id = child
            .parent_thread_id()
            .ok_or_else(|| StageError::Runtime {
                context: "a driver child has no parent thread".to_owned(),
                source: AgentRuntimeError::ThreadMissing {
                    task_id: tribal_domain::TaskId::new(),
                },
            })?;

        // The child is born with its input in the suspend-with-child
        // transaction; the driver adopts that rendered conversation (the
        // verifier's rubric, submission, and verdict schema) and re-sends
        // it verbatim, never re-rendering.
        let conversation = adopt_conversation(&mut conn, &child)
            .await
            .map_err(|source| driver_runtime_error("adopting the child's conversation", source))?;

        // The child runs under the sampling parameters its binding records,
        // the same parameters-follow-binding rule the stage loop obeys, so a
        // resumed child sends what it was admitted under, never a default.
        let binding = PgAgentBindingVersionRepository
            .find_by_id(&mut conn, child.binding_version_id())
            .await
            .map_err(|e| driver_db("loading the child's binding", e))?
            .ok_or_else(|| {
                driver_db(
                    "loading the child's binding",
                    DbError::NotFound {
                        entity: "agent_binding_version",
                        id: child.binding_version_id().to_string(),
                    },
                )
            })?;
        // The gateway routes the call by the child's stage, so a config
        // edit that moved the stage's route after the child was admitted
        // would run it under an endpoint its recorded binding does not
        // name; fail terminal before the call, as the stage path does.
        guard_route(
            child.pipeline_stage().as_str(),
            binding.definition(),
            stage_spec(self.stage_specs(), child.pipeline_stage()),
        )?;

        let request = request_from(&conversation, &binding.definition().parameters);
        let attribution = child_attribution(&child, task.attempt());
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let response = self
            .gateway()
            .complete(
                child.pipeline_stage(),
                request,
                PermitWait::Bounded { limit: remaining },
                &attribution,
            )
            .await
            .map_err(|e| crate::stages::map_gateway_error("driver child call", e))?;

        let Some(parent) = self.load_parent_resolution(&mut conn, parent_id).await? else {
            // The launching suspension record is written in the same
            // transaction as the child, so its absence is a consistency
            // fault, not a race.
            return Err(StageError::Runtime {
                context: "the child's parent carries no launching suspension".to_owned(),
                source: AgentRuntimeError::ThreadMissing {
                    task_id: tribal_domain::TaskId::new(),
                },
            });
        };

        // The parent re-reads the child's structured response from the
        // hand-back tool result and interprets the verdict itself.
        let outcome = commit_child_terminal(
            &mut conn,
            &child,
            task.id(),
            claim_token,
            clamp_to_i32(task.attempt()),
            &response,
            &parent,
        )
        .await
        .map_err(|source| driver_runtime_error("committing the child terminal", source))?;

        if outcome == ChildTerminalOutcome::ParentNotWaiting {
            tracing::info!(
                child_thread_id = %child.id(),
                "the child resolved into a parent no longer waiting; hand-back discarded",
            );
        }
        Ok(())
    }

    /// Loads the child thread the driver task drives and moves it to
    /// running, tolerating a reclaim that finds it already running.
    async fn run_child_thread(
        &self,
        conn: &mut sqlx::PgConnection,
        task: &AgentDriverTask,
        claim_token: uuid::Uuid,
    ) -> Result<AgentThread, StageError> {
        let child = PgAgentThreadRepository
            .find_by_id(conn, task.thread_id())
            .await
            .map_err(|e| driver_db("loading the child thread", e))?
            .ok_or_else(|| StageError::Runtime {
                context: "the driver task's child thread vanished".to_owned(),
                source: AgentRuntimeError::ThreadMissing {
                    task_id: tribal_domain::TaskId::new(),
                },
            })?;

        if child.status() == AgentThreadStatus::Queued {
            // Claim-guarded so a reclaimed-away driver cannot flip a child
            // it no longer owns.
            let mut txn = sqlx::Connection::begin(conn)
                .await
                .map_err(|e| driver_sqlx("beginning the child run transaction", e))?;
            if !PgAgentDriverTaskRepository
                .holds_claim(&mut txn, task.id(), claim_token)
                .await
                .map_err(|e| driver_db("verifying the child lease", e))?
            {
                return Err(StageError::OwnershipLost);
            }
            PgAgentThreadRepository
                .mark_running(&mut txn, child.id(), AgentThreadStatus::Queued)
                .await
                .map_err(|e| driver_db("marking the child running", e))?;
            txn.commit()
                .await
                .map_err(|e| driver_sqlx("committing the child run transaction", e))?;
            return PgAgentThreadRepository
                .find_by_id(conn, child.id())
                .await
                .map_err(|e| driver_db("re-reading the running child", e))?
                .ok_or_else(|| StageError::Runtime {
                    context: "the child thread vanished after mark-running".to_owned(),
                    source: AgentRuntimeError::ThreadMissing {
                        task_id: tribal_domain::TaskId::new(),
                    },
                });
        }
        Ok(child)
    }

    /// Reads the parent's launching suspension from its durable log. The
    /// record persists even when a cancellation has cleared the live
    /// suspension column, so a child resolving into a terminal parent
    /// still finds the call it answers.
    async fn load_parent_resolution(
        &self,
        conn: &mut sqlx::PgConnection,
        parent_id: AgentThreadId,
    ) -> Result<Option<ParentResolution>, StageError> {
        let records = PgAgentThreadRecordRepository
            .find_by_thread_id(conn, parent_id)
            .await
            .map_err(|e| driver_db("reading the parent's log", e))?;
        for record in records.iter().rev() {
            if record.kind() != AgentThreadRecordKind::Suspension {
                continue;
            }
            if let Ok(AgentThreadSuspension::DeferredToolResults {
                requesting_seq,
                pending_tool_call_ids,
            }) = serde_json::from_value(record.content().clone())
                && let Some(tool_call_id) = pending_tool_call_ids.into_iter().next()
            {
                return Ok(Some(ParentResolution {
                    thread_id: parent_id,
                    requesting_seq,
                    tool_call_id,
                }));
            }
        }
        Ok(None)
    }

    /// Dispositions a failed child execution: a retryable failure within
    /// budget re-queues the driver task; the final attempt drives the
    /// deferred-death transaction so the failure crosses into the parent.
    async fn dispose_failed_child(&self, task: &AgentDriverTask, error: &StageError) {
        let message = error.to_string();
        let last_attempt = task.attempt() + 1 > task.max_attempts();
        let terminal = matches!(error.to_error_kind().outcome(), ErrorOutcome::Terminal);

        if terminal || last_attempt {
            self.deferred_death(task, &message).await;
        } else {
            self.requeue_driver_task(task, &message).await;
        }
    }

    /// Drives the deferred-death transaction for a permanently failed
    /// child, falling back to a bare dispose when the parent cannot be
    /// resolved (an orphaned child with no launching record).
    async fn deferred_death(&self, task: &AgentDriverTask, message: &str) {
        let Some(claim_token) = task.claim_token() else {
            return;
        };
        let Ok(mut conn) = self.pool().acquire().await else {
            tracing::warn!("driver loop pool acquire failed for deferred death");
            return;
        };
        // A missing row is a genuine orphan: dead-letter the bare driver
        // task. A read error is transient and must not be conflated with
        // that, lest a DB hiccup dead-letter the only retry handle and
        // strand a suspended parent with a live child; leave the claimed
        // task for reclaim to re-drive instead.
        let child = match PgAgentThreadRepository
            .find_by_id(&mut conn, task.thread_id())
            .await
        {
            Ok(Some(child)) => child,
            Ok(None) => {
                self.dispose_driver_task(task, message).await;
                return;
            }
            Err(e) => {
                tracing::warn!(
                    driver_task_id = %task.id(),
                    error = %e,
                    "deferred death could not read the child; leaving the task for reclaim",
                );
                return;
            }
        };
        let parent = match child.parent_thread_id() {
            Some(parent_id) => self.load_parent_resolution(&mut conn, parent_id).await,
            None => Ok(None),
        };
        let parent = match parent {
            Ok(Some(parent)) => parent,
            Ok(None) => {
                self.dispose_driver_task(task, message).await;
                return;
            }
            Err(e) => {
                tracing::warn!(
                    driver_task_id = %task.id(),
                    error = %e,
                    "deferred death could not resolve the parent; leaving the task for reclaim",
                );
                return;
            }
        };
        if let Err(e) =
            commit_deferred_death(&mut conn, &child, task.id(), claim_token, &parent, message).await
        {
            if matches!(e, AgentRuntimeError::DrivingTaskNotBlocked { .. }) {
                // The parent is in an unmodelled non-blocked state, so the
                // hand-back cannot wake it and a reclaim would re-drive the
                // dead child forever, each cycle a fresh model call.
                // Dead-letter the driver task to terminate the loop; the
                // orphaned child is left for a sweep, but no further spend
                // accrues.
                self.dispose_driver_task(task, message).await;
            } else {
                tracing::warn!(
                    driver_task_id = %task.id(),
                    error = %e,
                    "deferred-death transaction failed; leaving the task for reclaim",
                );
            }
        }
    }

    /// Re-queues a claimed driver task with an incremented attempt and a
    /// backoff.
    async fn requeue_driver_task(&self, task: &AgentDriverTask, message: &str) {
        let Some(claim_token) = task.claim_token() else {
            return;
        };
        let Ok(mut conn) = self.pool().acquire().await else {
            return;
        };
        let backoff = 2u32.saturating_pow(task.attempt());
        let available_at = chrono::Utc::now() + chrono::Duration::seconds(i64::from(backoff));
        if let Err(e) = PgAgentDriverTaskRepository
            .requeue(
                &mut conn,
                task.id(),
                claim_token,
                task.attempt() + 1,
                available_at,
                message,
            )
            .await
        {
            tracing::warn!(driver_task_id = %task.id(), error = %e, "driver task requeue failed");
        }
    }

    /// Dead-letters a claimed driver task that cannot be dispositioned any
    /// other way.
    async fn dispose_driver_task(&self, task: &AgentDriverTask, message: &str) {
        let Some(claim_token) = task.claim_token() else {
            return;
        };
        let Ok(mut conn) = self.pool().acquire().await else {
            return;
        };
        if let Err(e) = PgAgentDriverTaskRepository
            .dead_letter(&mut conn, task.id(), claim_token, message)
            .await
        {
            tracing::warn!(driver_task_id = %task.id(), error = %e, "driver task dead-letter failed");
        }
    }

    /// One reclaim pass over stale driver-task leases, each disposed
    /// through the same path a fresh failure takes.
    async fn reclaim_stale_driver_tasks(&self) {
        let timeout_seconds =
            u32::try_from(self.config().task_timeout().as_secs()).unwrap_or(u32::MAX);
        for _ in 0..DRIVER_CLAIM_BATCH {
            let stale = {
                let Ok(mut conn) = self.pool().acquire().await else {
                    return;
                };
                match PgAgentDriverTaskRepository
                    .lock_stale(&mut conn, timeout_seconds)
                    .await
                {
                    Ok(Some(task)) => task,
                    Ok(None) => return,
                    Err(e) => {
                        tracing::warn!(error = %e, "driver reclaim scan failed");
                        return;
                    }
                }
            };
            tracing::warn!(driver_task_id = %stale.id(), "reclaiming a stale driver task");
            self.dispose_failed_child(&stale, &StageError::OwnershipLost)
                .await;
        }
    }

    async fn acquire_driver_conn(
        &self,
    ) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, StageError> {
        self.pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: "driver".into(),
                context: "acquiring a driver connection".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })
    }
}

/// The child's binding-pinned sampling rides a fresh request; the
/// conversation is the adopted committed input.
fn request_from(
    conversation: &RenderedConversation,
    parameters: &StageParameters,
) -> CompletionRequest {
    CompletionRequest {
        system: conversation.system.clone(),
        messages: conversation
            .messages
            .iter()
            .map(|m| {
                if m.role == Role::Assistant.as_str() {
                    Message::Assistant {
                        content: m.content.clone(),
                        tool_calls: vec![],
                    }
                } else {
                    Message::User {
                        content: m.content.clone(),
                    }
                }
            })
            .collect(),
        tools: vec![],
        temperature: narrow_temperature(parameters.temperature),
        max_tokens: parameters.max_tokens,
        // A child renders its own output contract into the record; the
        // verifier's verdict schema rides through here so the response is
        // schema-constrained without the driver loop knowing the shape.
        response_format: conversation
            .response_schema
            .clone()
            .map(|schema| ResponseFormat::JsonSchema { schema }),
    }
}

/// The child's ledger attribution: thread-keyed, no stage task to
/// reference, carrying the driver attempt so a reclaimed run's orphaned
/// row stays thread-attributed.
fn child_attribution(child: &AgentThread, attempt: u32) -> UsageAttribution {
    UsageAttribution {
        owner: UsageOwner::Thread {
            job_id: child.job_id(),
            thread_id: child.id(),
            record_id: None,
            attempt: clamp_to_i32(attempt),
        },
        system_prompt_version_id: None,
        user_prompt_version_id: None,
        trace_id: tribal_telemetry::current_trace_id(),
    }
}

fn driver_runtime_error(context: &str, source: AgentRuntimeError) -> StageError {
    crate::worker::map_runtime_error("driver", context, source)
}

fn driver_db(context: &str, source: tribal_db::DbError) -> StageError {
    StageError::Database {
        stage: "driver".into(),
        context: context.into(),
        source,
    }
}

fn driver_sqlx(context: &str, source: sqlx::Error) -> StageError {
    driver_db(
        context,
        tribal_db::DbError::QueryFailed {
            context: context.into(),
            source,
        },
    )
}
