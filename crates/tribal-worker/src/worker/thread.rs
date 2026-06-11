//! Stage assembly for the thread runtime: bindings and thread setup.
//!
//! The worker derives each stage's agent definition from the boot-time
//! stage specs and the job's prompt versions — bindings are populated
//! from the existing configuration, never replacing it — then hands
//! execution to the runtime. The default binding is one-shot with no
//! tools and no budget caps, reproducing launched behaviour exactly.

use tribal_agent_runtime::{AgentRuntimeError, StageThread, ensure_stage_thread, resolve_binding};
use tribal_db::{PgPromptVersionRepository, PromptVersionRepository};
use tribal_domain::{
    AgentDefinition, ExecutionBudgets, Job, PromptVersionId, StageExecutorKind, Task,
};
use tribal_inference::{CompletionStageSpec, CompletionStageSpecs};

use crate::{error::StageError, worker::Worker};

impl Worker {
    /// Establishes the thread a claimed stage task drives: derives the
    /// stage's definition, resolves its content-addressed binding, and
    /// finds or creates the thread.
    ///
    /// Runs on a fresh connection before the stage executes, on the claim
    /// side of the no-transaction-across-inference rule.
    pub(crate) async fn establish_stage_thread(
        &self,
        job: &Job,
        task: &Task,
    ) -> Result<StageThread, StageError> {
        let stage = task.task_type().as_str();
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "acquiring connection for thread setup".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;

        let definition = self
            .stage_definition(&mut conn, job, task)
            .await
            .map_err(|source| map_runtime_error(stage, "deriving the stage binding", source))?;
        let binding = resolve_binding(&mut conn, &definition)
            .await
            .map_err(|source| map_runtime_error(stage, "resolving the binding version", source))?;

        ensure_stage_thread(&mut conn, job, task, binding.id())
            .await
            .map_err(|source| map_runtime_error(stage, "establishing the thread", source))
    }

    /// Builds the stage's definition from its boot-time endpoint spec and
    /// the job's prompt versions' content hashes, so a prompt edit, model
    /// change, or endpoint change is a new binding version.
    async fn stage_definition(
        &self,
        conn: &mut sqlx::PgConnection,
        job: &Job,
        task: &Task,
    ) -> Result<AgentDefinition, AgentRuntimeError> {
        let spec = stage_spec(self.stage_specs(), task);
        let (system_pv_id, user_pv_id) = crate::stages::prompt_version_ids_for_task(job, task);

        let system_hash = prompt_hash(conn, system_pv_id).await?;
        let user_hash = prompt_hash(conn, user_pv_id).await?;

        Ok(AgentDefinition::builder()
            .pipeline_stage(task.task_type())
            .executor(StageExecutorKind::OneShot)
            .provider(spec.provider)
            .model(spec.model.clone())
            .base_url(spec.base_url.clone())
            .prompt_hashes(vec![system_hash, user_hash])
            .budgets(ExecutionBudgets::default())
            .tools(Vec::new())
            .build())
    }
}

/// Selects the boot-time endpoint spec for a task's stage.
fn stage_spec<'a>(specs: &'a CompletionStageSpecs, task: &Task) -> &'a CompletionStageSpec {
    match task.task_type() {
        tribal_domain::TaskType::Extraction => &specs.extraction,
        tribal_domain::TaskType::Triage => &specs.triage,
        tribal_domain::TaskType::Relation => &specs.relation,
    }
}

/// Reads one prompt version's content hash.
async fn prompt_hash(
    conn: &mut sqlx::PgConnection,
    id: PromptVersionId,
) -> Result<String, AgentRuntimeError> {
    let version = PgPromptVersionRepository
        .find_by_id(conn, id)
        .await
        .map_err(|source| AgentRuntimeError::Database {
            context: format!("loading prompt version {id} for the binding"),
            source,
        })?;
    Ok(version.content_hash().to_owned())
}

/// Maps a runtime error onto the stage error taxonomy: lost ownership
/// stays lost ownership, database faults stay database faults, and
/// consistency faults surface as internal errors.
pub(crate) fn map_runtime_error(
    stage: &str,
    context: &str,
    source: AgentRuntimeError,
) -> StageError {
    match source {
        AgentRuntimeError::StatusCasMissed { .. } | AgentRuntimeError::LeaseLost { .. } => {
            StageError::OwnershipLost
        }
        AgentRuntimeError::Database {
            context: inner,
            source,
        } => StageError::Database {
            stage: stage.into(),
            context: inner,
            source,
        },
        source @ (AgentRuntimeError::ThreadMissing { .. }
        | AgentRuntimeError::ContentSerialisation { .. }) => StageError::Runtime {
            context: context.to_owned(),
            source,
        },
    }
}
