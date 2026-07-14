//! Manager-clock thread retention.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use tribal_db::{
    AgentThreadRepository as _, DbError, PgAgentThreadRepository, ThreadPruneCriteria,
};
use tribal_wire::management::{
    MutationMode, ThreadPruneApplied, ThreadPruneOutcome, ThreadPrunePlan, ThreadPruneRequest,
    ThreadPruneResult,
};

use super::database::{DatabaseAccess, DatabaseAccessError};

const THREAD_PRUNE_ROOT_LIMIT: u32 = 500;

type ManagerClock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub(super) enum ThreadAdministrationError {
    #[error(transparent)]
    Session(#[from] DatabaseAccessError),
    #[error("thread retention interval exceeds the manager clock range")]
    Retention,
    #[error("thread retention database operation failed: {source}")]
    Database {
        #[source]
        source: DbError,
    },
}

#[derive(Clone)]
pub(super) struct ThreadAdministration {
    database: DatabaseAccess,
    clock: ManagerClock,
    root_limit: u32,
}

impl ThreadAdministration {
    pub(super) fn new(database: DatabaseAccess) -> Self {
        Self {
            database,
            clock: Arc::new(Utc::now),
            root_limit: THREAD_PRUNE_ROOT_LIMIT,
        }
    }

    #[cfg(test)]
    fn with_policy(database: DatabaseAccess, clock: ManagerClock, root_limit: u32) -> Self {
        Self {
            database,
            clock,
            root_limit,
        }
    }

    pub(super) async fn prune(
        &self,
        request: ThreadPruneRequest,
    ) -> Result<ThreadPruneResult, ThreadAdministrationError> {
        let session = self
            .database
            .mutation_session(&request.expected_revision)
            .await?;
        let interval = Duration::try_days(i64::from(request.older_than.get()))
            .ok_or(ThreadAdministrationError::Retention)?;
        let completed_before = (self.clock)()
            .checked_sub_signed(interval)
            .ok_or(ThreadAdministrationError::Retention)?;
        let criteria = ThreadPruneCriteria {
            completed_before,
            stage: request.stage,
            cascade: request.cascade,
            root_limit: self.root_limit,
        };
        let mut transaction = session.pool.begin().await.map_err(transaction_error)?;
        let refused = PgAgentThreadRepository
            .count_refused_prune_roots(&mut transaction, &criteria)
            .await
            .map_err(database_error)?;
        let pruned = PgAgentThreadRepository
            .prune_threads(&mut transaction, &criteria)
            .await
            .map_err(database_error)?;
        let outcome = match request.mode {
            MutationMode::Preview => {
                transaction.rollback().await.map_err(transaction_error)?;
                ThreadPruneOutcome::Preview {
                    plan: ThreadPrunePlan {
                        eligible: pruned,
                        refused,
                    },
                }
            }
            MutationMode::Apply => {
                transaction.commit().await.map_err(transaction_error)?;
                ThreadPruneOutcome::Applied {
                    result: ThreadPruneApplied { pruned, refused },
                }
            }
        };
        Ok(session.revisioned(outcome))
    }
}

fn transaction_error(source: sqlx::Error) -> ThreadAdministrationError {
    database_error(DbError::QueryFailed {
        context: "thread retention transaction failed".to_owned(),
        source,
    })
}

fn database_error(source: DbError) -> ThreadAdministrationError {
    ThreadAdministrationError::Database { source }
}

#[cfg(test)]
mod tests {
    use tribal_db::{
        AgentBindingVersionRepository as _, AgentThreadRepository as _, DrivingTaskRef,
        JobRepository as _, NewAgentBindingVersion, NewAgentThread,
        PgAgentBindingVersionRepository, PgAgentThreadRepository, PgJobRepository,
        PgPrincipalRepository, PgProjectRepository, PgTaskRepository, PrincipalRepository as _,
        ProjectRepository as _, TaskRepository as _,
    };
    use tribal_domain::{
        AGENT_THREAD_FORMAT_VERSION, AgentThread, AgentThreadId, AgentThreadStage,
        AgentThreadStatus, AgentThreadTerminal, GitRemote, TaskType,
    };
    use tribal_test_utils::{
        a_new_job, a_new_principal, a_new_project, a_new_prompt_version, a_new_system_fingerprint,
        a_new_task, an_agent_definition, insert_prompt_version, shift_timestamp_by_id,
        upsert_system_fingerprint,
    };
    use tribal_wire::management::{ConfigRevision, RetentionDays};

    use super::*;
    use crate::management::{configuration::ConfigAuthority, worker};

    const BINDING_HASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    struct Harness {
        database: tribal_test_utils::TestDb,
        temp: tempfile::TempDir,
        worker_runtime: worker::ConfigWorkerRuntime,
        administration: ThreadAdministration,
        revision: ConfigRevision,
    }

    impl Harness {
        async fn new(root_limit: u32) -> Self {
            let database = tribal_test_utils::TestDb::new().await;
            let temp = tempfile::tempdir().expect("temporary thread-retention root");
            let config_path = temp.path().join("tribal.yaml");
            let config = tribal_config::TribalConfig::minimum_valid(database.database_url());
            std::fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();
            let (worker, worker_runtime) =
                worker::spawn(ConfigAuthority::new(config_path)).expect("config worker starts");
            let revision = worker.resolved_snapshot().await.unwrap().revision;
            let administration = ThreadAdministration::with_policy(
                DatabaseAccess::new(worker),
                Arc::new(Utc::now),
                root_limit,
            );
            Self {
                database,
                temp,
                worker_runtime,
                administration,
                revision,
            }
        }

        fn request(&self, cascade: bool, mode: MutationMode) -> ThreadPruneRequest {
            ThreadPruneRequest {
                expected_revision: self.revision.clone(),
                older_than: RetentionDays::try_from(1).unwrap(),
                stage: None,
                cascade,
                mode,
            }
        }

        fn shutdown(self) {
            let Self {
                database,
                temp,
                worker_runtime,
                administration,
                revision: _,
            } = self;
            drop(administration);
            worker_runtime.join().unwrap();
            drop(temp);
            drop(database);
        }
    }

    async fn insert_thread(
        connection: &mut sqlx::PgConnection,
        suffix: &str,
        parent: Option<AgentThreadId>,
    ) -> AgentThread {
        let principal = PgPrincipalRepository
            .insert(
                connection,
                &a_new_principal()
                    .principal_key(format!("user:retention-{suffix}"))
                    .build(),
            )
            .await
            .unwrap();
        let project = PgProjectRepository
            .insert(
                connection,
                &a_new_project()
                    .git_remote(GitRemote::from_parts(
                        "github.com",
                        &format!("test/retention-{suffix}"),
                        None,
                    ))
                    .build(),
            )
            .await
            .unwrap();
        let prompt = insert_prompt_version(connection, &a_new_prompt_version().build()).await;
        let fingerprint =
            upsert_system_fingerprint(connection, &a_new_system_fingerprint().build()).await;
        let job = PgJobRepository
            .insert(
                connection,
                &a_new_job()
                    .project_id(project.id())
                    .principal_id(principal.id())
                    .extraction_system_prompt_version_id(prompt)
                    .extraction_user_prompt_version_id(prompt)
                    .triage_system_prompt_version_id(prompt)
                    .triage_user_prompt_version_id(prompt)
                    .relation_system_prompt_version_id(prompt)
                    .relation_user_prompt_version_id(prompt)
                    .system_fingerprint_hash(fingerprint)
                    .build(),
            )
            .await
            .unwrap();
        let task = PgTaskRepository
            .insert(connection, &a_new_task().job_id(job.id()).build())
            .await
            .unwrap();
        let binding = PgAgentBindingVersionRepository
            .record(
                connection,
                &NewAgentBindingVersion::builder()
                    .hash(BINDING_HASH.to_owned())
                    .pipeline_stage(TaskType::Extraction)
                    .definition(an_agent_definition().build())
                    .build(),
            )
            .await
            .unwrap();
        PgAgentThreadRepository
            .insert(
                connection,
                &NewAgentThread::builder()
                    .parent_thread_id(parent)
                    .stage(AgentThreadStage::Product(TaskType::Extraction))
                    .binding_version_id(Some(binding.id()))
                    .driving_task(DrivingTaskRef::Stage(task.id()))
                    .principal_id(Some(principal.id()))
                    .format_version(AGENT_THREAD_FORMAT_VERSION)
                    .build(),
            )
            .await
            .unwrap()
    }

    async fn complete(connection: &mut sqlx::PgConnection, thread: &AgentThread, aged: bool) {
        PgAgentThreadRepository
            .mark_running(connection, thread.id(), AgentThreadStatus::Queued)
            .await
            .unwrap();
        PgAgentThreadRepository
            .complete(
                connection,
                thread.id(),
                AgentThreadTerminal::Completed,
                AgentThreadStatus::Running,
            )
            .await
            .unwrap();
        if aged {
            shift_timestamp_by_id(
                connection,
                "agent_threads",
                "completed_at",
                *thread.id().inner(),
                -Duration::days(2),
            )
            .await;
        }
    }

    async fn thread_count(database: &tribal_test_utils::TestDb) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_threads")
            .fetch_one(database.pool())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_preview_is_zero_effect_and_apply_is_root_bounded() {
        let harness = Harness::new(1).await;
        let mut connection = harness.database.pool().acquire().await.unwrap();
        let first = insert_thread(&mut connection, "bounded-first", None).await;
        let second = insert_thread(&mut connection, "bounded-second", None).await;
        complete(&mut connection, &first, true).await;
        complete(&mut connection, &second, true).await;
        drop(connection);

        let preview = harness
            .administration
            .prune(harness.request(false, MutationMode::Preview))
            .await
            .unwrap();
        assert!(matches!(
            preview.value,
            ThreadPruneOutcome::Preview {
                plan: ThreadPrunePlan {
                    eligible: 1,
                    refused: 0,
                }
            }
        ));
        assert_eq!(thread_count(&harness.database).await, 2);

        let applied = harness
            .administration
            .prune(harness.request(false, MutationMode::Apply))
            .await
            .unwrap();
        assert!(matches!(
            applied.value,
            ThreadPruneOutcome::Applied {
                result: ThreadPruneApplied {
                    pruned: 1,
                    refused: 0,
                }
            }
        ));
        assert_eq!(thread_count(&harness.database).await, 1);
        harness.shutdown();
    }

    #[tokio::test]
    async fn test_cascade_collects_terminal_descendants_but_refuses_live_ones() {
        let harness = Harness::new(10).await;
        let mut connection = harness.database.pool().acquire().await.unwrap();
        let root = insert_thread(&mut connection, "terminal-root", None).await;
        let child = insert_thread(&mut connection, "terminal-child", Some(root.id())).await;
        complete(&mut connection, &root, true).await;
        complete(&mut connection, &child, false).await;
        drop(connection);

        let refused = harness
            .administration
            .prune(harness.request(false, MutationMode::Preview))
            .await
            .unwrap();
        assert!(matches!(
            refused.value,
            ThreadPruneOutcome::Preview {
                plan: ThreadPrunePlan {
                    eligible: 0,
                    refused: 1,
                }
            }
        ));
        let cascaded = harness
            .administration
            .prune(harness.request(true, MutationMode::Preview))
            .await
            .unwrap();
        assert!(matches!(
            cascaded.value,
            ThreadPruneOutcome::Preview {
                plan: ThreadPrunePlan {
                    eligible: 2,
                    refused: 0,
                }
            }
        ));
        assert_eq!(thread_count(&harness.database).await, 2);
        harness
            .administration
            .prune(harness.request(true, MutationMode::Apply))
            .await
            .unwrap();
        assert_eq!(thread_count(&harness.database).await, 0);

        let mut connection = harness.database.pool().acquire().await.unwrap();
        let live_root = insert_thread(&mut connection, "live-root", None).await;
        insert_thread(&mut connection, "live-child", Some(live_root.id())).await;
        complete(&mut connection, &live_root, true).await;
        drop(connection);
        let live_refused = harness
            .administration
            .prune(harness.request(true, MutationMode::Preview))
            .await
            .unwrap();
        assert!(matches!(
            live_refused.value,
            ThreadPruneOutcome::Preview {
                plan: ThreadPrunePlan {
                    eligible: 0,
                    refused: 1,
                }
            }
        ));
        assert_eq!(thread_count(&harness.database).await, 2);
        harness.shutdown();
    }
}
