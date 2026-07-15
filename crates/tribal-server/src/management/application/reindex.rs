//! Revision-bound reindex administration.

use std::sync::Arc;

use tribal_agent_runtime::PgLedgerSink;
use tribal_db::{DbError, PgPrincipalRepository, PrincipalRepository as _};
use tribal_domain::{LOCAL_PRINCIPAL_KEY, PrincipalId};
use tribal_inference::InferenceGateway;
use tribal_wire::management::{
    GenesisEmbeddingInput, GenesisProviderAvailability, ManagementResponseError, MutationMode,
    ReindexApplyResolution, ReindexCancelOutcome, ReindexCancelRequest, ReindexCancelResult,
    ReindexPlan, ReindexPruneCounts, ReindexPruneOutcome, ReindexPruneRequest, ReindexPruneResult,
    ReindexRunOutcome, ReindexRunRequest, ReindexRunResult,
};
use tribal_worker::{
    ReindexCancelOutcome as WorkerCancelOutcome, ReindexOpError,
    ReindexPruneOutcome as WorkerPruneOutcome, ReindexResolution,
    ReindexRunOutcome as WorkerRunOutcome, ReindexRunRequest as WorkerRunRequest,
    drop_superseded_indexes, reindex_cancel, reindex_prune, reindex_run,
};

use super::database::{DatabaseAccess, DatabaseAccessError, DatabaseSession};
use crate::{
    error::AppError,
    management::product::ProductSession,
    startup::{CatalogueCredentialResolver, build_command_registry, completion_stage_specs},
};

#[derive(Debug, thiserror::Error)]
pub(super) enum ReindexAdministrationError {
    #[error(transparent)]
    Session(#[from] DatabaseAccessError),
    #[error("reindex target is invalid")]
    Target,
    #[error("reindex product validation refused the target")]
    Public(ManagementResponseError),
    #[error("reindex gateway preparation failed: {source}")]
    Gateway {
        #[source]
        source: AppError,
    },
    #[error("reindex service failed: {source}")]
    Operation {
        #[source]
        source: ReindexOpError,
    },
    #[error("reindex database operation failed: {source}")]
    Database {
        #[source]
        source: DbError,
    },
}

#[derive(Clone)]
pub(super) struct ReindexAdministration {
    database: DatabaseAccess,
}

impl ReindexAdministration {
    pub(super) fn new(database: DatabaseAccess) -> Self {
        Self { database }
    }

    pub(super) async fn run(
        &self,
        product: &ProductSession,
        request: ReindexRunRequest,
    ) -> Result<ReindexRunResult, ReindexAdministrationError> {
        let session = self
            .database
            .mutation_session(&request.expected_revision)
            .await?;
        Self::validate_target(product, &request.target, &session).await?;
        let gateway = gateway(&session)?;
        let principal = match request.mode {
            MutationMode::Preview => PrincipalId::new(),
            MutationMode::Apply => local_principal(&session).await?,
        };
        let outcome = reindex_run(
            &session.pool,
            &gateway,
            &WorkerRunRequest {
                provider: request.target.provider.to_string(),
                model: request.target.model,
                dimensions: request.target.dimensions,
                base_url: request.target.base_url,
                dry_run: matches!(request.mode, MutationMode::Preview),
            },
            principal,
        )
        .await
        .map_err(|source| ReindexAdministrationError::Operation { source })?;
        Ok(session.revisioned(run_outcome(request.mode, outcome)?))
    }

    pub(super) async fn cancel(
        &self,
        request: ReindexCancelRequest,
    ) -> Result<ReindexCancelResult, ReindexAdministrationError> {
        let session = self
            .database
            .mutation_session(&request.expected_revision)
            .await?;
        let mut transaction = session.pool.begin().await.map_err(transaction_error)?;
        let outcome = reindex_cancel(&mut transaction)
            .await
            .map_err(database_error)?;
        transaction.commit().await.map_err(transaction_error)?;
        Ok(session.revisioned(match outcome {
            WorkerCancelOutcome::Cancelled(run_id) => ReindexCancelOutcome::Cancelled { run_id },
            WorkerCancelOutcome::NoLiveRun => ReindexCancelOutcome::NoLiveRun,
        }))
    }

    pub(super) async fn prune(
        &self,
        request: ReindexPruneRequest,
    ) -> Result<ReindexPruneResult, ReindexAdministrationError> {
        let session = self
            .database
            .mutation_session(&request.expected_revision)
            .await?;
        let mut transaction = session.pool.begin().await.map_err(transaction_error)?;
        let outcome = reindex_prune(&mut transaction)
            .await
            .map_err(database_error)?;
        let counts = prune_counts(&outcome);
        match request.mode {
            MutationMode::Preview => {
                transaction.rollback().await.map_err(transaction_error)?;
                Ok(session.revisioned(ReindexPruneOutcome::Preview { candidates: counts }))
            }
            MutationMode::Apply => {
                transaction.commit().await.map_err(transaction_error)?;
                drop_indexes(&session, &outcome).await;
                Ok(session.revisioned(ReindexPruneOutcome::Applied { deleted: counts }))
            }
        }
    }

    async fn validate_target(
        product: &ProductSession,
        target: &GenesisEmbeddingInput,
        session: &DatabaseSession,
    ) -> Result<(), ReindexAdministrationError> {
        let options = product
            .genesis_options()
            .await
            .map_err(ReindexAdministrationError::Public)?;
        if options.revision != session.revision {
            return Err(ReindexAdministrationError::Public(
                ManagementResponseError {
                    message: "configuration changed before reindex validation".to_owned(),
                    error: tribal_wire::management::ManagementError::ConfigConflict {
                        expected: session.revision.clone(),
                        actual: options.revision,
                    },
                },
            ));
        }
        let available = options.providers.into_iter().any(|option| {
            option.provider == target.provider
                && matches!(
                    option.availability,
                    GenesisProviderAvailability::Available { .. }
                )
        });
        let model_valid =
            !target.model.is_empty() && !target.model.chars().any(char::is_whitespace);
        let dimensions_valid = target.dimensions.is_none_or(|dimensions| {
            (1..=tribal_domain::MAX_EMBEDDING_DIMENSIONS).contains(&dimensions)
        });
        if available && model_valid && dimensions_valid {
            Ok(())
        } else {
            Err(ReindexAdministrationError::Target)
        }
    }
}

fn gateway(session: &DatabaseSession) -> Result<InferenceGateway, ReindexAdministrationError> {
    let registry = build_command_registry(&session.config)
        .map_err(|source| ReindexAdministrationError::Gateway { source })?;
    let sink = Arc::new(PgLedgerSink::new(
        session.pool.clone(),
        tribal_telemetry::noop_recorder(),
    ));
    InferenceGateway::new(
        registry,
        &completion_stage_specs(&session.config),
        Arc::new(CatalogueCredentialResolver::new(
            session.config.credentials.clone(),
        )),
        sink,
    )
    .map_err(|source| ReindexAdministrationError::Gateway {
        source: AppError::ProviderSetup {
            context: source.to_string(),
        },
    })
}

async fn local_principal(
    session: &DatabaseSession,
) -> Result<PrincipalId, ReindexAdministrationError> {
    let mut connection = session.pool.acquire().await.map_err(|source| {
        database_error(DbError::QueryFailed {
            context: "acquiring reindex principal connection".to_owned(),
            source,
        })
    })?;
    PgPrincipalRepository
        .find_by_key(&mut connection, LOCAL_PRINCIPAL_KEY)
        .await
        .map_err(database_error)?
        .ok_or_else(|| {
            database_error(DbError::NotFound {
                entity: "principal",
                id: LOCAL_PRINCIPAL_KEY.to_owned(),
            })
        })
        .map(|principal| principal.id())
}

fn run_outcome(
    mode: MutationMode,
    outcome: WorkerRunOutcome,
) -> Result<ReindexRunOutcome, ReindexAdministrationError> {
    let plan = ReindexPlan {
        provider: outcome.provider,
        model: outcome.model,
        dimensions: outcome.dimensions,
        normalised_base_url: outcome.normalised_base_url,
        estimated_items: outcome.estimated_items,
        estimated_tags: outcome.estimated_tags,
    };
    match (mode, outcome.resolution) {
        (MutationMode::Preview, ReindexResolution::Plan) => Ok(ReindexRunOutcome::Preview { plan }),
        (MutationMode::Apply, resolution) => Ok(ReindexRunOutcome::Applied {
            plan,
            resolution: match resolution {
                ReindexResolution::Created { run_id } => ReindexApplyResolution::Created { run_id },
                ReindexResolution::AlreadyLive { run_id } => {
                    ReindexApplyResolution::AlreadyLive { run_id }
                }
                ReindexResolution::Unchanged => ReindexApplyResolution::Unchanged,
                ReindexResolution::LockContended => ReindexApplyResolution::LockContended,
                ReindexResolution::Plan => return Err(ReindexAdministrationError::Target),
            },
        }),
        (MutationMode::Preview, _) => Err(ReindexAdministrationError::Target),
    }
}

fn prune_counts(outcome: &WorkerPruneOutcome) -> ReindexPruneCounts {
    ReindexPruneCounts {
        profiles: outcome.profiles_superseded,
        embeddings: outcome.embeddings_deleted,
        tag_embeddings: outcome.tag_embeddings_deleted,
    }
}

async fn drop_indexes(session: &DatabaseSession, outcome: &WorkerPruneOutcome) {
    if !outcome.superseded_epochs.is_empty()
        && let Ok(mut connection) = session.pool.acquire().await
    {
        drop_superseded_indexes(&mut connection, &outcome.superseded_epochs).await;
    }
}

fn transaction_error(source: sqlx::Error) -> ReindexAdministrationError {
    database_error(DbError::QueryFailed {
        context: "reindex transaction failed".to_owned(),
        source,
    })
}

fn database_error(source: DbError) -> ReindexAdministrationError {
    ReindexAdministrationError::Database { source }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        Json, Router,
        extract::State,
        routing::{get, post},
    };
    use tribal_db::{
        AdvisoryLockRepository as _, EmbeddingProfileRepository as _, PgAdvisoryLockRepository,
        PgEmbeddingProfileRepository, advisory_locks,
    };
    use tribal_domain::{EmbeddingProfileState, ProviderKind};
    use tribal_wire::management::{ConfigRevision, ReindexApplyResolution};

    use super::*;
    use crate::management::{
        application::database::find_or_create_principal,
        configuration::ConfigAuthority,
        product::{ProductService, ProductSession},
        worker::{self, ConfigWorkerRuntime},
    };

    struct Harness {
        database: tribal_test_utils::TestDb,
        temp: tempfile::TempDir,
        worker_runtime: ConfigWorkerRuntime,
        product: ProductSession,
        administration: ReindexAdministration,
        revision: ConfigRevision,
    }

    impl Harness {
        async fn new() -> Self {
            let database = tribal_test_utils::TestDb::new().await;
            let temp = tempfile::tempdir().expect("temporary reindex root");
            let config_path = temp.path().join("tribal.yaml");
            let config = tribal_config::TribalConfig::minimum_valid(database.database_url());
            std::fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();
            let (worker, worker_runtime) =
                worker::spawn(ConfigAuthority::new(config_path)).expect("config worker starts");
            let revision = worker.resolved_snapshot().await.unwrap().revision;
            let product = ProductService::new(worker.clone()).session();
            let administration = ReindexAdministration::new(DatabaseAccess::new(worker));
            Self {
                database,
                temp,
                worker_runtime,
                product,
                administration,
                revision,
            }
        }

        fn shutdown(self) {
            let Self {
                database,
                temp,
                worker_runtime,
                product,
                administration,
                revision: _,
            } = self;
            drop(administration);
            drop(product);
            worker_runtime.join().unwrap();
            drop(temp);
            drop(database);
        }

        fn run_request(&self, base_url: &str, mode: MutationMode) -> ReindexRunRequest {
            ReindexRunRequest {
                expected_revision: self.revision.clone(),
                target: GenesisEmbeddingInput {
                    provider: ProviderKind::Ollama,
                    model: "test-embedding".to_owned(),
                    dimensions: Some(3),
                    base_url: Some(base_url.to_owned()),
                },
                mode,
            }
        }
    }

    struct ProbeServer {
        base_url: String,
        embeds: Arc<AtomicUsize>,
        task: tokio::task::JoinHandle<()>,
    }

    impl ProbeServer {
        async fn start() -> Self {
            let embeds = Arc::new(AtomicUsize::new(0));
            let app = Router::new()
                .route("/api/embed", post(embed))
                .route("/api/tags", get(tags))
                .with_state(Arc::clone(&embeds));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("probe listener binds");
            let address = listener.local_addr().expect("probe address resolves");
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.expect("probe server runs");
            });
            Self {
                base_url: format!("http://{address}"),
                embeds,
                task,
            }
        }

        async fn shutdown(self) {
            self.task.abort();
            let _ = self.task.await;
        }
    }

    async fn embed(State(calls): State<Arc<AtomicUsize>>) -> Json<serde_json::Value> {
        calls.fetch_add(1, Ordering::SeqCst);
        Json(serde_json::json!({
            "embeddings": [[0.25, 0.25, 0.25]],
            "prompt_eval_count": 1,
        }))
    }

    async fn tags() -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "models": [{ "name": "test-embedding", "digest": "sha256:test" }]
        }))
    }

    async fn row_count(database: &tribal_test_utils::TestDb, table: &str) -> i64 {
        let query = format!("SELECT COUNT(*) FROM {table}");
        sqlx::query_scalar::<_, i64>(&query)
            .fetch_one(database.pool())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_preview_estimates_without_probe_ledger_or_durable_run() {
        let harness = Harness::new().await;
        let server = ProbeServer::start().await;
        let profiles_before = row_count(&harness.database, "embedding_profiles").await;
        let runs_before = row_count(&harness.database, "reindex_runs").await;
        let ledger_before = row_count(&harness.database, "token_usage").await;

        let result = harness
            .administration
            .run(
                &harness.product,
                harness.run_request(&server.base_url, MutationMode::Preview),
            )
            .await
            .expect("preview succeeds");

        assert!(matches!(result.value, ReindexRunOutcome::Preview { .. }));
        assert_eq!(server.embeds.load(Ordering::SeqCst), 0);
        assert_eq!(
            row_count(&harness.database, "embedding_profiles").await,
            profiles_before
        );
        assert_eq!(
            row_count(&harness.database, "reindex_runs").await,
            runs_before
        );
        assert_eq!(
            row_count(&harness.database, "token_usage").await,
            ledger_before
        );
        server.shutdown().await;
        harness.shutdown();
    }

    #[tokio::test]
    async fn test_apply_ledgers_one_probe_and_preserves_single_flight_and_cancel_idempotence() {
        let harness = Harness::new().await;
        let server = ProbeServer::start().await;
        let mut connection = harness.database.pool().acquire().await.unwrap();
        find_or_create_principal(&mut connection, LOCAL_PRINCIPAL_KEY)
            .await
            .unwrap();
        drop(connection);

        let created = harness
            .administration
            .run(
                &harness.product,
                harness.run_request(&server.base_url, MutationMode::Apply),
            )
            .await
            .expect("apply creates a run");
        let run_id = match created.value {
            ReindexRunOutcome::Applied {
                resolution: ReindexApplyResolution::Created { run_id },
                ..
            } => run_id,
            other => panic!("expected created run, got {other:?}"),
        };
        assert_eq!(server.embeds.load(Ordering::SeqCst), 1);
        assert_eq!(row_count(&harness.database, "token_usage").await, 1);
        assert_eq!(row_count(&harness.database, "reindex_runs").await, 1);

        let repeated = harness
            .administration
            .run(
                &harness.product,
                harness.run_request(&server.base_url, MutationMode::Apply),
            )
            .await
            .expect("repeat apply resumes the live run");
        assert!(matches!(
            repeated.value,
            ReindexRunOutcome::Applied {
                resolution: ReindexApplyResolution::AlreadyLive { run_id: id },
                ..
            } if id == run_id
        ));
        assert_eq!(row_count(&harness.database, "reindex_runs").await, 1);

        let cancelled = harness
            .administration
            .cancel(ReindexCancelRequest {
                expected_revision: harness.revision.clone(),
            })
            .await
            .expect("live run cancels");
        assert!(matches!(
            cancelled.value,
            ReindexCancelOutcome::Cancelled { run_id: id } if id == run_id
        ));
        let repeated_cancel = harness
            .administration
            .cancel(ReindexCancelRequest {
                expected_revision: harness.revision.clone(),
            })
            .await
            .expect("repeated cancel is idempotent");
        assert!(matches!(
            repeated_cancel.value,
            ReindexCancelOutcome::NoLiveRun
        ));
        assert_eq!(server.embeds.load(Ordering::SeqCst), 2);

        let mut lock = harness.database.pool().begin().await.unwrap();
        PgAdvisoryLockRepository
            .acquire_exclusive_xact(&mut lock, advisory_locks::REINDEX_SINGLE_FLIGHT)
            .await
            .unwrap();
        let contended = harness
            .administration
            .run(
                &harness.product,
                harness.run_request(&server.base_url, MutationMode::Apply),
            )
            .await
            .expect("contention is an outcome");
        assert!(matches!(
            contended.value,
            ReindexRunOutcome::Applied {
                resolution: ReindexApplyResolution::LockContended,
                ..
            }
        ));
        lock.rollback().await.unwrap();
        assert_eq!(row_count(&harness.database, "reindex_runs").await, 1);
        assert_eq!(row_count(&harness.database, "token_usage").await, 3);

        server.shutdown().await;
        harness.shutdown();
    }

    #[tokio::test]
    async fn test_prune_preview_rolls_back_and_apply_returns_the_same_counts() {
        let harness = Harness::new().await;
        let mut connection = harness.database.pool().acquire().await.unwrap();
        let profile = PgEmbeddingProfileRepository
            .insert(
                &mut connection,
                &tribal_test_utils::a_new_embedding_profile().build(),
            )
            .await
            .unwrap();
        PgEmbeddingProfileRepository
            .mark_failed(&mut connection, profile.id())
            .await
            .unwrap();
        drop(connection);

        let preview = harness
            .administration
            .prune(ReindexPruneRequest {
                expected_revision: harness.revision.clone(),
                mode: MutationMode::Preview,
            })
            .await
            .expect("prune preview succeeds");
        assert!(matches!(
            preview.value,
            ReindexPruneOutcome::Preview {
                candidates: ReindexPruneCounts {
                    profiles: 1,
                    embeddings: 0,
                    tag_embeddings: 0,
                }
            }
        ));
        let mut connection = harness.database.pool().acquire().await.unwrap();
        assert_eq!(
            PgEmbeddingProfileRepository
                .find_by_id(&mut connection, profile.id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            EmbeddingProfileState::Failed
        );
        drop(connection);

        let applied = harness
            .administration
            .prune(ReindexPruneRequest {
                expected_revision: harness.revision.clone(),
                mode: MutationMode::Apply,
            })
            .await
            .expect("prune apply succeeds");
        assert!(matches!(
            applied.value,
            ReindexPruneOutcome::Applied {
                deleted: ReindexPruneCounts {
                    profiles: 1,
                    embeddings: 0,
                    tag_embeddings: 0,
                }
            }
        ));
        let mut connection = harness.database.pool().acquire().await.unwrap();
        assert_eq!(
            PgEmbeddingProfileRepository
                .find_by_id(&mut connection, profile.id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            EmbeddingProfileState::Superseded
        );
        drop(connection);

        harness.shutdown();
    }
}
