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

use std::sync::{Arc, Mutex, MutexGuard};

use tribal_db::{
    GraphIdentityRepository, PgGraphIdentityRepository, PgReindexRunRepository,
    PgReindexTaskRepository, ReindexRunRepository, ReindexTaskRepository,
};
use tribal_domain::{ReindexRunState, StorageTransitionId};
use tribal_wire::management::{
    BlockingResolution, ConfigRevision, GraphActivity, GraphActivityKind, GraphActivityStatus,
    Revisioned, StorageTransitionAssessment, StorageTransitionVerdict,
};

use super::{
    database::{DatabaseAccess, DatabaseAccessError},
    operation::OperationContext,
};

/// The shared local barrier and assessment service; one instance per manager.
#[derive(Clone, Debug)]
pub(in crate::management) struct StorageTransitionGate {
    occupancy: Arc<Mutex<Occupancy>>,
}

/// What currently occupies the local barrier.
#[derive(Debug, Default)]
struct Occupancy {
    transition: Option<StorageTransitionId>,
    lifecycle: usize,
    migrations: usize,
}

/// The kinds a released guard decrements.
#[derive(Debug, Clone, Copy)]
enum AdmittedKind {
    Lifecycle,
    Migration,
}

/// An admitted migration or lifecycle occupancy, held until the operation's
/// terminal reply; dropping it releases the slot.
#[derive(Debug)]
pub(in crate::management) struct LocalAdmissionGuard {
    occupancy: Arc<Mutex<Occupancy>>,
    kind: AdmittedKind,
}

impl Drop for LocalAdmissionGuard {
    fn drop(&mut self) {
        let mut occupancy = lock(&self.occupancy);
        match self.kind {
            AdmittedKind::Lifecycle => occupancy.lifecycle = occupancy.lifecycle.saturating_sub(1),
            AdmittedKind::Migration => {
                occupancy.migrations = occupancy.migrations.saturating_sub(1);
            }
        }
    }
}

impl StorageTransitionGate {
    pub(in crate::management) fn new() -> Self {
        Self {
            occupancy: Arc::new(Mutex::new(Occupancy::default())),
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
        occupancy.migrations += 1;
        Ok(LocalAdmissionGuard {
            occupancy: Arc::clone(&self.occupancy),
            kind: AdmittedKind::Migration,
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
        occupancy.lifecycle += 1;
        Ok(LocalAdmissionGuard {
            occupancy: Arc::clone(&self.occupancy),
            kind: AdmittedKind::Lifecycle,
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
                    Some(wait) => GraphActivityStatus::WaitingForRetry {
                        resume_at_unix_ms: u64::try_from(wait.resume_at.timestamp_millis()).ok(),
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

        let (migrations, lifecycle) = {
            let occupancy = lock(&self.occupancy);
            (occupancy.migrations, occupancy.lifecycle)
        };
        if migrations > 0 {
            activities.push(GraphActivity {
                kind: GraphActivityKind::DatabaseMigration,
                status: GraphActivityStatus::Running,
                resolution: BlockingResolution::Wait,
            });
        }
        if lifecycle > 0 {
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
    use tribal_domain::StorageTransitionId;
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
        worker::ConfigWorkerRuntime,
    ) {
        let temp = tempfile::tempdir().expect("temporary config root");
        let path = temp.path().join("tribal.yaml");
        let config = TribalConfig::minimum_valid(database_url);
        std::fs::write(&path, serde_yaml::to_string(&config).unwrap()).unwrap();
        let (worker, runtime) = worker::spawn(ConfigAuthority::new(path)).unwrap();
        (temp, DatabaseAccess::new(worker), runtime)
    }

    #[tokio::test]
    async fn test_assessment_is_ready_with_the_source_identity_when_nothing_is_live() {
        let ctx = TestDb::new().await;
        let (_temp, database, _runtime) = database_access(ctx.database_url());
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
        let (_temp, database, _runtime) = database_access(ctx.database_url());
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
        let (_temp, database, _runtime) = database_access(ctx.database_url());
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

    #[tokio::test]
    async fn test_assessment_projects_admitted_migration_and_lifecycle_occupancy() {
        let ctx = TestDb::new().await;
        let (_temp, database, _runtime) = database_access(ctx.database_url());
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
