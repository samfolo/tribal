//! Configured-target graph identity, resolved at bounded points and served
//! revision-coherently to the manager snapshot.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tribal_domain::GraphId;
use tribal_wire::management::{ConfigRevision, DatabaseTargetState};

use super::{
    application::{inspect_target_state, operation::OperationContext},
    worker::ConfigWorkerClient,
};

/// One resolution outcome bound to the exact configuration it was proved at.
#[derive(Clone)]
pub(crate) enum GraphIdentityResolution {
    /// No resolution has completed yet.
    Pending,
    /// A completed resolution for one configuration revision.
    Terminal {
        revision: ConfigRevision,
        database_url: String,
        graph_id: Option<GraphId>,
    },
}

/// Resolves and caches the configured target's graph identity.
///
/// Resolution happens only at bounded points — manager start, a
/// configuration change, session admission, and a terminal initialise or
/// bootstrap — so the snapshot path reads the cache and never opens a
/// database connection.
#[derive(Clone)]
pub(crate) struct GraphIdentityResolver {
    inner: Arc<ResolverInner>,
}

struct ResolverInner {
    config: ConfigWorkerClient,
    shutdown: CancellationToken,
    state: tokio::sync::watch::Sender<GraphIdentityResolution>,
    /// Serialises resolutions so concurrent admissions share one proof.
    resolving: tokio::sync::Mutex<()>,
}

impl GraphIdentityResolver {
    pub(crate) fn new(config: ConfigWorkerClient, shutdown: CancellationToken) -> Self {
        let (state, _) = tokio::sync::watch::channel(GraphIdentityResolution::Pending);
        Self {
            inner: Arc::new(ResolverInner {
                config,
                shutdown,
                state,
                resolving: tokio::sync::Mutex::new(()),
            }),
        }
    }

    /// Classifies the configured target afresh and publishes the outcome.
    ///
    /// The classification shares inspection's bounded window, so an
    /// unreachable target settles to a terminal `None` rather than stalling
    /// the caller.
    pub(crate) async fn reprove(&self) {
        let _guard = self.inner.resolving.lock().await;
        let Some((revision, url)) = self.configured_target().await else {
            return;
        };
        let graph_id = classify(&url).await;
        self.inner
            .state
            .send_replace(GraphIdentityResolution::Terminal {
                revision,
                database_url: url,
                graph_id,
            });
    }

    /// Re-publishes at the current revision, reconnecting only when the
    /// configured URL changed; a configuration edit elsewhere carries the
    /// existing proof forward to the new revision.
    pub(crate) async fn refresh(&self) {
        let _guard = self.inner.resolving.lock().await;
        let Some((revision, url)) = self.configured_target().await else {
            return;
        };
        let carried = match &*self.inner.state.borrow() {
            GraphIdentityResolution::Terminal {
                database_url,
                graph_id,
                ..
            } if *database_url == url => Some(*graph_id),
            GraphIdentityResolution::Terminal { .. } | GraphIdentityResolution::Pending => None,
        };
        let graph_id = match carried {
            Some(graph_id) => graph_id,
            None => classify(&url).await,
        };
        self.inner
            .state
            .send_replace(GraphIdentityResolution::Terminal {
                revision,
                database_url: url,
                graph_id,
            });
    }

    /// The identity published for one snapshot revision: the cached proof
    /// when it is bound to that exact revision, `None` otherwise.
    pub(crate) fn published(&self, revision: &ConfigRevision) -> Option<GraphId> {
        published_for(&self.inner.state.borrow(), revision)
    }

    #[cfg(test)]
    fn is_terminal(&self) -> bool {
        matches!(
            &*self.inner.state.borrow(),
            GraphIdentityResolution::Terminal { .. }
        )
    }

    async fn configured_target(&self) -> Option<(ConfigRevision, String)> {
        let operation = OperationContext::new(self.inner.shutdown.clone());
        match self
            .inner
            .config
            .for_operation(&operation)
            .resolved_snapshot()
            .await
        {
            Ok(snapshot) => {
                let url = snapshot.config.database.url.clone();
                Some((snapshot.revision, url))
            }
            Err(error) => {
                tracing::warn!(%error, "graph identity resolution could not read configuration");
                None
            }
        }
    }
}

/// A proof publishes only at the exact revision it was resolved against;
/// any other revision, and a pending resolution, fail closed.
fn published_for(
    resolution: &GraphIdentityResolution,
    revision: &ConfigRevision,
) -> Option<GraphId> {
    match resolution {
        GraphIdentityResolution::Terminal {
            revision: proved,
            graph_id,
            ..
        } if proved == revision => *graph_id,
        GraphIdentityResolution::Terminal { .. } | GraphIdentityResolution::Pending => None,
    }
}

/// Classifies one configured URL onto the snapshot's identity space; an
/// unconfigured target is a terminal `None` without a connection attempt.
async fn classify(url: &str) -> Option<GraphId> {
    if url.is_empty() {
        return None;
    }
    identity_of(&inspect_target_state(url).await)
}

/// Maps a target classification onto the identity space: only a ready
/// target proves an identity.
fn identity_of(state: &DatabaseTargetState) -> Option<GraphId> {
    match state {
        DatabaseTargetState::Ready { graph_id } => Some(*graph_id),
        DatabaseTargetState::Unavailable { .. }
        | DatabaseTargetState::Uninitialised
        | DatabaseTargetState::Behind { .. }
        | DatabaseTargetState::Ahead => None,
    }
}

#[cfg(test)]
mod tests {
    use tribal_config::TribalConfig;
    use tribal_wire::management::{
        ConfigDigest, DatabaseTargetFailure, DatabaseTargetFailureKind, FailurePresentation,
    };

    use super::*;
    use crate::management::{configuration::ConfigAuthority, worker};

    fn failure() -> DatabaseTargetFailure {
        DatabaseTargetFailure {
            kind: DatabaseTargetFailureKind::HostUnreachable,
            presentation: FailurePresentation {
                message: "unreachable".to_owned(),
                remediation: None,
            },
        }
    }

    fn revision(seed: &[u8]) -> ConfigRevision {
        ConfigRevision::from_digest(&ConfigDigest::from_bytes(seed))
    }

    #[test]
    fn test_publication_requires_the_snapshot_revision() {
        let graph_id = GraphId::new();
        let proved = revision(b"r1");
        let newer = revision(b"r2");
        let terminal = GraphIdentityResolution::Terminal {
            revision: proved.clone(),
            database_url: "postgres://proved/tribal".to_owned(),
            graph_id: Some(graph_id),
        };

        assert_eq!(
            published_for(&GraphIdentityResolution::Pending, &proved),
            None,
            "pending publishes nothing"
        );
        assert_eq!(published_for(&terminal, &proved), Some(graph_id));
        assert_eq!(
            published_for(&terminal, &newer),
            None,
            "a proof from another revision must fail closed"
        );
    }

    #[tokio::test]
    async fn test_reprove_settles_terminal_for_an_unreachable_target() {
        let temp = tempfile::tempdir().expect("temporary config root");
        let config_path = temp.path().join("tribal.yaml");
        let config = TribalConfig::minimum_valid("postgres://user:pass@127.0.0.1:1/tribal");
        std::fs::write(
            &config_path,
            serde_yaml::to_string(&config).expect("config serialises"),
        )
        .expect("config writes");
        let (config, _worker_runtime) =
            worker::spawn(ConfigAuthority::new(config_path)).expect("config worker starts");
        let shutdown = CancellationToken::new();
        let resolver = GraphIdentityResolver::new(config.clone(), shutdown.clone());
        let operation = OperationContext::new(shutdown);
        let snapshot = config
            .for_operation(&operation)
            .resolved_snapshot()
            .await
            .expect("configuration resolves");

        resolver.reprove().await;

        assert!(
            resolver.is_terminal(),
            "an unreachable target still settles"
        );
        assert_eq!(resolver.published(&snapshot.revision), None);
    }

    #[test]
    fn test_only_a_ready_target_proves_an_identity() {
        let graph_id = GraphId::new();
        assert_eq!(
            identity_of(&DatabaseTargetState::Ready { graph_id }),
            Some(graph_id)
        );
        for state in [
            DatabaseTargetState::Unavailable { failure: failure() },
            DatabaseTargetState::Uninitialised,
            DatabaseTargetState::Behind { pending_count: 3 },
            DatabaseTargetState::Ahead,
        ] {
            assert_eq!(identity_of(&state), None);
        }
    }
}
