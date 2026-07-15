//! Revision-bound MCP integration rendering.

use std::path::Path;

use tribal_db::{DbError, PgProjectRepository, ProjectRepository as _};
use tribal_domain::{BearerToken, Project, ProjectId, TransportKind};
use tribal_wire::management::{
    AdministrationFailure, ConfiguredMcpTarget, McpConfigEntry, McpConfigRequest, McpConfigResult,
    McpTarget, McpTargetSelection, NetworkIntegrationAuth, ProjectSelector,
    PublicMcpConfigDocument, PublicMcpServerEntry, SensitiveMcpConfigDocument, StdioProjectContext,
};

use super::{
    credential::{CredentialCoordinator, CredentialCoordinatorError},
    database::{DatabaseAccess, DatabaseAccessError, DatabaseSession},
};
use crate::{
    commands::serve::ServeProjectMode,
    management::{configuration::ConfigAuthorityError, worker::ConfigWorkerClient},
    output::{resolved_advertised_url, snippet_key},
};

#[derive(Debug, thiserror::Error)]
pub(super) enum IntegrationAdministrationError {
    #[error(transparent)]
    Session(#[from] DatabaseAccessError),
    #[error("configuration path is unavailable: {0}")]
    Configuration(#[from] ConfigAuthorityError),
    #[error("integration target is incompatible with its exposure policy")]
    IncompatibleTarget,
    #[error("project source is invalid")]
    ProjectSource,
    #[error("project is not registered")]
    ProjectNotFound { id: Option<ProjectId> },
    #[error("project repository failed: {source}")]
    ProjectRepository {
        #[source]
        source: DbError,
    },
    #[error("persisted credential export failed: {source}")]
    Credential {
        #[source]
        source: CredentialCoordinatorError,
    },
    #[error("canonical network audience is unavailable")]
    Audience,
}

#[derive(Clone)]
pub(super) struct IntegrationAdministration {
    config: ConfigWorkerClient,
    database: DatabaseAccess,
    credentials: CredentialCoordinator,
}

pub(super) struct PreparedMcpConfig {
    session: DatabaseSession,
    entry: PreparedMcpEntry,
}

enum PreparedMcpEntry {
    Public(PublicMcpConfigDocument),
    PersistedBearer {
        transport: TransportKind,
        url: String,
    },
}

impl PreparedMcpConfig {
    pub(super) fn revision(&self) -> &tribal_wire::management::ConfigRevision {
        &self.session.revision
    }

    pub(super) fn requires_bearer(&self) -> bool {
        matches!(self.entry, PreparedMcpEntry::PersistedBearer { .. })
    }

    pub(super) fn render(
        self,
        bearer: Option<&BearerToken>,
    ) -> Result<McpConfigResult, IntegrationAdministrationError> {
        let entry = match self.entry {
            PreparedMcpEntry::Public(document) => McpConfigEntry::Public { document },
            PreparedMcpEntry::PersistedBearer { transport, url } => {
                let bearer = bearer.ok_or(IntegrationAdministrationError::IncompatibleTarget)?;
                McpConfigEntry::PersistedBearer {
                    document: sensitive_network_document(transport, &url, bearer),
                }
            }
        };
        Ok(self.session.revisioned(entry))
    }
}

impl IntegrationAdministration {
    pub(super) fn new(
        config: ConfigWorkerClient,
        database: DatabaseAccess,
        credentials: CredentialCoordinator,
    ) -> Self {
        Self {
            config,
            database,
            credentials,
        }
    }

    pub(super) async fn preflight_target(
        &self,
        expected_revision: &tribal_wire::management::ConfigRevision,
        selection: &McpTargetSelection,
    ) -> Result<McpTarget, IntegrationAdministrationError> {
        let snapshot = self.config.resolved_snapshot().await?;
        if &snapshot.revision != expected_revision {
            return Err(IntegrationAdministrationError::Session(
                DatabaseAccessError::RevisionConflict {
                    expected: expected_revision.clone(),
                    actual: snapshot.revision,
                },
            ));
        }
        let target = resolve_target(snapshot.config.server.transport, selection.clone())?;
        if let McpTarget::Stdio {
            context:
                StdioProjectContext::Project {
                    selector: ProjectSelector::WorkingTree { directory },
                },
        } = &target
        {
            crate::git::detect_git_remote_from(Path::new(directory.as_str()))
                .map_err(|_| IntegrationAdministrationError::ProjectSource)?;
        }
        Ok(target)
    }

    pub(super) async fn mcp_config(
        &self,
        request: McpConfigRequest,
    ) -> Result<McpConfigResult, IntegrationAdministrationError> {
        let prepared = self.prepare(request).await?;
        let bearer = if prepared.requires_bearer() {
            let audience = crate::startup::resolve_oauth_runtime(&prepared.session.config)
                .map_err(|_| IntegrationAdministrationError::Audience)?
                .canonical_resource;
            Some(
                self.credentials
                    .export_persisted(clone_session(&prepared.session), audience)
                    .await
                    .map_err(|source| IntegrationAdministrationError::Credential { source })?,
            )
        } else {
            None
        };
        prepared.render(bearer.as_ref())
    }

    pub(super) async fn prepare(
        &self,
        request: McpConfigRequest,
    ) -> Result<PreparedMcpConfig, IntegrationAdministrationError> {
        let session = self
            .database
            .mutation_session(&request.expected_revision)
            .await?;
        let target = resolve_target(session.config.server.transport, request.target)?;
        let entry = match target {
            McpTarget::Stdio { context } => {
                let config_path = self.config.path().await?;
                let (mode, server_name) = resolve_stdio(&session, context).await?;
                PreparedMcpEntry::Public(PublicMcpConfigDocument {
                    server_name,
                    entry: PublicMcpServerEntry::Stdio {
                        command: "tribal".to_owned(),
                        args: stdio_args(Path::new(&config_path.path), mode),
                    },
                })
            }
            McpTarget::Http { auth } => prepare_network(&session, TransportKind::Http, auth)?,
            McpTarget::Sse { auth } => prepare_network(&session, TransportKind::Sse, auth)?,
        };
        Ok(PreparedMcpConfig { session, entry })
    }
}

fn prepare_network(
    session: &DatabaseSession,
    transport: TransportKind,
    auth: NetworkIntegrationAuth,
) -> Result<PreparedMcpEntry, IntegrationAdministrationError> {
    let url = resolved_advertised_url(&session.config);
    let entry = match auth {
        NetworkIntegrationAuth::OAuth => PreparedMcpEntry::Public(PublicMcpConfigDocument {
            server_name: "tribal".to_owned(),
            entry: match transport {
                TransportKind::Http => PublicMcpServerEntry::Http { url },
                TransportKind::Sse => PublicMcpServerEntry::Sse { url },
                TransportKind::Stdio => {
                    return Err(IntegrationAdministrationError::IncompatibleTarget);
                }
            },
        }),
        NetworkIntegrationAuth::ExportPersistedBearer => {
            PreparedMcpEntry::PersistedBearer { transport, url }
        }
    };
    Ok(entry)
}

fn resolve_target(
    configured: TransportKind,
    selection: McpTargetSelection,
) -> Result<McpTarget, IntegrationAdministrationError> {
    match selection {
        McpTargetSelection::Explicit { target } => Ok(target),
        McpTargetSelection::Configured { policy } => match (configured, policy) {
            (TransportKind::Stdio, ConfiguredMcpTarget::Public { stdio_context }) => {
                Ok(McpTarget::Stdio {
                    context: stdio_context,
                })
            }
            (TransportKind::Stdio, ConfiguredMcpTarget::ExportPersistedBearer { .. }) => {
                Err(IntegrationAdministrationError::IncompatibleTarget)
            }
            (TransportKind::Http, ConfiguredMcpTarget::Public { .. }) => Ok(McpTarget::Http {
                auth: NetworkIntegrationAuth::OAuth,
            }),
            (TransportKind::Http, ConfiguredMcpTarget::ExportPersistedBearer { .. }) => {
                Ok(McpTarget::Http {
                    auth: NetworkIntegrationAuth::ExportPersistedBearer,
                })
            }
            (TransportKind::Sse, ConfiguredMcpTarget::Public { .. }) => Ok(McpTarget::Sse {
                auth: NetworkIntegrationAuth::OAuth,
            }),
            (TransportKind::Sse, ConfiguredMcpTarget::ExportPersistedBearer { .. }) => {
                Ok(McpTarget::Sse {
                    auth: NetworkIntegrationAuth::ExportPersistedBearer,
                })
            }
        },
    }
}

async fn resolve_stdio(
    session: &DatabaseSession,
    context: StdioProjectContext,
) -> Result<(ServeProjectMode, String), IntegrationAdministrationError> {
    match context {
        StdioProjectContext::Unscoped => Ok((ServeProjectMode::Unscoped, "tribal".to_owned())),
        StdioProjectContext::Project { selector } => {
            let project = resolve_project(session, selector).await?;
            let name = snippet_key(project.git_remote());
            Ok((ServeProjectMode::Project(project.id()), name))
        }
    }
}

async fn resolve_project(
    session: &DatabaseSession,
    selector: ProjectSelector,
) -> Result<Project, IntegrationAdministrationError> {
    let mut connection = session.pool.acquire().await.map_err(|source| {
        IntegrationAdministrationError::ProjectRepository {
            source: DbError::QueryFailed {
                context: "acquiring integration project connection".to_owned(),
                source,
            },
        }
    })?;
    match selector {
        ProjectSelector::Id { id } => PgProjectRepository
            .find_by_id(&mut connection, id)
            .await
            .map_err(|source| match source {
                DbError::NotFound { .. } => {
                    IntegrationAdministrationError::ProjectNotFound { id: Some(id) }
                }
                source => IntegrationAdministrationError::ProjectRepository { source },
            }),
        ProjectSelector::WorkingTree { directory } => {
            let remote = crate::git::detect_git_remote_from(Path::new(directory.as_str()))
                .map_err(|_| IntegrationAdministrationError::ProjectSource)?;
            PgProjectRepository
                .find_by_git_remote(&mut connection, &remote)
                .await
                .map_err(|source| IntegrationAdministrationError::ProjectRepository { source })?
                .ok_or(IntegrationAdministrationError::ProjectNotFound { id: None })
        }
    }
}

fn stdio_args(config_path: &Path, mode: ServeProjectMode) -> Vec<String> {
    let mut args = vec![
        "--config".to_owned(),
        config_path.display().to_string(),
        "serve".to_owned(),
    ];
    match mode {
        ServeProjectMode::Auto => {}
        ServeProjectMode::Unscoped => args.push("--unscoped".to_owned()),
        ServeProjectMode::Project(id) => {
            args.push("--project".to_owned());
            args.push(id.to_string());
        }
    }
    args
}

fn sensitive_network_document(
    transport: TransportKind,
    url: &str,
    bearer: &BearerToken,
) -> SensitiveMcpConfigDocument {
    SensitiveMcpConfigDocument::new(serde_json::json!({
        "mcpServers": {
            "tribal": {
                "type": transport.to_string(),
                "url": url,
                "headers": { "Authorization": format!("Bearer {}", bearer.as_str()) },
            }
        }
    }))
}

fn clone_session(session: &DatabaseSession) -> DatabaseSession {
    DatabaseSession {
        revision: session.revision.clone(),
        config: session.config.clone(),
        pool: session.pool.clone(),
    }
}

pub(super) fn public_failure(error: &IntegrationAdministrationError) -> AdministrationFailure {
    match error {
        IntegrationAdministrationError::IncompatibleTarget => {
            AdministrationFailure::IntegrationTargetIncompatible
        }
        IntegrationAdministrationError::ProjectSource => {
            AdministrationFailure::ProjectSourceInvalid
        }
        IntegrationAdministrationError::ProjectNotFound { id: Some(id) } => {
            AdministrationFailure::ProjectNotFound { id: *id }
        }
        IntegrationAdministrationError::ProjectNotFound { id: None } => {
            AdministrationFailure::IntegrationUnavailable
        }
        IntegrationAdministrationError::Credential {
            source: CredentialCoordinatorError::Unavailable,
        } => AdministrationFailure::PersistedCredentialUnavailable,
        IntegrationAdministrationError::Credential {
            source: CredentialCoordinatorError::Store(_),
        } => AdministrationFailure::PersistedCredentialRecoveryFailed,
        IntegrationAdministrationError::Session(DatabaseAccessError::Connection { .. })
        | IntegrationAdministrationError::ProjectRepository { .. }
        | IntegrationAdministrationError::Credential {
            source:
                CredentialCoordinatorError::Connection { .. }
                | CredentialCoordinatorError::Database { .. },
        } => AdministrationFailure::DatabaseUnavailable,
        IntegrationAdministrationError::Session(_)
        | IntegrationAdministrationError::Configuration(_)
        | IntegrationAdministrationError::Audience
        | IntegrationAdministrationError::Credential {
            source: CredentialCoordinatorError::GeneratedToken,
        } => AdministrationFailure::IntegrationUnavailable,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use tribal_db::{NewProject, PgProjectRepository, ProjectRepository as _};
    use tribal_domain::{GitRemote, LOCAL_PRINCIPAL_KEY, full_access_scopes};
    use tribal_wire::management::{ConfigRevision, McpConfigEntry};

    use super::*;
    use crate::management::{
        authority::ConfigAuthorityNamespace,
        configuration::ConfigAuthority,
        worker::{self, ConfigWorkerRuntime},
    };

    struct Harness {
        database: tribal_test_utils::TestDb,
        temp: tempfile::TempDir,
        worker_runtime: ConfigWorkerRuntime,
        credential_runtime: super::super::credential::CredentialCoordinatorRuntime,
        access: DatabaseAccess,
        credentials: CredentialCoordinator,
        administration: IntegrationAdministration,
        revision: ConfigRevision,
    }

    impl Harness {
        async fn new(transport: TransportKind) -> Self {
            let database = tribal_test_utils::TestDb::new().await;
            let temp = tempfile::tempdir().expect("temporary integration root");
            let config_path = temp.path().join("tribal.yaml");
            let mut config = tribal_config::TribalConfig::minimum_valid(database.database_url());
            config.server.transport = transport;
            std::fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();
            let (worker, worker_runtime) =
                worker::spawn(ConfigAuthority::new(config_path)).expect("config worker starts");
            let revision = worker.resolved_snapshot().await.unwrap().revision;
            let namespace = ConfigAuthorityNamespace::from_test("0123456789abcdef01234567");
            let (credentials, credential_runtime) =
                CredentialCoordinator::spawn_with_root(namespace, temp.path());
            let access = DatabaseAccess::new(worker.clone());
            let administration =
                IntegrationAdministration::new(worker, access.clone(), credentials.clone());
            Self {
                database,
                temp,
                worker_runtime,
                credential_runtime,
                access,
                credentials,
                administration,
                revision,
            }
        }

        async fn shutdown(self) {
            let Self {
                database,
                temp,
                worker_runtime,
                credential_runtime,
                access,
                credentials,
                administration,
                revision: _,
            } = self;
            drop(administration);
            drop(credentials);
            drop(access);
            credential_runtime.join().await.unwrap();
            worker_runtime.join().unwrap();
            drop(temp);
            drop(database);
        }

        fn request(&self, target: McpTargetSelection) -> McpConfigRequest {
            McpConfigRequest {
                expected_revision: self.revision.clone(),
                target,
            }
        }
    }

    #[test]
    fn test_configured_and_explicit_targets_resolve_without_auth_ambiguity() {
        let context = StdioProjectContext::Unscoped;
        assert!(matches!(
            resolve_target(
                TransportKind::Stdio,
                McpTargetSelection::Configured {
                    policy: ConfiguredMcpTarget::Public {
                        stdio_context: context.clone(),
                    },
                },
            ),
            Ok(McpTarget::Stdio { .. })
        ));
        assert!(matches!(
            resolve_target(
                TransportKind::Http,
                McpTargetSelection::Configured {
                    policy: ConfiguredMcpTarget::Public {
                        stdio_context: context.clone(),
                    },
                },
            ),
            Ok(McpTarget::Http {
                auth: NetworkIntegrationAuth::OAuth
            })
        ));
        assert!(matches!(
            resolve_target(
                TransportKind::Sse,
                McpTargetSelection::Configured {
                    policy: ConfiguredMcpTarget::ExportPersistedBearer {
                        stdio_context: context.clone(),
                    },
                },
            ),
            Ok(McpTarget::Sse {
                auth: NetworkIntegrationAuth::ExportPersistedBearer
            })
        ));
        assert!(matches!(
            resolve_target(
                TransportKind::Http,
                McpTargetSelection::Explicit {
                    target: McpTarget::Stdio { context },
                },
            ),
            Ok(McpTarget::Stdio { .. })
        ));
    }

    #[test]
    fn test_configured_bearer_export_refuses_stdio() {
        let error = resolve_target(
            TransportKind::Stdio,
            McpTargetSelection::Configured {
                policy: ConfiguredMcpTarget::ExportPersistedBearer {
                    stdio_context: StdioProjectContext::Unscoped,
                },
            },
        )
        .expect_err("stdio cannot carry bearer auth");

        assert!(matches!(
            public_failure(&error),
            AdministrationFailure::IntegrationTargetIncompatible
        ));
    }

    #[tokio::test]
    async fn test_configured_unscoped_stdio_is_ambient_project_proof() {
        let harness = Harness::new(TransportKind::Stdio).await;
        let result = harness
            .administration
            .mcp_config(harness.request(McpTargetSelection::Configured {
                policy: ConfiguredMcpTarget::Public {
                    stdio_context: StdioProjectContext::Unscoped,
                },
            }))
            .await
            .expect("configured stdio renders");

        let McpConfigEntry::Public { document } = result.value else {
            panic!("stdio is public")
        };
        assert_eq!(document.server_name, "tribal");
        let PublicMcpServerEntry::Stdio { command, args } = document.entry else {
            panic!("stdio entry expected")
        };
        assert_eq!(command, "tribal");
        assert!(args.ends_with(&["serve".to_owned(), "--unscoped".to_owned()]));
        assert!(!args.iter().any(|argument| argument == "--project"));
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn test_explicit_project_stdio_uses_stable_id_and_remote_server_name() {
        let harness = Harness::new(TransportKind::Http).await;
        let remote = GitRemote::from_parts("github.com", "acme/widgets", None);
        let project = PgProjectRepository
            .insert(
                &mut harness.database.pool().acquire().await.unwrap(),
                &NewProject::builder()
                    .git_remote(remote)
                    .name("widgets".to_owned())
                    .default_branch("main".to_owned())
                    .schema_version(1)
                    .settings(serde_json::json!({}))
                    .build(),
            )
            .await
            .unwrap();
        let result = harness
            .administration
            .mcp_config(harness.request(McpTargetSelection::Explicit {
                target: McpTarget::Stdio {
                    context: StdioProjectContext::Project {
                        selector: ProjectSelector::Id { id: project.id() },
                    },
                },
            }))
            .await
            .expect("project stdio renders");

        let McpConfigEntry::Public { document } = result.value else {
            panic!("stdio is public")
        };
        assert_eq!(document.server_name, "tribal@acme/widgets");
        let PublicMcpServerEntry::Stdio { args, .. } = document.entry else {
            panic!("stdio entry expected")
        };
        assert!(args.ends_with(&["--project".to_owned(), project.id().to_string()]));
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn test_configured_http_and_explicit_sse_render_public_oauth_entries() {
        let http = Harness::new(TransportKind::Http).await;
        let result = http
            .administration
            .mcp_config(http.request(McpTargetSelection::Configured {
                policy: ConfiguredMcpTarget::Public {
                    stdio_context: StdioProjectContext::Unscoped,
                },
            }))
            .await
            .expect("configured http renders");
        let McpConfigEntry::Public { document } = result.value else {
            panic!("OAuth entry is public")
        };
        assert!(matches!(document.entry, PublicMcpServerEntry::Http { .. }));
        http.shutdown().await;

        let sse = Harness::new(TransportKind::Http).await;
        let result = sse
            .administration
            .mcp_config(sse.request(McpTargetSelection::Explicit {
                target: McpTarget::Sse {
                    auth: NetworkIntegrationAuth::OAuth,
                },
            }))
            .await
            .expect("explicit sse renders");
        let McpConfigEntry::Public { document } = result.value else {
            panic!("OAuth entry is public")
        };
        assert!(matches!(document.entry, PublicMcpServerEntry::Sse { .. }));
        sse.shutdown().await;
    }

    #[tokio::test]
    async fn test_persisted_bearer_export_is_scoped_and_redacted() {
        let harness = Harness::new(TransportKind::Http).await;
        let session = harness
            .access
            .mutation_session(&harness.revision)
            .await
            .unwrap();
        let audience = crate::startup::resolve_oauth_runtime(&session.config)
            .unwrap()
            .canonical_resource;
        let issued = harness
            .credentials
            .issue_persisted(
                session,
                LOCAL_PRINCIPAL_KEY.to_owned(),
                full_access_scopes(),
                audience,
                Utc::now() + Duration::hours(1),
            )
            .await
            .unwrap();
        let secret = issued.raw;
        let result = harness
            .administration
            .mcp_config(harness.request(McpTargetSelection::Explicit {
                target: McpTarget::Http {
                    auth: NetworkIntegrationAuth::ExportPersistedBearer,
                },
            }))
            .await
            .expect("persisted bearer renders");

        let McpConfigEntry::PersistedBearer { document } = result.value else {
            panic!("persisted bearer entry expected")
        };
        assert!(!format!("{document:?}").contains(&secret));
        assert!(!document.to_string().contains(&secret));
        assert!(document.with_document(|value| value.to_string().contains(&secret)));
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn test_coordinator_exit_is_a_typed_persisted_bearer_refusal() {
        let harness = Harness::new(TransportKind::Sse).await;
        let Harness {
            database,
            temp,
            worker_runtime,
            credential_runtime,
            access,
            credentials,
            administration,
            revision,
        } = harness;
        credential_runtime.abort().await;
        let error = administration
            .mcp_config(McpConfigRequest {
                expected_revision: revision,
                target: McpTargetSelection::Explicit {
                    target: McpTarget::Sse {
                        auth: NetworkIntegrationAuth::ExportPersistedBearer,
                    },
                },
            })
            .await
            .expect_err("dead coordinator refuses bearer export");

        assert!(matches!(
            public_failure(&error),
            AdministrationFailure::PersistedCredentialUnavailable
        ));
        drop(administration);
        drop(credentials);
        drop(access);
        worker_runtime.join().unwrap();
        drop(temp);
        drop(database);
    }
}
