//! Project registration and bounded inventory.

use std::{path::Path, str::FromStr as _};

use tribal_db::{DbError, NewProject, PgProjectRepository, ProjectPageKey, ProjectRepository};
use tribal_domain::{GitRemote, Project, ProjectId};
use tribal_wire::management::{
    AdministrationFailure, InventoryItemRef, ManagementError, ManagementResponseError, ProjectList,
    ProjectListRequest, ProjectPage, ProjectRegisterInput, ProjectRegisterOutcome,
    ProjectRegisterRequest, ProjectRegisterResult, ProjectRegistrationSource, ProjectSummary,
};

use super::{
    database::{DatabaseAccess, DatabaseAccessError},
    pagination::{
        INVENTORY_RESULT_BUDGET, InventoryCursor, InventoryCursorError, InventoryMethod,
        InventoryPosition,
    },
};

const PROJECT_SCHEMA_VERSION: u32 = 1;
const DEFAULT_BRANCH: &str = "main";

#[derive(Debug, thiserror::Error)]
pub(super) enum ProjectAdministrationError {
    #[error(transparent)]
    Session(#[from] DatabaseAccessError),
    #[error("project source is invalid")]
    Source,
    #[error("project repository failed: {source}")]
    Repository {
        #[source]
        source: DbError,
    },
    #[error(transparent)]
    Cursor(#[from] InventoryCursorError),
    #[error("project cursor contains an invalid position")]
    CursorPosition,
    #[error("project inventory item exceeds the response budget")]
    ItemTooLarge(ProjectId),
}

#[derive(Clone)]
pub(super) struct ProjectAdministration {
    database: DatabaseAccess,
}

impl ProjectAdministration {
    pub(super) fn new(database: DatabaseAccess) -> Self {
        Self { database }
    }

    pub(super) fn preflight(
        project: &ProjectRegisterInput,
    ) -> Result<(), ProjectAdministrationError> {
        resolve_source(project.source.clone()).map(|_| ())
    }

    pub(super) async fn register(
        &self,
        request: ProjectRegisterRequest,
    ) -> Result<ProjectRegisterResult, ProjectAdministrationError> {
        let session = self
            .database
            .mutation_session(&request.expected_revision)
            .await?;
        let remote = resolve_source(request.project.source)?;
        let name = request
            .project
            .name
            .unwrap_or_else(|| remote.path().to_owned());
        let default_branch = request
            .project
            .default_branch
            .unwrap_or_else(|| DEFAULT_BRANCH.to_owned());
        let candidate = NewProject::builder()
            .git_remote(remote.clone())
            .name(name)
            .default_branch(default_branch)
            .schema_version(PROJECT_SCHEMA_VERSION)
            .settings(serde_json::json!({}))
            .build();
        let mut connection = session.pool.acquire().await.map_err(database_connection)?;
        let outcome = match PgProjectRepository
            .insert(&mut connection, &candidate)
            .await
        {
            Ok(project) => ProjectRegisterOutcome::Registered {
                project: summary(&project),
            },
            Err(DbError::UniqueViolation { .. }) => {
                let project = PgProjectRepository
                    .find_by_git_remote(&mut connection, &remote)
                    .await
                    .map_err(repository)?
                    .ok_or_else(|| ProjectAdministrationError::Repository {
                        source: DbError::NotFound {
                            entity: "project",
                            id: remote.to_string(),
                        },
                    })?;
                ProjectRegisterOutcome::AlreadyRegistered {
                    project: summary(&project),
                }
            }
            Err(source) => return Err(repository(source)),
        };
        Ok(session.revisioned(outcome))
    }

    pub(super) async fn list(
        &self,
        request: ProjectListRequest,
    ) -> Result<ProjectList, ProjectAdministrationError> {
        let session = self.database.read_session().await?;
        let cursor = request
            .page
            .after
            .as_ref()
            .map(|cursor| {
                InventoryCursor::decode(cursor, InventoryMethod::ProjectList, &session.revision)
            })
            .transpose()?;
        let mut connection = session.pool.acquire().await.map_err(database_connection)?;
        let (high_water, after) = if let Some(cursor) = cursor {
            (
                project_key(&cursor.high_water)?,
                Some(project_key(&cursor.after)?),
            )
        } else {
            let Some(high_water) = PgProjectRepository
                .page_high_water(&mut connection)
                .await
                .map_err(repository)?
            else {
                return Ok(session.revisioned(ProjectPage {
                    items: Vec::new(),
                    next: None,
                }));
            };
            (high_water, None)
        };
        let fetch_limit = request.page.size.get().saturating_add(1);
        let projects = PgProjectRepository
            .list_page(&mut connection, high_water, after, fetch_limit)
            .await
            .map_err(repository)?;
        let requested = usize::from(request.page.size.get());
        let mut items = Vec::with_capacity(projects.len().min(requested));
        let mut next = None;
        for (index, project) in projects.iter().enumerate() {
            if index == requested {
                next = items
                    .last()
                    .map(|item| project_cursor(&session.revision, high_water, item).encode())
                    .transpose()?;
                break;
            }
            let candidate = summary(project);
            let has_more = index + 1 < projects.len();
            let candidate_next = if has_more {
                Some(project_cursor(&session.revision, high_water, &candidate).encode()?)
            } else {
                None
            };
            items.push(candidate);
            let candidate_page = session.revisioned(ProjectPage {
                items: items.clone(),
                next: candidate_next.clone(),
            });
            if serde_json::to_vec(&candidate_page)
                .map_err(|source| InventoryCursorError::Encoding { source })?
                .len()
                > INVENTORY_RESULT_BUDGET
            {
                let oversized = items.pop().ok_or(InventoryCursorError::InternalInvariant)?;
                if items.is_empty() {
                    return Err(ProjectAdministrationError::ItemTooLarge(oversized.id));
                }
                next = items
                    .last()
                    .map(|item| project_cursor(&session.revision, high_water, item).encode())
                    .transpose()?;
                break;
            }
            next = candidate_next;
        }
        Ok(session.revisioned(ProjectPage { items, next }))
    }
}

pub(super) fn public_error(error: ProjectAdministrationError) -> ManagementResponseError {
    match error {
        ProjectAdministrationError::Session(DatabaseAccessError::RevisionConflict {
            expected,
            actual,
        })
        | ProjectAdministrationError::Cursor(InventoryCursorError::Stale { expected, actual }) => {
            ManagementResponseError {
                message: "configuration changed before project administration".to_owned(),
                error: ManagementError::ConfigConflict { expected, actual },
            }
        }
        ProjectAdministrationError::Source => administration_error(
            "project source is invalid",
            AdministrationFailure::ProjectSourceInvalid,
        ),
        ProjectAdministrationError::ItemTooLarge(id) => administration_error(
            "project inventory item exceeds the response budget",
            AdministrationFailure::InventoryItemTooLarge {
                item: InventoryItemRef::Project(id),
            },
        ),
        ProjectAdministrationError::Cursor(
            InventoryCursorError::Malformed | InventoryCursorError::WrongMethod,
        )
        | ProjectAdministrationError::CursorPosition => ManagementResponseError {
            message: "project inventory cursor is invalid".to_owned(),
            error: ManagementError::ConfigurationInvalid { fields: Vec::new() },
        },
        ProjectAdministrationError::Cursor(
            InventoryCursorError::Encoding { .. } | InventoryCursorError::InternalInvariant,
        ) => ManagementResponseError {
            message: "project inventory could not be encoded".to_owned(),
            error: ManagementError::InternalInvariant,
        },
        ProjectAdministrationError::Session(DatabaseAccessError::Configuration(error)) => {
            super::super::configuration::management_error(error)
        }
        ProjectAdministrationError::Session(DatabaseAccessError::Connection { .. })
        | ProjectAdministrationError::Repository { .. } => administration_error(
            "project database is unavailable",
            AdministrationFailure::DatabaseUnavailable,
        ),
    }
}

fn resolve_source(
    source: ProjectRegistrationSource,
) -> Result<GitRemote, ProjectAdministrationError> {
    match source {
        ProjectRegistrationSource::GitRemote { remote } => Ok(remote),
        ProjectRegistrationSource::WorkingTree { directory } => {
            crate::git::detect_git_remote_from(Path::new(directory.as_str()))
                .map_err(|_| ProjectAdministrationError::Source)
        }
    }
}

fn summary(project: &Project) -> ProjectSummary {
    ProjectSummary {
        id: project.id(),
        git_remote: project.git_remote().clone(),
        name: project.name().to_owned(),
        default_branch: project.default_branch().to_owned(),
        created_at: project.created_at(),
        updated_at: project.updated_at(),
    }
}

fn project_key(position: &InventoryPosition) -> Result<ProjectPageKey, ProjectAdministrationError> {
    Ok(ProjectPageKey {
        created_at: position.created_at,
        id: ProjectId::from_str(&position.id)
            .map_err(|_| ProjectAdministrationError::CursorPosition)?,
    })
}

fn project_cursor(
    revision: &tribal_wire::management::ConfigRevision,
    high_water: ProjectPageKey,
    after: &ProjectSummary,
) -> InventoryCursor {
    InventoryCursor::new(
        InventoryMethod::ProjectList,
        revision.clone(),
        InventoryPosition {
            created_at: high_water.created_at,
            id: high_water.id.to_string(),
        },
        InventoryPosition {
            created_at: after.created_at,
            id: after.id.to_string(),
        },
    )
}

fn database_connection(source: sqlx::Error) -> ProjectAdministrationError {
    ProjectAdministrationError::Repository {
        source: DbError::QueryFailed {
            context: "acquiring project administration connection".to_owned(),
            source,
        },
    }
}

fn repository(source: DbError) -> ProjectAdministrationError {
    ProjectAdministrationError::Repository { source }
}

fn administration_error(message: &str, failure: AdministrationFailure) -> ManagementResponseError {
    ManagementResponseError {
        message: message.to_owned(),
        error: ManagementError::Administration { failure },
    }
}
