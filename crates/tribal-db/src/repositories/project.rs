//! Project repository: trait definition and Postgres implementation.
//!
//! Projects are the top-level organisational unit, identified by their
//! `git_remote` URL.  The repository provides insert, lookup, and listing
//! operations.  `find_by_git_remote` returns `Option<Project>` because
//! absence is a valid outcome in project resolution flows.

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_domain::{Project, ProjectId};
use typed_builder::TypedBuilder;

use crate::DbError;

const SCHEMA_VERSION_EXCEEDS_I32: &str = "schema_version exceeds i32::MAX";
const SCHEMA_VERSION_OVERFLOW: &str = "negative schema_version in database — data corruption";

/// Input for creating a new project.
///
/// Contains only caller-provided fields.  Server-generated values
/// (`id`, `created_at`, `updated_at`) are produced by Postgres via
/// `DEFAULT` clauses and returned via `RETURNING *`.
#[derive(Debug, TypedBuilder)]
pub struct NewProject {
    /// The git remote URL (stable project identity).
    pub git_remote: String,
    /// Human-friendly project name.
    pub name: String,
    /// Default branch (e.g. `"main"`).
    pub default_branch: String,
    /// Optional project type hint.
    #[builder(default)]
    pub project_type: Option<String>,
    /// Settings schema version.
    pub schema_version: u32,
    /// Project-specific configuration (JSONB).
    pub settings: serde_json::Value,
}

/// Data access operations for projects.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.  Callers provide a connection
/// from whichever pool is appropriate (production) or pass a
/// `TestTransaction` (tests).
#[async_trait]
pub trait ProjectRepository {
    /// Inserts a new project and returns the fully populated domain type.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] if a project with the same
    /// `git_remote` already exists.  Returns [`DbError::QueryFailed`] on
    /// other database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new_project: &NewProject,
    ) -> Result<Project, DbError>;

    /// Finds a project by its ID.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] if no project with the given ID
    /// exists (a broken reference).  Returns [`DbError::QueryFailed`]
    /// on database errors.
    async fn find_by_id(&self, conn: &mut PgConnection, id: ProjectId) -> Result<Project, DbError>;

    /// Finds a project by its git remote URL.
    ///
    /// Returns `None` if no matching project exists.  Absence is a valid
    /// outcome (used in project resolution flows), not an error.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_git_remote(
        &self,
        conn: &mut PgConnection,
        git_remote: &str,
    ) -> Result<Option<Project>, DbError>;

    /// Lists all projects, ordered by `created_at` ascending.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn list(&self, conn: &mut PgConnection) -> Result<Vec<Project>, DbError>;
}

/// Postgres implementation of [`ProjectRepository`].
///
/// A zero-sized type with no internal state.  The caller provides
/// the database connection explicitly on each method call.
pub struct PgProjectRepository;

#[async_trait]
impl ProjectRepository for PgProjectRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new_project: &NewProject,
    ) -> Result<Project, DbError> {
        let schema_version =
            i32::try_from(new_project.schema_version).expect(SCHEMA_VERSION_EXCEEDS_I32);

        let row = sqlx::query!(
            r#"
            INSERT INTO projects (git_remote, name, default_branch, project_type, schema_version, settings)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
            "#,
            new_project.git_remote,
            new_project.name,
            new_project.default_branch,
            new_project.project_type,
            schema_version,
            new_project.settings,
        )
        .fetch_one(&mut *conn)
        .await;

        match row {
            Ok(r) => Ok(Project::builder()
                .id(ProjectId::from(r.id))
                .git_remote(r.git_remote)
                .name(r.name)
                .default_branch(r.default_branch)
                .project_type(r.project_type)
                .schema_version(u32::try_from(r.schema_version).expect(SCHEMA_VERSION_OVERFLOW))
                .settings(r.settings)
                .created_at(r.created_at)
                .updated_at(r.updated_at)
                .build()),
            Err(e) => {
                if let Some(uv) = super::common::constraint::try_into_unique_violation(&e) {
                    Err(uv)
                } else {
                    Err(DbError::QueryFailed {
                        context: "inserting project".to_owned(),
                        source: e,
                    })
                }
            }
        }
    }

    async fn find_by_id(&self, conn: &mut PgConnection, id: ProjectId) -> Result<Project, DbError> {
        let r = sqlx::query!(r#"SELECT * FROM projects WHERE id = $1"#, id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding project by id {id}"),
                source: e,
            })?
            .ok_or_else(|| DbError::NotFound {
                entity: "project",
                id: id.to_string(),
            })?;

        Ok(Project::builder()
            .id(ProjectId::from(r.id))
            .git_remote(r.git_remote)
            .name(r.name)
            .default_branch(r.default_branch)
            .project_type(r.project_type)
            .schema_version(u32::try_from(r.schema_version).expect(SCHEMA_VERSION_OVERFLOW))
            .settings(r.settings)
            .created_at(r.created_at)
            .updated_at(r.updated_at)
            .build())
    }

    async fn find_by_git_remote(
        &self,
        conn: &mut PgConnection,
        git_remote: &str,
    ) -> Result<Option<Project>, DbError> {
        let r = sqlx::query!(
            r#"SELECT * FROM projects WHERE git_remote = $1"#,
            git_remote,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("finding project by git_remote '{git_remote}'"),
            source: e,
        })?;

        Ok(r.map(|r| {
            Project::builder()
                .id(ProjectId::from(r.id))
                .git_remote(r.git_remote)
                .name(r.name)
                .default_branch(r.default_branch)
                .project_type(r.project_type)
                .schema_version(u32::try_from(r.schema_version).expect(SCHEMA_VERSION_OVERFLOW))
                .settings(r.settings)
                .created_at(r.created_at)
                .updated_at(r.updated_at)
                .build()
        }))
    }

    async fn list(&self, conn: &mut PgConnection) -> Result<Vec<Project>, DbError> {
        let rows = sqlx::query!(r#"SELECT * FROM projects ORDER BY created_at ASC"#)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "listing projects".to_owned(),
                source: e,
            })?;

        Ok(rows
            .into_iter()
            .map(|r| {
                Project::builder()
                    .id(ProjectId::from(r.id))
                    .git_remote(r.git_remote)
                    .name(r.name)
                    .default_branch(r.default_branch)
                    .project_type(r.project_type)
                    .schema_version(u32::try_from(r.schema_version).expect(SCHEMA_VERSION_OVERFLOW))
                    .settings(r.settings)
                    .created_at(r.created_at)
                    .updated_at(r.updated_at)
                    .build()
            })
            .collect())
    }
}
