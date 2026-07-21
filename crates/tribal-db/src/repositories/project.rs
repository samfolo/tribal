//! Project repository: trait definition and Postgres implementation.
//!
//! Projects are the top-level organisational unit. The repository separates
//! caller-created Git projects from the database-owned System project.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use tribal_domain::{GitRemote, Project, ProjectId, ProjectOrigin};
use typed_builder::TypedBuilder;

use crate::DbError;

#[derive(sqlx::FromRow)]
struct ProjectRow {
    id: uuid::Uuid,
    origin: serde_json::Value,
    name: String,
    project_type: Option<String>,
    schema_version: i32,
    settings: serde_json::Value,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

/// Input for creating a new project.
///
/// Contains only caller-provided fields.  Server-generated values
/// (`id`, `created_at`, `updated_at`) are produced by Postgres via
/// `DEFAULT` clauses and returned via `RETURNING *`.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewGitProject {
    /// Canonical Git remote identity.
    pub remote: GitRemote,
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

/// Database-observed result of ensuring the System project.
#[derive(Debug, Clone, PartialEq)]
pub enum EnsureSystemOutcome {
    /// This call inserted the System project.
    Inserted(Project),
    /// The System project already existed.
    Existing(Project),
}

/// Stable keyset position for project inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectPageKey {
    pub created_at: DateTime<Utc>,
    pub id: ProjectId,
}

/// Data access operations for projects.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.  Callers provide a connection
/// from whichever pool is appropriate (production) or pass a
/// `TestTransaction` (tests).
#[async_trait]
pub trait ProjectRepository: Send + Sync {
    /// Inserts a new Git project and returns the populated domain type.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] if a project with the same
    /// `git_remote` already exists.  Returns [`DbError::QueryFailed`] on
    /// other database errors.
    async fn insert_git(
        &self,
        conn: &mut PgConnection,
        new_project: &NewGitProject,
    ) -> Result<Project, DbError>;

    /// Ensures the graph-owned System project exists.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] when the write or follow-up lookup
    /// fails, and [`DbError::NotFound`] when the skipped insert's follow-up
    /// lookup cannot see the competing row (a concurrent delete, or a
    /// caller-held snapshot that predates it).
    async fn ensure_system(&self, conn: &mut PgConnection) -> Result<EnsureSystemOutcome, DbError>;

    /// Returns the graph-owned System project.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] when the invariant is absent and
    /// [`DbError::QueryFailed`] when the lookup fails.
    async fn find_system(&self, conn: &mut PgConnection) -> Result<Project, DbError>;

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
        git_remote: &GitRemote,
    ) -> Result<Option<Project>, DbError>;

    /// Lists all projects, ordered by `created_at` ascending.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn list(&self, conn: &mut PgConnection) -> Result<Vec<Project>, DbError>;

    /// Captures the newest row visible at the start of an inventory walk.
    async fn page_high_water(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Option<ProjectPageKey>, DbError>;

    /// Lists one descending keyset window bounded by a captured high water.
    async fn list_page(
        &self,
        conn: &mut PgConnection,
        high_water: ProjectPageKey,
        after: Option<ProjectPageKey>,
        limit: u16,
    ) -> Result<Vec<Project>, DbError>;
}

/// Postgres implementation of [`ProjectRepository`].
///
/// A zero-sized type with no internal state.  The caller provides
/// the database connection explicitly on each method call.
pub struct PgProjectRepository;

#[async_trait]
impl ProjectRepository for PgProjectRepository {
    async fn insert_git(
        &self,
        conn: &mut PgConnection,
        new_project: &NewGitProject,
    ) -> Result<Project, DbError> {
        let schema_version = i32::try_from(new_project.schema_version)
            .map_err(|source| decode_error("encoding project schema version", source))?;

        let row = sqlx::query_as!(
            ProjectRow,
            r#"
            INSERT INTO projects (origin, name, project_type, schema_version, settings)
            VALUES (
                jsonb_build_object(
                    'kind', 'git',
                    'remote', $1::text,
                    'default_branch', $2::text
                ),
                $3, $4, $5, $6
            )
            RETURNING *
            "#,
            new_project.remote.as_str(),
            new_project.default_branch,
            new_project.name,
            new_project.project_type,
            schema_version,
            new_project.settings,
        )
        .fetch_one(&mut *conn)
        .await;

        match row {
            Ok(row) => map_project(row),
            Err(e) => {
                if let Some(uv) = super::common::constraint::try_into_unique_violation(&e) {
                    Err(uv)
                } else {
                    Err(DbError::QueryFailed {
                        context: "inserting Git project".to_owned(),
                        source: e,
                    })
                }
            }
        }
    }

    async fn ensure_system(&self, conn: &mut PgConnection) -> Result<EnsureSystemOutcome, DbError> {
        let inserted = sqlx::query_as!(
            ProjectRow,
            r#"
            INSERT INTO projects (origin, name, project_type, schema_version, settings)
            VALUES ('{"kind":"system"}'::jsonb, 'General', NULL, 1, '{}'::jsonb)
            ON CONFLICT DO NOTHING
            RETURNING *
            "#,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(|source| DbError::QueryFailed {
            context: "ensuring System project".to_owned(),
            source,
        })?;

        if let Some(row) = inserted {
            return map_project(row).map(EnsureSystemOutcome::Inserted);
        }

        self.find_system(conn)
            .await
            .map(EnsureSystemOutcome::Existing)
    }

    async fn find_system(&self, conn: &mut PgConnection) -> Result<Project, DbError> {
        let row = sqlx::query_as!(
            ProjectRow,
            r#"SELECT * FROM projects WHERE origin->>'kind' = 'system'"#,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(|source| DbError::QueryFailed {
            context: "finding System project".to_owned(),
            source,
        })?
        .ok_or_else(|| DbError::NotFound {
            entity: "system project",
            id: "system".to_owned(),
        })?;

        map_project(row)
    }

    async fn find_by_id(&self, conn: &mut PgConnection, id: ProjectId) -> Result<Project, DbError> {
        let row = sqlx::query_as!(
            ProjectRow,
            r#"SELECT * FROM projects WHERE id = $1"#,
            id.inner()
        )
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

        map_project(row)
    }

    async fn find_by_git_remote(
        &self,
        conn: &mut PgConnection,
        git_remote: &GitRemote,
    ) -> Result<Option<Project>, DbError> {
        let row = sqlx::query_as!(
            ProjectRow,
            r#"SELECT * FROM projects WHERE origin->>'kind' = 'git' AND origin->>'remote' = $1"#,
            git_remote.as_str(),
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("finding project by git_remote '{git_remote}'"),
            source: e,
        })?;

        row.map(map_project).transpose()
    }

    async fn list(&self, conn: &mut PgConnection) -> Result<Vec<Project>, DbError> {
        let rows = sqlx::query_as!(
            ProjectRow,
            r#"SELECT * FROM projects ORDER BY created_at ASC"#
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "listing projects".to_owned(),
            source: e,
        })?;

        rows.into_iter().map(map_project).collect()
    }

    async fn page_high_water(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Option<ProjectPageKey>, DbError> {
        let row = sqlx::query!(
            r#"
            SELECT created_at, id
            FROM projects
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            "#
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(|source| DbError::QueryFailed {
            context: "capturing project inventory high water".to_owned(),
            source,
        })?;
        Ok(row.map(|row| ProjectPageKey {
            created_at: row.created_at,
            id: ProjectId::from(row.id),
        }))
    }

    async fn list_page(
        &self,
        conn: &mut PgConnection,
        high_water: ProjectPageKey,
        after: Option<ProjectPageKey>,
        limit: u16,
    ) -> Result<Vec<Project>, DbError> {
        let rows = match after {
            Some(after) => {
                sqlx::query_as!(
                    ProjectRow,
                    r"
                    SELECT * FROM projects
                    WHERE (created_at, id) <= ($1, $2)
                      AND (created_at, id) < ($3, $4)
                    ORDER BY created_at DESC, id DESC
                    LIMIT $5
                    ",
                    high_water.created_at,
                    high_water.id.inner(),
                    after.created_at,
                    after.id.inner(),
                    i64::from(limit),
                )
                .fetch_all(&mut *conn)
                .await
            }
            None => {
                sqlx::query_as!(
                    ProjectRow,
                    r"
                    SELECT * FROM projects
                    WHERE (created_at, id) <= ($1, $2)
                    ORDER BY created_at DESC, id DESC
                    LIMIT $3
                    ",
                    high_water.created_at,
                    high_water.id.inner(),
                    i64::from(limit),
                )
                .fetch_all(&mut *conn)
                .await
            }
        }
        .map_err(|source| DbError::QueryFailed {
            context: "listing a project inventory page".to_owned(),
            source,
        })?;

        rows.into_iter().map(map_project).collect()
    }
}

fn map_project(row: ProjectRow) -> Result<Project, DbError> {
    let origin = decode_project_origin(row.origin)?;
    let schema_version = u32::try_from(row.schema_version)
        .map_err(|source| decode_error("decoding project schema version", source))?;
    Ok(Project::builder()
        .id(ProjectId::from(row.id))
        .origin(origin)
        .name(row.name)
        .project_type(row.project_type)
        .schema_version(schema_version)
        .settings(row.settings)
        .created_at(row.created_at)
        .updated_at(row.updated_at)
        .build())
}

fn decode_project_origin(origin: serde_json::Value) -> Result<ProjectOrigin, DbError> {
    serde_json::from_value(origin).map_err(|source| decode_error("decoding project origin", source))
}

fn decode_error(
    context: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> DbError {
    DbError::QueryFailed {
        context: context.to_owned(),
        source: sqlx::Error::Decode(Box::new(source)),
    }
}
