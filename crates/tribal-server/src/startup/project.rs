//! Project context resolution with a 4-level cascade.
//!
//! Resolution order: CLI flag → `TRIBAL_PROJECT_ID` env var → git remote
//! heuristic → `None`.

use std::path::Path;

use gix::remote::Direction;
use sqlx::PgPool;
use tribal_config::ENV_PROJECT_ID;
use tribal_db::{DbError, PgProjectRepository, ProjectRepository};
use tribal_domain::{GitRemote, ProjectId};
use tribal_mcp::ResolvedProject;

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Resolves the project context for this server instance.
///
/// Cascade:
/// 1. `cli_project` — explicit `--project` flag (format: `proj_{uuid}`)
/// 2. `TRIBAL_PROJECT_ID` — environment variable (same format)
/// 3. Git remote heuristic — discovers the repository and reads the
///    origin remote URL
/// 4. `None` — no project context; MCP `set_context` must be used
pub(crate) async fn resolve_project(
    pool: &PgPool,
    cli_project: Option<String>,
) -> Result<Option<ResolvedProject>, AppError> {
    // 1. CLI flag
    if let Some(raw) = cli_project {
        let project = resolve_by_id(pool, &raw).await?;
        return Ok(Some(project));
    }

    // 2. Environment variable
    if let Ok(raw) = std::env::var(ENV_PROJECT_ID)
        && !raw.is_empty()
    {
        let project = resolve_by_id(pool, &raw).await?;
        return Ok(Some(project));
    }

    // 3. Git remote heuristic
    if let Some(project) = resolve_by_git_remote(pool).await? {
        return Ok(Some(project));
    }

    // 4. No project context
    tracing::info!("no project resolved; MCP set_context required");
    Ok(None)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parses a prefixed project ID and looks it up in the database.
async fn resolve_by_id(pool: &PgPool, raw: &str) -> Result<ResolvedProject, AppError> {
    let project_id: ProjectId = raw.parse().map_err(|_| AppError::ProjectResolution {
        context: format!("invalid project ID format: {raw}"),
    })?;

    let repo = PgProjectRepository;
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| AppError::pool_acquire("project lookup", e))?;

    let project = repo
        .find_by_id(&mut conn, project_id)
        .await
        .map_err(|source| match &source {
            DbError::NotFound { .. } => AppError::ProjectResolution {
                context: format!("project {raw} not found in database"),
            },
            _ => AppError::ProjectResolution {
                context: format!("failed to look up project {raw}: {source}"),
            },
        })?;

    tracing::info!(
        project_id = %project.id(),
        project_name = %project.name(),
        "resolved project from explicit ID",
    );

    Ok(ResolvedProject::builder()
        .id(project.id())
        .name(project.name())
        .git_remote(project.git_remote().clone())
        .build())
}

/// Discovers the git repository from the current working directory,
/// reads the origin remote URL, and looks up the project in the database.
async fn resolve_by_git_remote(pool: &PgPool) -> Result<Option<ResolvedProject>, AppError> {
    let Some(remote_url) = discover_origin_url(Path::new(".")) else {
        return Ok(None);
    };

    let repo = PgProjectRepository;
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| AppError::pool_acquire("git remote lookup", e))?;

    let project = repo
        .find_by_git_remote(&mut conn, &remote_url)
        .await
        .map_err(|source| AppError::Database { source })?;

    if let Some(p) = project {
        tracing::info!(
            project_id = %p.id(),
            project_name = %p.name(),
            git_remote = %remote_url,
            "resolved project from git remote",
        );
        Ok(Some(
            ResolvedProject::builder()
                .id(p.id())
                .name(p.name())
                .git_remote(p.git_remote().clone())
                .build(),
        ))
    } else {
        tracing::debug!(git_remote = %remote_url, "no project registered for this remote");
        Ok(None)
    }
}

/// Uses `gix` to discover the git repository starting from `start_dir`
/// and extract the origin remote URL, returning it as a [`GitRemote`]
/// in canonical form. Returns `None` if discovery fails or origin is
/// not configured.
fn discover_origin_url(start_dir: &Path) -> Option<GitRemote> {
    let repo = match gix::discover(start_dir) {
        Ok(repo) => repo,
        Err(e) => {
            tracing::debug!(%e, "git repository discovery failed");
            return None;
        }
    };

    let remote = match repo.find_default_remote(Direction::Fetch) {
        Some(Ok(remote)) => remote,
        Some(Err(e)) => {
            tracing::debug!(%e, "failed to read default fetch remote");
            return None;
        }
        None => {
            tracing::debug!("no default fetch remote configured");
            return None;
        }
    };

    let url = remote.url(Direction::Fetch)?;
    let host = url.host()?;
    let path = url.path.to_string();
    let port = url.port;

    Some(GitRemote::from_parts(host, &path, port))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discover_origin_url_returns_some_in_repo() {
        // This test runs inside the tribal repo, so discovery should succeed.
        let url = discover_origin_url(Path::new("."));
        assert!(url.is_some(), "expected to discover origin in tribal repo");
    }

    #[test]
    fn test_discover_origin_url_returns_none_outside_repo() {
        // A fresh tempdir is guaranteed not to be inside a git repository.
        let tmp = tempfile::tempdir().expect("should create tempdir");

        let result = discover_origin_url(tmp.path());
        assert!(result.is_none(), "expected None outside a git repository");
    }
}
