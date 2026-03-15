//! Project context resolution with a 4-level cascade.
//!
//! Resolution order: CLI flag → `TRIBAL_PROJECT_ID` env var → git remote
//! heuristic → `None`.

use sqlx::PgPool;
use tribal_db::{PgProjectRepository, ProjectRepository};
use tribal_domain::ProjectId;
use tribal_mcp::ResolvedProject;

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const ENV_PROJECT_ID: &str = "TRIBAL_PROJECT_ID";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Resolves the project context for this server instance.
///
/// Cascade:
/// 1. `cli_project` — explicit `--project` flag (format: `proj_{uuid}`)
/// 2. `TRIBAL_PROJECT_ID` — environment variable (same format)
/// 3. Git remote heuristic — reads `.git/config` in the working directory
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
    if let Ok(raw) = std::env::var(ENV_PROJECT_ID) {
        if !raw.is_empty() {
            let project = resolve_by_id(pool, &raw).await?;
            return Ok(Some(project));
        }
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
    let mut conn = pool.acquire().await.map_err(|e| AppError::Database {
        source: tribal_db::DbError::QueryFailed {
            context: "acquire connection for project lookup".into(),
            source: e,
        },
    })?;

    let project = repo
        .find_by_id(&mut conn, project_id)
        .await
        .map_err(|source| AppError::ProjectResolution {
            context: format!("project {raw} not found in database: {source}"),
        })?;

    tracing::info!(
        project_id = %project.id,
        project_name = %project.name,
        "resolved project from explicit ID",
    );

    Ok(ResolvedProject {
        id: project.id,
        name: project.name,
        git_remote: project.git_remote,
    })
}

/// Reads the git origin remote URL from `.git/config` and looks up the
/// project by normalised remote.
async fn resolve_by_git_remote(pool: &PgPool) -> Result<Option<ResolvedProject>, AppError> {
    let git_config_path = std::path::Path::new(".git/config");
    if !git_config_path.exists() {
        return Ok(None);
    }

    let content = tokio::fs::read_to_string(git_config_path)
        .await
        .map_err(|source| AppError::PromptIo {
            context: "read .git/config for project resolution".into(),
            source,
        })?;

    let Some(remote_url) = parse_origin_url(&content) else {
        return Ok(None);
    };

    let normalised = normalise_git_remote(&remote_url);

    let repo = PgProjectRepository;
    let mut conn = pool.acquire().await.map_err(|e| AppError::Database {
        source: tribal_db::DbError::QueryFailed {
            context: "acquire connection for git remote lookup".into(),
            source: e,
        },
    })?;

    let project = repo.find_by_git_remote(&mut conn, &normalised).await.map_err(
        |source| AppError::Database { source },
    )?;

    match project {
        Some(p) => {
            tracing::info!(
                project_id = %p.id,
                project_name = %p.name,
                git_remote = %normalised,
                "resolved project from git remote",
            );
            Ok(Some(ResolvedProject {
                id: p.id,
                name: p.name,
                git_remote: p.git_remote,
            }))
        }
        None => {
            tracing::debug!(git_remote = %normalised, "no project registered for this remote");
            Ok(None)
        }
    }
}

/// Extracts the `url` value from the `[remote "origin"]` section of a
/// git config file.
fn parse_origin_url(content: &str) -> Option<String> {
    let mut in_origin = false;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_origin = trimmed == "[remote \"origin\"]";
            continue;
        }
        if in_origin {
            if let Some(url) = trimmed.strip_prefix("url = ") {
                return Some(url.trim().to_owned());
            }
        }
    }
    None
}

/// Normalises a git remote URL for consistent matching.
///
/// Strips trailing `.git`, protocol prefix, and `user@` prefix to produce
/// a `host:owner/repo` form.
fn normalise_git_remote(url: &str) -> String {
    let mut s = url.to_owned();

    // Strip trailing .git
    if let Some(stripped) = s.strip_suffix(".git") {
        s = stripped.to_owned();
    }

    // Strip protocol prefix
    if let Some(rest) = s.strip_prefix("https://") {
        s = rest.to_owned();
    } else if let Some(rest) = s.strip_prefix("http://") {
        s = rest.to_owned();
    } else if let Some(rest) = s.strip_prefix("ssh://") {
        s = rest.to_owned();
    }

    // Strip user@ prefix (e.g. git@)
    if let Some(at_pos) = s.find('@') {
        if at_pos < s.find(':').unwrap_or(s.len()) {
            s = s[at_pos + 1..].to_owned();
        }
    }

    // Replace first : with / for SSH-style URLs (github.com:owner/repo)
    if let Some(colon_pos) = s.find(':') {
        if !s[..colon_pos].contains('/') {
            s.replace_range(colon_pos..=colon_pos, "/");
        }
    }

    s
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_origin_url_ssh() {
        let config = r#"
[remote "origin"]
    url = git@github.com:user/repo.git
    fetch = +refs/heads/*:refs/remotes/origin/*
"#;
        assert_eq!(
            parse_origin_url(config),
            Some("git@github.com:user/repo.git".into()),
        );
    }

    #[test]
    fn test_parse_origin_url_https() {
        let config = r#"
[remote "origin"]
    url = https://github.com/user/repo.git
"#;
        assert_eq!(
            parse_origin_url(config),
            Some("https://github.com/user/repo.git".into()),
        );
    }

    #[test]
    fn test_parse_origin_url_missing() {
        let config = r#"
[remote "upstream"]
    url = git@github.com:other/repo.git
"#;
        assert_eq!(parse_origin_url(config), None);
    }

    #[test]
    fn test_normalise_ssh_remote() {
        assert_eq!(
            normalise_git_remote("git@github.com:user/repo.git"),
            "github.com/user/repo",
        );
    }

    #[test]
    fn test_normalise_https_remote() {
        assert_eq!(
            normalise_git_remote("https://github.com/user/repo.git"),
            "github.com/user/repo",
        );
    }

    #[test]
    fn test_normalise_no_suffix() {
        assert_eq!(
            normalise_git_remote("git@github.com:user/repo"),
            "github.com/user/repo",
        );
    }

    #[test]
    fn test_normalise_plain_https() {
        assert_eq!(
            normalise_git_remote("https://gitlab.com/org/project"),
            "gitlab.com/org/project",
        );
    }
}
