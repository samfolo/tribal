//! The concrete watchers, each a handler over the generic [`watch_path`] loop.

use std::{
    ffi::OsStr,
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};

use sqlx::PgPool;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tribal_mcp::ActivePromptVersions;

use super::{reload::reload_single_prompt, watch::watch_path};
use crate::{error::AppError, startup::PromptTemplateLocation};

/// Initialises the prompt hot-reload watcher on `prompts_dir`.
///
/// The watcher is created synchronously (fail-fast on init failure); the
/// returned future is spawned on the main runtime.
///
/// # Errors
///
/// Returns [`AppError::FileWatcher`] when the underlying watcher cannot be
/// created or `prompts_dir` cannot be registered.
pub(crate) fn init_prompt_watcher(
    prompts_dir: PathBuf,
    pool: PgPool,
    active_prompt_versions: Arc<RwLock<ActivePromptVersions>>,
    cancellation_token: CancellationToken,
) -> Result<impl Future<Output = ()>, AppError> {
    let root = prompts_dir.clone();
    watch_path(
        &root,
        "prompts",
        notify::RecursiveMode::Recursive,
        move |paths| {
            let prompts_dir = prompts_dir.clone();
            let pool = pool.clone();
            let active_prompt_versions = active_prompt_versions.clone();
            async move {
                for path in paths {
                    reload_and_log(&prompts_dir, &path, &pool, &active_prompt_versions).await;
                }
            }
        },
        cancellation_token,
    )
}

/// Reloads the prompt at `path` and records a changed version in managed logs.
async fn reload_and_log(
    prompts_dir: &Path,
    path: &Path,
    pool: &PgPool,
    active_prompt_versions: &Arc<RwLock<ActivePromptVersions>>,
) -> bool {
    let Some(location) = PromptTemplateLocation::from_path(prompts_dir, path) else {
        return false;
    };
    if let Some(reloaded) = reload_single_prompt(location, path, pool, active_prompt_versions).await
    {
        tracing::info!(
            stage = %reloaded.stage,
            role = %reloaded.role,
            version_id = %reloaded.version_id,
            "prompt template reloaded",
        );
        return true;
    }
    false
}

/// Initialises the config-file watcher on the directory holding `config_path`.
///
/// It watches the containing directory non-recursively, not the file node: the
/// server's own writer lands a change by atomic rename, which fires no watch on
/// the replaced node, and the file is identified within its directory by name —
/// robust to the path normalisation a watch backend may apply. External edits
/// are opaque and restart-scoped, so they are recorded in the managed log
/// stream and adopted only by a later runtime restart.
///
/// # Errors
///
/// Returns [`AppError::FileWatcher`] when the underlying watcher cannot be
/// created or the directory cannot be registered.
pub(crate) fn init_config_watcher(
    config_path: &Path,
    cancellation_token: CancellationToken,
) -> Result<impl Future<Output = ()> + use<>, AppError> {
    let directory = config_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or_else(|| PathBuf::from("."), Path::to_owned);
    let config_file_name = config_path.file_name().map(OsStr::to_owned);
    let watched_file = config_path.to_owned();

    watch_path(
        &directory,
        "config",
        notify::RecursiveMode::NonRecursive,
        move |paths| {
            let config_file_name = config_file_name.clone();
            let watched_file = watched_file.clone();
            async move {
                if is_config_edit(&paths, config_file_name.as_deref()) {
                    tracing::info!(
                        path = %watched_file.display(),
                        "configuration changed on disk; runtime restart required",
                    );
                }
            }
        },
        cancellation_token,
    )
}

/// Whether a settled watch batch is a config edit to announce: it touches the
/// watched file by `name`, and its content is not the writer's own recorded
/// persist — that self-write `config.set` already announced authoritatively.
fn is_config_edit(paths: &[PathBuf], name: Option<&OsStr>) -> bool {
    let Some(name) = name else {
        return false;
    };
    paths.iter().any(|path| path.file_name() == Some(name))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::{PromptRole, PromptStage};
    use tribal_test_utils::TestDb;

    use super::*;
    use crate::startup::{ensure_prompt_files, load_prompts};

    #[test]
    fn test_an_unrelated_file_is_not_a_config_edit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("tribal.yaml");
        let name = config_path.file_name().map(OsStr::to_owned);
        let other = dir.path().join("other.yaml");
        assert!(
            !is_config_edit(&[other], name.as_deref()),
            "a batch that does not touch the watched file is not a config edit",
        );
    }

    async fn prompt_reload_harness() -> (
        Arc<RwLock<ActivePromptVersions>>,
        tempfile::TempDir,
        PgPool,
        TestDb,
    ) {
        let db = TestDb::new().await;
        let pool = db.create_pool().await.expect("create per-test pool");
        let prompts_dir = tempfile::tempdir().expect("prompts tempdir");
        ensure_prompt_files(prompts_dir.path())
            .await
            .expect("write default prompts");
        let active = Arc::new(RwLock::new(
            load_prompts(&pool, prompts_dir.path())
                .await
                .expect("load prompts"),
        ));
        (active, prompts_dir, pool, db)
    }

    #[tokio::test]
    async fn test_a_reload_publishes_prompt_reloaded() {
        let (active, prompts_dir, pool, _db) = prompt_reload_harness().await;
        let stage = PromptStage::Extraction;
        let role = PromptRole::System;
        let target = PromptTemplateLocation::one_shot(stage, role);
        let path = target.resolve(prompts_dir.path());
        let original = tokio::fs::read_to_string(&path)
            .await
            .expect("read original");
        tokio::fs::write(&path, format!("{original}\n{{# publish test #}}"))
            .await
            .expect("edit prompt");

        assert!(reload_and_log(prompts_dir.path(), &path, &pool, &active).await);
    }

    #[tokio::test]
    async fn test_an_idempotent_reload_publishes_nothing() {
        let (active, prompts_dir, pool, _db) = prompt_reload_harness().await;
        let target = PromptTemplateLocation::one_shot(PromptStage::Extraction, PromptRole::System);
        let path = target.resolve(prompts_dir.path());

        // No edit: the on-disk content already matches the loaded version.
        assert!(!reload_and_log(prompts_dir.path(), &path, &pool, &active).await);
    }
}
