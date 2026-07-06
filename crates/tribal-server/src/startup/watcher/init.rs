//! The concrete watchers, each a handler over the generic [`watch_path`] loop.

use std::{
    ffi::OsStr,
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};

use sqlx::PgPool;
use tokio::sync::{RwLock, broadcast};
use tokio_util::sync::CancellationToken;
use tribal_mcp::ActivePromptVersions;
use tribal_wire::control::{ControlEvent, WriteEffect};

use super::{reload::reload_single_prompt, self_write::SelfWriteSentinel, watch::watch_path};
use crate::{error::AppError, startup::PromptTemplateLocation};

/// Initialises the prompt hot-reload watcher on `prompts_dir`, publishing
/// [`ControlEvent::PromptReloaded`] for every version that actually changes.
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
    events: broadcast::Sender<ControlEvent>,
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
            let events = events.clone();
            async move {
                for path in paths {
                    reload_and_publish(
                        &prompts_dir,
                        &path,
                        &pool,
                        &active_prompt_versions,
                        &events,
                    )
                    .await;
                }
            }
        },
        cancellation_token,
    )
}

/// Reloads the prompt at `path` and, when a version actually changes, publishes
/// [`ControlEvent::PromptReloaded`]. A path that is not a prompt template is
/// ignored.
async fn reload_and_publish(
    prompts_dir: &Path,
    path: &Path,
    pool: &PgPool,
    active_prompt_versions: &Arc<RwLock<ActivePromptVersions>>,
    events: &broadcast::Sender<ControlEvent>,
) {
    let Some(location) = PromptTemplateLocation::from_path(prompts_dir, path) else {
        return;
    };
    if let Some(reloaded) = reload_single_prompt(location, path, pool, active_prompt_versions).await
    {
        let _ = events.send(ControlEvent::PromptReloaded {
            stage: reloaded.stage,
            role: reloaded.role,
            version_id: reloaded.version_id,
        });
    }
}

/// Initialises the config-file watcher on the directory holding `config_path`,
/// publishing [`ControlEvent::ConfigChanged`] when the file is edited out from
/// under the running server.
///
/// It watches the containing directory non-recursively, not the file node: the
/// server's own writer lands a change by atomic rename, which fires no watch on
/// the replaced node, and the file is identified within its directory by name —
/// robust to the path normalisation a watch backend may apply. The event
/// carries no keys — an external edit is opaque, so a client re-reads the whole
/// surface — and [`WriteEffect::NeedsRestart`]: external edits stay
/// restart-scoped by decision — live adoption serves only the write path's own
/// validated writes, never a file edited out-of-band.
///
/// The server's own `config.set` rename lands here too; `sentinel` lets that
/// self-write be recognised by its content and suppressed, so the authoritative
/// per-key event `config.set` already published is not shadowed by a second,
/// keyless, restart-labelled one.
///
/// # Errors
///
/// Returns [`AppError::FileWatcher`] when the underlying watcher cannot be
/// created or the directory cannot be registered.
pub(crate) fn init_config_watcher(
    config_path: &Path,
    events: broadcast::Sender<ControlEvent>,
    sentinel: SelfWriteSentinel,
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
            let events = events.clone();
            let sentinel = sentinel.clone();
            async move {
                if is_external_config_edit(
                    &paths,
                    config_file_name.as_deref(),
                    &watched_file,
                    &sentinel,
                )
                .await
                {
                    let _ = events.send(ControlEvent::ConfigChanged {
                        keys: Vec::new(),
                        effect: WriteEffect::NeedsRestart,
                    });
                }
            }
        },
        cancellation_token,
    )
}

/// Whether a settled watch batch is a config edit to announce: it touches the
/// watched file by `name`, and its content is not the writer's own recorded
/// persist — that self-write `config.set` already announced authoritatively.
async fn is_external_config_edit(
    paths: &[PathBuf],
    name: Option<&OsStr>,
    watched_file: &Path,
    sentinel: &SelfWriteSentinel,
) -> bool {
    let Some(name) = name else {
        return false;
    };
    if !paths.iter().any(|path| path.file_name() == Some(name)) {
        return false;
    }
    if let Ok(current) = tokio::fs::read(watched_file).await
        && sentinel.claims(&current)
    {
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::broadcast;
    use tribal_domain::{PromptRole, PromptStage};
    use tribal_test_utils::TestDb;

    use super::*;
    use crate::startup::{ensure_prompt_files, load_prompts};

    #[tokio::test]
    async fn test_a_config_edit_reaches_a_subscriber_as_config_changed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("tribal.yaml");
        std::fs::write(&config_path, "server: {}\n").expect("seed config");

        let (events, mut subscriber) = broadcast::channel(8);
        let cancellation_token = CancellationToken::new();

        let watcher = init_config_watcher(
            &config_path,
            events,
            SelfWriteSentinel::default(),
            cancellation_token.clone(),
        )
        .expect("the config watcher initialises");
        let handle = tokio::spawn(watcher);

        // Let the watch register before the edit.
        tokio::time::sleep(Duration::from_millis(100)).await;
        std::fs::write(&config_path, "server: {}\nlogging: {}\n").expect("edit config");

        let event = tokio::time::timeout(Duration::from_secs(5), subscriber.recv())
            .await
            .expect("an event arrives before the timeout")
            .expect("the watcher published");
        assert!(
            matches!(event, ControlEvent::ConfigChanged { .. }),
            "an external config edit publishes config.changed, got {event:?}",
        );

        cancellation_token.cancel();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_a_recorded_self_write_is_not_an_external_edit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("tribal.yaml");
        let name = config_path.file_name().map(OsStr::to_owned);
        let bytes = b"server: {}\nlogging: {}\n".to_vec();
        std::fs::write(&config_path, &bytes).expect("write config");

        let sentinel = SelfWriteSentinel::default();
        sentinel.record(bytes);

        // The writer's own persist is suppressed once, then a later edit of the
        // same file publishes.
        assert!(
            !is_external_config_edit(
                std::slice::from_ref(&config_path),
                name.as_deref(),
                &config_path,
                &sentinel
            )
            .await,
            "the recorded self-write is not announced as an external edit",
        );
        std::fs::write(&config_path, "server: {}\n").expect("external edit");
        assert!(
            is_external_config_edit(
                std::slice::from_ref(&config_path),
                name.as_deref(),
                &config_path,
                &sentinel
            )
            .await,
            "a later edit with different content is announced",
        );
    }

    #[tokio::test]
    async fn test_an_unrelated_file_is_not_a_config_edit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("tribal.yaml");
        let name = config_path.file_name().map(OsStr::to_owned);
        let other = dir.path().join("other.yaml");
        assert!(
            !is_external_config_edit(
                &[other],
                name.as_deref(),
                &config_path,
                &SelfWriteSentinel::default()
            )
            .await,
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
        let (events, mut subscriber) = broadcast::channel(8);

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

        reload_and_publish(prompts_dir.path(), &path, &pool, &active, &events).await;

        match subscriber.try_recv().expect("a reload publishes an event") {
            ControlEvent::PromptReloaded {
                stage: reloaded_stage,
                role: reloaded_role,
                ..
            } => {
                assert_eq!(reloaded_stage, stage.as_str());
                assert_eq!(reloaded_role, role.as_str());
            }
            other => panic!("expected prompt.reloaded, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_an_idempotent_reload_publishes_nothing() {
        let (active, prompts_dir, pool, _db) = prompt_reload_harness().await;
        let (events, mut subscriber) = broadcast::channel(8);

        let target = PromptTemplateLocation::one_shot(PromptStage::Extraction, PromptRole::System);
        let path = target.resolve(prompts_dir.path());

        // No edit: the on-disk content already matches the loaded version.
        reload_and_publish(prompts_dir.path(), &path, &pool, &active, &events).await;

        assert!(
            matches!(
                subscriber.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ),
            "an unchanged prompt publishes nothing",
        );
    }
}
