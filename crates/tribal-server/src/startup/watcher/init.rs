//! Watcher initialisation and event loop.

use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};

use notify_debouncer_mini::{DebounceEventResult, new_debouncer};
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::warn;
use tribal_mcp::ActivePromptVersions;

use super::{
    constants::{LOG_WATCHER_CHANNEL_CLOSED, LOG_WATCHER_ERROR},
    reload::reload_single_prompt,
};
use crate::{error::AppError, startup::PromptTemplateLocation};

const DEBOUNCE_DURATION: Duration = Duration::from_millis(500);

/// Bounded channel capacity for debounced events. Stale events are
/// dropped if the channel is full — the next batch will pick up the
/// current file state.
const EVENT_CHANNEL_CAPACITY: usize = 16;

/// Initialises the file watcher on `prompts_dir` and returns a future
/// that runs the event loop until the cancellation token fires.
///
/// The watcher itself is created synchronously (fail-fast on init
/// failure). The returned future is spawned on the main runtime.
///
/// # Errors
///
/// Returns [`AppError::PromptWatcher`] if the underlying `notify`
/// watcher cannot be created or the watch path cannot be registered.
pub(crate) fn init_prompt_watcher(
    prompts_dir: PathBuf,
    pool: PgPool,
    active_prompt_versions: Arc<RwLock<ActivePromptVersions>>,
    cancellation_token: tokio_util::sync::CancellationToken,
) -> Result<impl Future<Output = ()>, AppError> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<DebounceEventResult>(EVENT_CHANNEL_CAPACITY);

    let mut debouncer = new_debouncer(DEBOUNCE_DURATION, move |event: DebounceEventResult| {
        let _ = tx.try_send(event);
    })
    .map_err(|source| AppError::PromptWatcher {
        context: "create debouncer".into(),
        source,
    })?;

    debouncer
        .watcher()
        .watch(&prompts_dir, notify::RecursiveMode::Recursive)
        .map_err(|source| AppError::PromptWatcher {
            context: format!("watch {}", prompts_dir.display()),
            source,
        })?;

    Ok(async move {
        loop {
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Some(Ok(events)) => {
                            for event in events {
                                let Some(location) = PromptTemplateLocation::from_path(&prompts_dir, &event.path) else {
                                    continue;
                                };
                                reload_single_prompt(
                                    location,
                                    &event.path,
                                    &pool,
                                    &active_prompt_versions,
                                )
                                .await;
                            }
                        }
                        Some(Err(error)) => {
                            warn!(%error, LOG_WATCHER_ERROR);
                        }
                        None => {
                            warn!(LOG_WATCHER_CHANNEL_CLOSED);
                            break;
                        }
                    }
                }
                () = cancellation_token.cancelled() => break,
            }
        }
        drop(debouncer);
    })
}
