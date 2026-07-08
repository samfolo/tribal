//! A generic debounced filesystem watcher.
//!
//! The watcher registers a recursive `notify` watch on a root, debounces the
//! raw events, and runs a caller's async handler on each settled batch of
//! changed paths until the cancellation token fires. Every concrete watcher —
//! prompt hot-reload, config-change notification — is this loop plus a handler.

use std::{
    future::Future,
    path::{Path, PathBuf},
    time::Duration,
};

#[cfg(test)]
use notify_debouncer_mini::{
    Config as DebounceConfig, new_debouncer_opt,
    notify::{Config as NotifyConfig, PollWatcher},
};
use notify_debouncer_mini::{DebounceEventHandler, DebounceEventResult, Debouncer};
#[cfg(not(test))]
use notify_debouncer_mini::{new_debouncer, notify::RecommendedWatcher};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::constants::{LOG_WATCHER_CHANNEL_CLOSED, LOG_WATCHER_ERROR};
use crate::error::AppError;

/// How long changes settle before a batch is delivered — long enough to coalesce
/// an editor's write burst, short enough to feel live.
const DEBOUNCE_DURATION: Duration = Duration::from_millis(500);

/// Bounded channel capacity for debounced batches. A full channel drops stale
/// batches; the next one carries the current file state.
const EVENT_CHANNEL_CAPACITY: usize = 16;

#[cfg(not(test))]
fn new_file_debouncer<Handler>(
    handler: Handler,
) -> Result<Debouncer<RecommendedWatcher>, notify::Error>
where
    Handler: DebounceEventHandler,
{
    new_debouncer(DEBOUNCE_DURATION, handler)
}

#[cfg(test)]
fn new_file_debouncer<Handler>(handler: Handler) -> Result<Debouncer<PollWatcher>, notify::Error>
where
    Handler: DebounceEventHandler,
{
    // Polling gives tests the same debouncer contract without native watcher availability.
    let notify_config = NotifyConfig::default()
        .with_poll_interval(Duration::from_millis(50))
        .with_compare_contents(true);
    let debounce_config = DebounceConfig::default()
        .with_timeout(DEBOUNCE_DURATION)
        .with_notify_config(notify_config);
    new_debouncer_opt::<Handler, PollWatcher>(debounce_config, handler)
}

/// Watches `root` at `mode` and runs `on_change` for each debounced batch of
/// changed paths, until the token fires.
///
/// The watcher is created synchronously (fail-fast on init failure); the
/// returned future is spawned on the main runtime.
///
/// # Errors
///
/// Returns [`AppError::FileWatcher`] when the `notify` watcher cannot be created
/// or the root cannot be registered.
pub(crate) fn watch_path<Handler, Fut>(
    root: &Path,
    what: &'static str,
    mode: notify::RecursiveMode,
    mut on_change: Handler,
    cancellation_token: CancellationToken,
) -> Result<impl Future<Output = ()> + use<Handler, Fut>, AppError>
where
    Handler: FnMut(Vec<PathBuf>) -> Fut,
    Fut: Future<Output = ()>,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel::<DebounceEventResult>(EVENT_CHANNEL_CAPACITY);

    let mut debouncer = new_file_debouncer(move |event: DebounceEventResult| {
        let _ = tx.try_send(event);
    })
    .map_err(|source| AppError::FileWatcher {
        context: format!("create {what} debouncer"),
        source,
    })?;

    debouncer
        .watcher()
        .watch(root, mode)
        .map_err(|source| AppError::FileWatcher {
            context: format!("watch {}", root.display()),
            source,
        })?;

    Ok(async move {
        loop {
            tokio::select! {
                result = rx.recv() => match result {
                    Some(Ok(events)) => {
                        let paths = events.into_iter().map(|event| event.path).collect();
                        on_change(paths).await;
                    }
                    Some(Err(error)) => warn!(%error, LOG_WATCHER_ERROR),
                    None => {
                        warn!(LOG_WATCHER_CHANNEL_CLOSED);
                        break;
                    }
                },
                () = cancellation_token.cancelled() => break,
            }
        }
        drop(debouncer);
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn test_a_file_change_reaches_the_handler() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cancellation_token = CancellationToken::new();
        let (seen_tx, mut seen_rx) = mpsc::channel::<Vec<PathBuf>>(4);

        let watcher = watch_path(
            dir.path(),
            "test",
            notify::RecursiveMode::NonRecursive,
            move |paths| {
                let seen_tx = seen_tx.clone();
                async move {
                    let _ = seen_tx.send(paths).await;
                }
            },
            cancellation_token.clone(),
        )
        .expect("the watcher initialises");
        let handle = tokio::spawn(watcher);

        // Give the watch time to register before the write.
        tokio::time::sleep(Duration::from_millis(100)).await;
        std::fs::write(dir.path().join("touched.txt"), b"hello").expect("write");

        let batch = tokio::time::timeout(Duration::from_secs(5), seen_rx.recv())
            .await
            .expect("a batch arrives before the timeout")
            .expect("the handler ran");
        assert!(
            batch.iter().any(|path| path.ends_with("touched.txt")),
            "the changed path reaches the handler: {batch:?}",
        );

        cancellation_token.cancel();
        let _ = handle.await;
    }
}
