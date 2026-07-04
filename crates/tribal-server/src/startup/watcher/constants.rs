//! Log and validation message constants for the filesystem watchers.

pub(super) const LOG_PROMPT_RELOADED: &str = "hot-reloaded prompt version";
pub(super) const LOG_PROMPT_VALIDATION_FAILED: &str =
    "prompt validation failed; keeping current version";
pub(super) const LOG_PROMPT_READ_FAILED: &str =
    "failed to read prompt file; keeping current version";
pub(super) const LOG_PROMPT_UPSERT_FAILED: &str =
    "failed to upsert prompt version; keeping current version";
pub(super) const LOG_WATCHER_ERROR: &str = "file watcher reported an error";
pub(super) const LOG_WATCHER_CHANNEL_CLOSED: &str =
    "file watcher channel closed unexpectedly; hot-reload disabled";
