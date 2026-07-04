//! Self-write suppression bridging `config.set` and the config-file watcher.

use std::sync::{Arc, Mutex};

/// Distinguishes the config writer's own atomic rename from an out-of-band edit.
///
/// `config.set` writes the file and announces its precise effect — live,
/// needs-restart, or shadowed — on the bus itself. The watcher, which watches
/// the directory to catch external edits, also sees that rename; without this it
/// would publish a second, keyless `config.changed` labelled `NeedsRestart`,
/// contradicting a live or shadowed write. The writer records the bytes it
/// persisted; the watcher suppresses the one fire whose file content still
/// matches them and publishes every genuine edit — one whose content differs,
/// including one that lands in the window between the write and the record.
#[derive(Clone, Default)]
pub(crate) struct SelfWriteSentinel {
    last: Arc<Mutex<Option<Vec<u8>>>>,
}

impl SelfWriteSentinel {
    /// Records the bytes a `config.set` just persisted as the next fire to
    /// expect from the watcher.
    pub(crate) fn record(&self, persisted: Vec<u8>) {
        *self.lock() = Some(persisted);
    }

    /// Whether `current` is the writer's own last persist, consumed so only the
    /// single fire it explains is suppressed; a later external edit differs and
    /// is published.
    pub(crate) fn claims(&self, current: &[u8]) -> bool {
        let mut last = self.lock();
        if last.as_deref() == Some(current) {
            *last = None;
            true
        } else {
            false
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Vec<u8>>> {
        self.last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_the_recorded_self_write_is_claimed_exactly_once() {
        let sentinel = SelfWriteSentinel::default();
        sentinel.record(b"a: 1\n".to_vec());
        assert!(
            sentinel.claims(b"a: 1\n"),
            "the writer's own persist is claimed",
        );
        assert!(
            !sentinel.claims(b"a: 1\n"),
            "the claim is consumed — a later identical fire is not suppressed twice",
        );
    }

    #[test]
    fn test_a_differing_edit_is_never_claimed() {
        let sentinel = SelfWriteSentinel::default();
        sentinel.record(b"a: 1\n".to_vec());
        assert!(
            !sentinel.claims(b"a: 2\n"),
            "an external edit with different content is published, not suppressed",
        );
        // And the record it did not match still stands for its own fire.
        assert!(
            sentinel.claims(b"a: 1\n"),
            "the unmatched record is not consumed"
        );
    }

    #[test]
    fn test_an_empty_sentinel_claims_nothing() {
        assert!(!SelfWriteSentinel::default().claims(b"a: 1\n"));
    }
}
