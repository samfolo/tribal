//! The entity-keyed event hub: channel kinds matched to payload semantics.
//!
//! Lifecycle state rides a `watch` channel (latest-value: a subscriber
//! always sees the current state, never a backlog). Delta streams ride a
//! bounded `broadcast` channel whose lagged receivers get an explicit
//! drop signal — a slow subscriber learns how many deltas it missed and
//! re-anchors, where a watch channel would silently gap the stream.
//! Publication on both channels is synchronous and never awaits a
//! subscriber, so a commit path can never be back-pressured by an
//! observer.

use std::{sync::Arc, time::Instant};

use dashmap::DashMap;
use tokio::sync::{broadcast, watch};

/// Delta buffer capacity per entry.
///
/// Bounds memory per live entity; a subscriber slower than this window
/// receives the lagged-drop signal rather than stalling the publisher.
pub const DEFAULT_DELTA_CAPACITY: usize = 256;

// ---------------------------------------------------------------------------
// HubEntry
// ---------------------------------------------------------------------------

/// One entity's channels and TTL metadata.
///
/// Created by whoever registers the entity; publishers send typed state
/// through `sender` and progress deltas through [`HubEntry::publish_delta`];
/// the background sweep evicts entries based on the TTL stamps.
pub struct HubEntry<S, D> {
    /// Watch channel sender carrying the entity's current state.
    pub sender: watch::Sender<S>,
    /// Keepalive receiver ensuring `sender.send()` always updates the
    /// stored value, even when no handler has subscribed yet.
    _keepalive_rx: watch::Receiver<S>,
    /// Bounded broadcast sender for progress deltas.
    delta_tx: broadcast::Sender<D>,
    /// When this entry was inserted into the map.
    pub inserted_at: Instant,
    /// When a terminal state was sent, if ever.
    pub terminal_at: Option<Instant>,
}

impl<S, D: Clone> HubEntry<S, D> {
    /// Creates an entry with an internal keepalive receiver and a delta
    /// buffer of [`DEFAULT_DELTA_CAPACITY`].
    #[must_use]
    pub fn new(sender: watch::Sender<S>, rx: watch::Receiver<S>) -> Self {
        Self {
            sender,
            _keepalive_rx: rx,
            delta_tx: broadcast::channel(DEFAULT_DELTA_CAPACITY).0,
            inserted_at: Instant::now(),
            terminal_at: None,
        }
    }

    /// Stamps `terminal_at` with the current instant if not already set.
    pub fn stamp_terminal(&mut self) {
        if self.terminal_at.is_none() {
            self.terminal_at = Some(Instant::now());
        }
    }

    /// Publishes one delta without awaiting anyone: a send with no
    /// subscribers is silently dropped (progress is advisory), and a
    /// subscriber slower than the buffer receives the lagged-drop signal
    /// on its next receive. Returns how many subscribers were reachable.
    pub fn publish_delta(&self, delta: D) -> usize {
        self.delta_tx.send(delta).unwrap_or(0)
    }

    /// Subscribes to the delta stream from this point onward.
    ///
    /// A receiver that falls more than the buffer behind gets
    /// [`broadcast::error::RecvError::Lagged`] carrying the missed count
    /// — the explicit signal to re-anchor on current state before
    /// continuing.
    #[must_use]
    pub fn subscribe_deltas(&self) -> broadcast::Receiver<D> {
        self.delta_tx.subscribe()
    }
}

#[cfg(feature = "test-helpers")]
impl<S, D: Clone> HubEntry<S, D> {
    /// Creates an entry with explicit timestamps for testing.
    #[must_use]
    pub fn with_timestamps_for_test(
        sender: watch::Sender<S>,
        rx: watch::Receiver<S>,
        inserted_at: Instant,
        terminal_at: Option<Instant>,
    ) -> Self {
        Self {
            sender,
            _keepalive_rx: rx,
            delta_tx: broadcast::channel(DEFAULT_DELTA_CAPACITY).0,
            inserted_at,
            terminal_at,
        }
    }
}

// ---------------------------------------------------------------------------
// EntityHub
// ---------------------------------------------------------------------------

/// Shared map of per-entity hub entries.
pub type EntityHub<K, S, D> = Arc<DashMap<K, HubEntry<S, D>>>;

#[cfg(test)]
mod tests {
    use super::*;

    fn an_entry() -> HubEntry<u32, String> {
        let (tx, rx) = watch::channel(0);
        HubEntry::new(tx, rx)
    }

    #[tokio::test]
    async fn test_lagged_subscriber_receives_the_drop_signal_and_re_anchors() {
        let (tx, rx) = watch::channel(0);
        let entry: HubEntry<u32, u64> = HubEntry {
            sender: tx,
            _keepalive_rx: rx,
            delta_tx: broadcast::channel(2).0,
            inserted_at: Instant::now(),
            terminal_at: None,
        };
        let mut deltas = entry.subscribe_deltas();

        // Four sends through a two-slot buffer: the subscriber missed two.
        for delta in 0..4_u64 {
            entry.publish_delta(delta);
        }

        let err = deltas
            .recv()
            .await
            .expect_err("a lagged subscriber must see the drop signal");
        assert!(matches!(
            err,
            broadcast::error::RecvError::Lagged(missed) if missed == 2,
        ));

        // Re-anchored: the buffered tail is still deliverable.
        assert_eq!(deltas.recv().await.expect("tail delta"), 2);
        assert_eq!(deltas.recv().await.expect("tail delta"), 3);
    }

    #[tokio::test]
    async fn test_publication_never_blocks_on_a_stalled_subscriber() {
        let entry = an_entry();
        let _stalled = entry.subscribe_deltas();

        // Far past the buffer, with the subscriber never receiving:
        // every publish returns immediately (the method is synchronous;
        // this pins that overflowing a stalled subscriber stays a plain
        // non-blocking send).
        for n in 0..(DEFAULT_DELTA_CAPACITY * 4) {
            let reached = entry.publish_delta(n.to_string());
            assert_eq!(reached, 1);
        }
    }

    #[tokio::test]
    async fn test_deltas_without_subscribers_are_silently_dropped() {
        let entry = an_entry();
        assert_eq!(entry.publish_delta("unheard".to_owned()), 0);
    }

    #[tokio::test]
    async fn test_watch_subscribers_see_latest_value_only() {
        let entry = an_entry();
        entry.sender.send_replace(1);
        entry.sender.send_replace(2);
        entry.sender.send_replace(3);

        let observer = entry.sender.subscribe();
        assert_eq!(
            *observer.borrow(),
            3,
            "state is latest-value: intermediate states are not a backlog",
        );
    }
}
