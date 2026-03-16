//! Per-job watch channel entry and shared map type.
//!
//! These types carry runtime/infrastructure concerns (tokio watch
//! channels, timestamps, concurrent maps) and live here rather than
//! in `tribal-domain` to keep the domain crate free of runtime
//! dependencies.

use std::{sync::Arc, time::Instant};

use dashmap::DashMap;
use tokio::sync::watch;
use tribal_domain::{JobId, JobState};

// ---------------------------------------------------------------------------
// JobWatchEntry
// ---------------------------------------------------------------------------

/// Per-job watch channel entry carrying typed state and TTL metadata.
///
/// Created by the ingest handler after committing a new job. The worker
/// sends [`JobState`] transitions through the `sender`; the MCP handler
/// subscribes to observe progress. The background sweep task evicts
/// entries based on TTL thresholds.
pub struct JobWatchEntry {
    /// Watch channel sender carrying the current [`JobState`].
    pub sender: watch::Sender<JobState>,
    /// Keepalive receiver ensuring `sender.send()` always updates the
    /// stored value, even when no handler has subscribed yet.
    _keepalive_rx: watch::Receiver<JobState>,
    /// When this entry was inserted into the map.
    pub inserted_at: Instant,
    /// When a terminal state was sent, if ever.
    pub terminal_at: Option<Instant>,
}

impl JobWatchEntry {
    /// Creates a new watch entry with an internal keepalive receiver.
    ///
    /// The keepalive receiver ensures `sender.send()` always updates
    /// the stored value, even before any handler subscribes.
    #[must_use]
    pub fn new(sender: watch::Sender<JobState>, rx: watch::Receiver<JobState>) -> Self {
        Self {
            sender,
            _keepalive_rx: rx,
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
}

#[cfg(feature = "test-helpers")]
impl JobWatchEntry {
    /// Creates a watch entry with explicit timestamps for testing.
    #[must_use]
    pub fn with_timestamps_for_test(
        sender: watch::Sender<JobState>,
        rx: watch::Receiver<JobState>,
        inserted_at: Instant,
        terminal_at: Option<Instant>,
    ) -> Self {
        Self {
            sender,
            _keepalive_rx: rx,
            inserted_at,
            terminal_at,
        }
    }
}

// ---------------------------------------------------------------------------
// JobStateTxs
// ---------------------------------------------------------------------------

/// Shared map of per-job watch channel entries.
pub type JobStateTxs = Arc<DashMap<JobId, JobWatchEntry>>;
