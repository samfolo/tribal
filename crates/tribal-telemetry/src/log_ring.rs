//! The in-process log ring and its capturing `tracing` layer.
//!
//! [`LogRingLayer`] captures every event the subscriber's filter admits into a
//! bounded [`LogRing`] and publishes it as a `logs.line` control event. The ring
//! answers `logs.tail`; the event feeds a subscribed control client live. Both
//! read the same lines, so a client's tail and its live stream never disagree.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use chrono::Utc;
use tokio::sync::broadcast;
use tracing::{
    Event, Level, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::Context};
use tribal_wire::control::{ControlEvent, LogLevel, LogLine};

/// How many recent lines the ring retains. `logs.tail` is capped by this.
const LOG_RING_CAPACITY: usize = 512;

/// A bounded, shared ring of the most recent captured log lines.
///
/// Cloning shares the same underlying buffer: the capturing layer holds one
/// clone, the control context another, and both see the same lines.
#[derive(Clone)]
pub struct LogRing {
    buffer: Arc<Mutex<VecDeque<LogLine>>>,
    capacity: usize,
}

impl LogRing {
    /// A ring retaining up to `capacity` most-recent lines.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
        }
    }

    /// Appends a line, evicting the oldest once the ring is full.
    fn push(&self, line: LogLine) {
        let mut buffer = self.lock();
        if buffer.len() == self.capacity {
            buffer.pop_front();
        }
        buffer.push_back(line);
    }

    /// The last `count` lines, oldest first, capped by the ring's size.
    #[must_use]
    pub fn tail(&self, count: usize) -> Vec<LogLine> {
        let buffer = self.lock();
        let start = buffer.len().saturating_sub(count);
        buffer.iter().skip(start).cloned().collect()
    }

    /// Locks the buffer, recovering the guard if a prior holder panicked — a
    /// poisoned log ring loses no correctness, so it is never a reason to crash.
    fn lock(&self) -> MutexGuard<'_, VecDeque<LogLine>> {
        self.buffer.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The `tracing` layer that captures each admitted event into a [`LogRing`] and
/// publishes it as a [`ControlEvent::LogsLine`].
pub(crate) struct LogRingLayer {
    ring: LogRing,
    events: broadcast::Sender<ControlEvent>,
}

impl LogRingLayer {
    /// Builds the layer and returns the ring it captures into, so the caller can
    /// hand the ring to whatever answers `logs.tail`.
    pub(crate) fn new(events: broadcast::Sender<ControlEvent>) -> (Self, LogRing) {
        let ring = LogRing::new(LOG_RING_CAPACITY);
        (
            Self {
                ring: ring.clone(),
                events,
            },
            ring,
        )
    }
}

impl<S: Subscriber> Layer<S> for LogRingLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut message = MessageVisitor::default();
        event.record(&mut message);

        let line = LogLine {
            at: Utc::now(),
            level: level_of(*event.metadata().level()),
            target: event.metadata().target().to_owned(),
            message: message.0,
        };

        self.ring.push(line.clone());
        // A send fails only when no client subscribes; the ring still holds the
        // line for the next `logs.tail`, so the drop is by design.
        let _ = self.events.send(ControlEvent::LogsLine { line });
    }
}

/// Projects a `tracing` level onto the wire's closed level set.
fn level_of(level: Level) -> LogLevel {
    match level {
        Level::TRACE => LogLevel::Trace,
        Level::DEBUG => LogLevel::Debug,
        Level::INFO => LogLevel::Info,
        Level::WARN => LogLevel::Warn,
        Level::ERROR => LogLevel::Error,
    }
}

/// Captures a `tracing` event's primary `message` field, ignoring structured
/// fields the [`LogLine`] shape does not carry.
#[derive(Default)]
struct MessageVisitor(String);

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::{layer::SubscriberExt, registry};

    use super::*;

    #[test]
    fn test_an_event_fills_the_ring_and_publishes_a_logs_line() {
        let (events, mut subscriber) = broadcast::channel(8);
        let (layer, ring) = LogRingLayer::new(events);

        // A scoped subscriber, so the test never touches the global default.
        tracing::subscriber::with_default(registry().with(layer), || {
            tracing::info!("captured line");
        });

        let tail = ring.tail(10);
        assert_eq!(tail.len(), 1, "the captured line lands in the ring");
        assert_eq!(tail[0].message, "captured line");
        assert_eq!(tail[0].level, LogLevel::Info);

        match subscriber.try_recv().expect("a logs.line was published") {
            ControlEvent::LogsLine { line } => assert_eq!(line.message, "captured line"),
            other => panic!("expected logs.line, got {other:?}"),
        }
    }

    #[test]
    fn test_the_ring_retains_only_the_last_capacity_lines() {
        let ring = LogRing::new(2);
        for index in 0..5 {
            ring.push(line(index));
        }
        let tail = ring.tail(10);
        assert_eq!(tail.len(), 2, "the ring never exceeds its capacity");
        assert_eq!(tail[0].message, "3");
        assert_eq!(tail[1].message, "4", "the newest line is last");
    }

    #[test]
    fn test_tail_returns_the_last_n_oldest_first() {
        let ring = LogRing::new(8);
        for index in 0..5 {
            ring.push(line(index));
        }
        let tail = ring.tail(3);
        let messages: Vec<&str> = tail.iter().map(|entry| entry.message.as_str()).collect();
        assert_eq!(messages, ["2", "3", "4"], "the last three, oldest first");
    }

    #[test]
    fn test_tail_of_more_than_held_returns_all() {
        let ring = LogRing::new(8);
        ring.push(line(0));
        ring.push(line(1));
        assert_eq!(ring.tail(100).len(), 2);
    }

    fn line(index: u32) -> LogLine {
        LogLine {
            at: Utc::now(),
            level: LogLevel::Info,
            target: "test".to_owned(),
            message: index.to_string(),
        }
    }
}
