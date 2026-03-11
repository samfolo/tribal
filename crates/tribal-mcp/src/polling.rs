//! Tick-driven polling abstraction.
//!
//! Production code uses [`TimedPollScheduler`] which sleeps between
//! iterations and respects a deadline. Tests substitute
//! [`ImmediatePollScheduler`] to drive iterations without wall-clock
//! delays — iteration count is controlled by the mock queue depth.

use std::time::Duration;

use tokio::time::Instant;

// ---------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------

/// Creates a [`TickSource`] for a given wait duration.
///
/// The scheduler knows the polling interval; the wait duration
/// (from the caller's `wait_seconds`) determines the deadline.
pub(crate) trait PollScheduler: Send + Sync {
    /// The tick source type produced by this scheduler.
    type Ticker: TickSource;

    /// Creates a tick source that will respect the given wait duration.
    fn create_ticker(&self, wait: Duration) -> Self::Ticker;
}

/// Controls when the next polling iteration runs.
///
/// Each call to [`tick`](TickSource::tick) waits until the next
/// iteration should proceed. Returns `true` to continue polling,
/// `false` to stop (deadline expired).
pub(crate) trait TickSource: Send + Sync {
    /// Waits for the next tick. Returns `true` to continue polling,
    /// `false` if the deadline has expired.
    fn tick(&self) -> impl Future<Output = bool> + Send + '_;
}

// ---------------------------------------------------------------------------
// TimedPollScheduler (production)
// ---------------------------------------------------------------------------

/// Production scheduler that sleeps for a fixed interval between
/// polling iterations, capped by a deadline.
pub(crate) struct TimedPollScheduler {
    pub(crate) interval: Duration,
}

impl PollScheduler for TimedPollScheduler {
    type Ticker = TimedTickSource;

    fn create_ticker(&self, wait: Duration) -> Self::Ticker {
        TimedTickSource {
            deadline: Instant::now() + wait,
            interval: self.interval,
        }
    }
}

/// Tick source that sleeps for a fixed interval, respecting a deadline.
pub(crate) struct TimedTickSource {
    deadline: Instant,
    interval: Duration,
}

impl TickSource for TimedTickSource {
    async fn tick(&self) -> bool {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        tokio::time::sleep(remaining.min(self.interval)).await;
        Instant::now() <= self.deadline
    }
}

// ---------------------------------------------------------------------------
// ImmediatePollScheduler (tests)
// ---------------------------------------------------------------------------

/// Test scheduler that produces tick sources which resolve immediately
/// without sleeping. Iteration count is controlled entirely by the
/// mock queue depth — when the mock returns a terminal job status,
/// the polling loop breaks.
#[cfg(test)]
pub(crate) struct ImmediatePollScheduler;

#[cfg(test)]
impl PollScheduler for ImmediatePollScheduler {
    type Ticker = ImmediateTickSource;

    fn create_ticker(&self, _wait: Duration) -> Self::Ticker {
        ImmediateTickSource
    }
}

/// Tick source that always returns `true` immediately.
#[cfg(test)]
pub(crate) struct ImmediateTickSource;

#[cfg(test)]
impl TickSource for ImmediateTickSource {
    async fn tick(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_timed_tick_source_returns_false_when_deadline_passed() {
        let ticker = TimedTickSource {
            deadline: Instant::now(),
            interval: Duration::from_secs(1),
        };

        assert!(
            !ticker.tick().await,
            "should return false when deadline has passed"
        );
    }

    #[tokio::test]
    async fn test_timed_tick_source_returns_true_when_time_remains() {
        let ticker = TimedTickSource {
            deadline: Instant::now() + Duration::from_secs(10),
            interval: Duration::from_millis(1),
        };

        assert!(ticker.tick().await, "should return true when time remains");
    }

    #[tokio::test]
    async fn test_immediate_tick_source_always_returns_true() {
        let ticker = ImmediateTickSource;

        for _ in 0..5 {
            assert!(ticker.tick().await);
        }
    }
}
