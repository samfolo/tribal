//! Tick-driven polling abstraction.
//!
//! Production code uses [`TimedPollScheduler`] which sleeps between
//! iterations and respects a deadline. Tests substitute
//! [`ImmediatePollScheduler`] to drive iterations without wall-clock
//! delays — iteration count is controlled by the mock queue depth.

#[cfg(test)]
use std::sync::atomic::{AtomicU32, Ordering};
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
/// without sleeping. The tick budget is derived from the wait duration
/// (one tick per second, matching the production interval) so tests
/// cannot loop forever if a mock is misconfigured.
#[cfg(test)]
pub(crate) struct ImmediatePollScheduler;

#[cfg(test)]
impl PollScheduler for ImmediatePollScheduler {
    type Ticker = ImmediateTickSource;

    fn create_ticker(&self, wait: Duration) -> Self::Ticker {
        ImmediateTickSource {
            remaining: AtomicU32::new(u32::try_from(wait.as_secs()).unwrap_or(u32::MAX)),
        }
    }
}

/// Tick source that returns `true` immediately up to a fixed budget,
/// then `false`. Prevents infinite loops in tests when mocks are
/// misconfigured.
#[cfg(test)]
pub(crate) struct ImmediateTickSource {
    remaining: AtomicU32,
}

#[cfg(test)]
impl TickSource for ImmediateTickSource {
    async fn tick(&self) -> bool {
        self.remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_ok()
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

    /// When `wait == interval`, the single sleep lands exactly on the
    /// deadline. The post-sleep gate (`now <= deadline`) must return
    /// `true` so the caller gets one poll — otherwise the wait was
    /// pointless.
    #[tokio::test(start_paused = true)]
    async fn test_timed_tick_source_returns_true_when_sleep_lands_on_deadline() {
        let ticker = TimedTickSource {
            deadline: Instant::now() + Duration::from_secs(1),
            interval: Duration::from_secs(1),
        };

        assert!(ticker.tick().await, "should allow a query at the deadline");
    }

    /// After the single tick that lands on the deadline, the next call
    /// to `tick` must return `false` — the pre-sleep gate sees
    /// `remaining == 0` and exits immediately.
    #[tokio::test(start_paused = true)]
    async fn test_timed_tick_source_returns_false_on_second_call_after_deadline() {
        let ticker = TimedTickSource {
            deadline: Instant::now() + Duration::from_secs(1),
            interval: Duration::from_secs(1),
        };

        assert!(
            ticker.tick().await,
            "first tick at deadline should return true"
        );
        assert!(
            !ticker.tick().await,
            "second tick past deadline should return false"
        );
    }

    /// With multiple intervals before the deadline, `tick` returns
    /// `true` for each interval then `false` once the deadline is
    /// reached.
    #[tokio::test(start_paused = true)]
    async fn test_timed_tick_source_multiple_intervals_then_stops() {
        let ticker = TimedTickSource {
            deadline: Instant::now() + Duration::from_secs(3),
            interval: Duration::from_secs(1),
        };

        assert!(ticker.tick().await, "tick 1 at 1s");
        assert!(ticker.tick().await, "tick 2 at 2s");
        assert!(ticker.tick().await, "tick 3 at 3s (deadline)");
        assert!(!ticker.tick().await, "tick 4 past deadline");
    }

    /// When the interval exceeds the wait duration, `tick` sleeps only
    /// for the remaining time and returns `true` for exactly one poll.
    #[tokio::test(start_paused = true)]
    async fn test_timed_tick_source_interval_exceeds_wait() {
        let ticker = TimedTickSource {
            deadline: Instant::now() + Duration::from_millis(500),
            interval: Duration::from_secs(2),
        };

        assert!(
            ticker.tick().await,
            "should sleep for remaining and allow one query"
        );
        assert!(!ticker.tick().await, "should stop after deadline");
    }

    #[tokio::test]
    async fn test_immediate_tick_source_respects_budget() {
        let scheduler = ImmediatePollScheduler;
        let ticker = scheduler.create_ticker(Duration::from_secs(3));

        assert!(ticker.tick().await, "tick 1 of 3");
        assert!(ticker.tick().await, "tick 2 of 3");
        assert!(ticker.tick().await, "tick 3 of 3");
        assert!(!ticker.tick().await, "budget exhausted");
    }
}
