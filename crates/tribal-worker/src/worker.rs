//! Worker struct and poll-claim-dispatch loop.
//!
//! The [`Worker`] owns the main event loop: poll for claimable tasks,
//! claim them atomically, dispatch each to the appropriate pipeline
//! stage, and commit or fail the result.  Concurrency is bounded by a
//! [`Semaphore`](tokio::sync::Semaphore) and graceful shutdown is
//! signalled via a [`CancellationToken`](tokio_util::sync::CancellationToken).

mod backoff;
mod dispatch;
mod heartbeat;

pub use dispatch::Worker;
