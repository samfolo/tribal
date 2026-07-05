//! Worker struct and poll-claim-dispatch loop.
//!
//! The [`Worker`] owns the main event loop: poll for claimable tasks,
//! claim them atomically, dispatch each to the appropriate pipeline
//! stage, and commit or fail the result.  Concurrency is bounded by a
//! [`Semaphore`](tokio::sync::Semaphore) and graceful shutdown is
//! signalled via a [`CancellationToken`](tokio_util::sync::CancellationToken).

pub(crate) mod backfill;
pub(crate) mod backoff;
pub mod coupling;
mod driver;
mod metering;
mod thread;
mod thread_sweep;

pub(crate) use thread::map_runtime_error;
mod dispatch;
pub(crate) mod heartbeat;
pub(crate) mod reindex;
pub(crate) mod reindex_ops;

pub use dispatch::Worker;
pub use heartbeat::ThreadReclaimStats;
pub use metering::MeteringTransport;
