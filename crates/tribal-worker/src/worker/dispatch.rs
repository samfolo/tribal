//! Worker struct, construction, and the poll-claim-dispatch loop.
//!
//! Split into submodules by concern:
//!
//! - [`core`] — `Worker` struct, accessors, poll-claim loop, reclaim.
//! - [`commit`] — Domain-effect commit handlers for each pipeline stage.
//! - [`failure`] — Failure handling, backoff, and lifecycle events.

mod commit;
mod core;
mod failure;

pub use self::core::Worker;
