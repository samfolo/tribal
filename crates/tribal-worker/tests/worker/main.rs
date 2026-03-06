//! Integration tests for the worker poll-claim-dispatch loop.
//!
//! Each test seeds data via committed raw connections (not pooled),
//! constructs a [`Worker`] with mock providers, runs the worker
//! briefly, then asserts on task and job state.
//!
//! Tests are serialised via [`serial_lock`] because all workers claim
//! from the same `tasks` table — parallel execution causes cross-test
//! interference.
//!
//! Seeding and assertion queries use [`TestContext::raw_connection`]
//! rather than pool connections to avoid the `PoolConnection::drop`
//! spawn issue that leaks connections across serialised tests.

mod common;
mod extraction;
mod fixtures;
mod lifecycle;
mod relation;
mod token_usage;
mod triage;
