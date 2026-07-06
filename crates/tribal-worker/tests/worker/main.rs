//! Integration tests for the worker poll-claim-dispatch loop.
//!
//! Each test seeds data via committed raw connections (not pooled),
//! constructs a [`Worker`] with mock providers, runs the worker
//! briefly, then asserts on task and job state.
//!
//! Each test gets its own isolated database via `TestDb::new`, so tests
//! run in parallel without cross-test interference on the shared `tasks`
//! table and need no end-of-test cleanup.
//!
//! Seeding and assertion queries use `TestDb::raw_connection` rather than
//! pool connections to avoid the `PoolConnection::drop` spawn issue that
//! leaks connections.

mod agentic;
mod common;
mod coupling;
mod driver;
mod extraction;
mod fixtures;
mod lifecycle;
mod managed;
mod managed_run;
mod relation;
mod threads;
mod token_usage;
mod triage;
mod turn_loop;
