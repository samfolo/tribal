#![warn(clippy::pedantic)]
#![allow(clippy::must_use_candidate)]
#![deny(warnings)]
//! Shared test infrastructure for Tribal: domain type factories,
//! test database setup and teardown, mock inference providers,
//! and assertion helpers.

mod db;
pub mod duration;
mod error;
mod factories;
mod fixtures;
mod lifecycle;
mod mock;
pub mod polling;
mod render;
mod seeding;
mod setup;
// `snapshot` must be `pub mod` (not `mod` + `pub use`) because the
// `assert_json_snapshot!` macro references `$crate::snapshot::assert_json_snapshot_impl`
// in its expansion, which requires the module path to be public.
pub mod snapshot;
pub mod text;
mod tracing_capture;

pub use db::{
    TestDb, TestTransaction, build_test_template, cluster_serial_lock, env_lock, lazy_pool,
};
pub use error::TestDbError;
pub use factories::*;
pub use fixtures::*;
pub use lifecycle::*;
pub use mock::async_dispatch::*;
pub use render::render_to_string;
pub use seeding::*;
pub use setup::*;
pub use tracing_capture::{CapturedEvent, CapturedSpan, TracingCapture};
