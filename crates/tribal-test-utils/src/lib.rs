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
mod lifecycle;
mod mock;
pub mod polling;
mod seeding;
mod setup;
pub mod text;

pub use db::{TestContext, TestTransaction, lazy_pool, serial_lock, test_context};
pub use error::TestDbError;
pub use factories::*;
pub use lifecycle::*;
pub use mock::async_dispatch::*;
pub use seeding::*;
pub use setup::*;
