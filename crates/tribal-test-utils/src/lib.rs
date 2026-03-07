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
mod mock_inference;
pub mod polling;
mod seeding;
mod setup;
pub mod stub_repositories;
mod text;

pub use db::{TestContext, TestTransaction, serial_lock, test_context};
pub use error::TestDbError;
pub use factories::*;
pub use lifecycle::*;
pub use mock_inference::*;
pub use seeding::*;
pub use setup::*;
