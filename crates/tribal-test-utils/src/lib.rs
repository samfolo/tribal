#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Shared test infrastructure for Tribal: domain type factories,
//! test database setup and teardown, mock inference providers,
//! and assertion helpers.

mod db;
mod error;

pub use db::{TestContext, TestTransaction, test_context};
pub use error::TestDbError;
