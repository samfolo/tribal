#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Database layer for Tribal: repository traits and implementations,
//! sqlx queries, migrations, and connection pool management.

mod error;
mod pool;
pub mod repositories;

/// Compiled migrations for the Tribal database schema.
///
/// Embeds all SQL migration files from `crates/tribal-db/migrations/` at
/// compile time. Used by [`tribal_test_utils::TestContext`] to run
/// migrations against test databases.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

pub use error::DbError;
pub use pool::create_pool;
pub use repositories::{
    NewPrincipal, NewProject, PgPrincipalRepository, PgProjectRepository, PrincipalRepository,
    ProjectRepository,
};
