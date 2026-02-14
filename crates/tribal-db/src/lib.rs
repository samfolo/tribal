#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Database layer for Tribal: repository traits and implementations,
//! sqlx queries, migrations, and connection pool management.

mod error;
mod pool;

pub use error::DbError;
pub use pool::create_pool;
