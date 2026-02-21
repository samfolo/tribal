//! Declarative seed builder and executor for constructing multi-entity
//! knowledge graphs in the test database.
//!
//! The [`Seed`] builder accumulates commands as pure data. The executor
//! inserts entities via the existing repository layer when
//! [`Seed::execute`] is called. Pre-built scenarios provide common test
//! graph shapes.

mod embeddings;
mod executor;
mod repository;
pub mod scenarios;
mod types;

pub use repository::commit_relation_batch;
pub use types::{Seed, SeedItemSpec, SeedReferenceSpec, SeedResult, item, reference};
