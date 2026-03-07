//! Generic async dispatch infrastructure and domain-specific mocks.
//!
//! The [`core`] module provides `MockAsyncDispatchCore`, a generic
//! dispatch engine parameterised over request, response, and error
//! types.  The [`inference`] and [`repositories`] modules build on
//! this core for their respective domains.

pub(crate) mod core;
mod inference;
mod repositories;

pub use core::{ExhaustBehaviour, MockProviderOptions};

pub use inference::*;
pub use repositories::*;
