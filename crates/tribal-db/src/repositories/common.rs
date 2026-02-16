//! Shared helpers for repository implementations.
//!
//! This module contains cross-cutting concerns used by multiple
//! repositories: constraint-violation mapping, cursor encoding and
//! decoding, and other utilities.

pub(super) mod constraint;
pub(super) mod cursor;
