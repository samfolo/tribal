//! Shared helpers for repository implementations.
//!
//! This module contains cross-cutting concerns used by multiple
//! repositories: column-list formatting, constraint-violation mapping,
//! cursor encoding and decoding, and other utilities.

pub(super) mod columns;
pub(super) mod constraint;
pub(super) mod cursor;
pub(super) mod halfvec;
