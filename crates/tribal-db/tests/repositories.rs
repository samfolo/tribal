//! Integration tests for repository implementations.
//!
//! All tests in this binary share a single testcontainers Postgres
//! instance via [`tribal_test_utils::test_context`].  Each test uses
//! [`TestTransaction`](tribal_test_utils::TestTransaction) for isolation
//! via transaction rollback.

mod principal;
mod project;
