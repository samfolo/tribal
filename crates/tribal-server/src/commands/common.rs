//! Constants shared across CLI command implementations.

// ---------------------------------------------------------------------------
// Database defaults
// ---------------------------------------------------------------------------

/// Default database URL used when no other source provides one.
///
/// Injected as a command-defaults-layer value so that the figment cascade
/// still respects YAML, env vars, and CLI overrides.
pub(crate) const DEFAULT_DATABASE_URL: &str = "postgresql://tribal@localhost:5432/tribal";

// ---------------------------------------------------------------------------
// Pool configuration
// ---------------------------------------------------------------------------

/// Maximum connections for single-operation command pools.
pub(crate) const COMMAND_POOL_MAX_CONNECTIONS: u32 = 1;

/// Statement timeout for CLI command operations (30 seconds).
///
/// Commands that run migrations use a longer timeout defined locally.
pub(crate) const COMMAND_STATEMENT_TIMEOUT_MS: u64 = 30_000;

// ---------------------------------------------------------------------------
// Project schema
// ---------------------------------------------------------------------------

/// Schema version for the project settings JSON shape.
///
/// Increment when the structure of `Project.settings` changes in a way
/// that requires migration of existing values. There is no canonical
/// source elsewhere — this is the single definition.
pub(crate) const PROJECT_SCHEMA_VERSION: u32 = 1;
