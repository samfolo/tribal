//! Span attribute constants for consistent tracing instrumentation.
//!
//! These constants define the field names used in `tracing` spans across
//! the Tribal workspace.  Using constants rather than string literals
//! prevents typos and ensures consistent naming in structured log output
//! and OpenTelemetry export.
//!
//! # Usage
//!
//! ```ignore
//! use tribal_telemetry::span_attrs;
//!
//! let span = tracing::info_span!(
//!     "process_job",
//!     { span_attrs::PROJECT_ID } = %project_id,
//!     { span_attrs::JOB_ID } = %job_id,
//! );
//! ```

/// Span field name for the project identifier.
pub const PROJECT_ID: &str = "tribal.project_id";

/// Span field name for the principal (user or agent) key.
pub const PRINCIPAL_KEY: &str = "tribal.principal_key";

/// Span field name for the job identifier.
pub const JOB_ID: &str = "tribal.job_id";

/// Span field name for the task identifier.
pub const TASK_ID: &str = "tribal.task_id";

/// Span field name for the episode identifier.
pub const EPISODE_ID: &str = "tribal.episode_id";

/// Span field name for the transport type (e.g. `"stdio"`, `"http"`, `"sse"`).
pub const TRANSPORT: &str = "tribal.transport";
