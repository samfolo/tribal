//! The local control plane: a peer-authenticated Unix-socket JSON-RPC bridge an
//! operator client speaks to a running binary.
//!
//! The wire contract — every request, response, and event DTO — lives in
//! [`tribal_wire::control`]; this module is its transport in the binary. It
//! binds the socket, admits only the local operator (peer-credential UID match)
//! speaking a supported [contract version](tribal_wire::control::CONTROL_CONTRACT_VERSION),
//! frames JSON-RPC over `Content-Length`, dispatches the `config.*`/`server.*`/
//! `token.*` crossings to the surfaces that answer them, and publishes a runtime
//! descriptor a client discovers the socket through. It is a control plane,
//! sibling to `transport/` — not an MCP [`TransportKind`](tribal_config::TransportKind)
//! — and it never blocks the binary from serving MCP: a plane that cannot bind
//! is logged and skipped.

use std::{path::PathBuf, sync::Arc, time::Instant};

use sqlx::PgPool;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tribal_config::TribalConfig;
use tribal_wire::control::{ControlEvent, ProjectSummary};

mod descriptor;
mod dispatch;
mod error;
mod event;
mod framing;
mod socket;

pub(crate) use socket::spawn_control_plane;

/// The control event bus's channel capacity. A subscriber that falls this many
/// events behind lags and is told to re-read state rather than blocking a
/// publisher (concurrency: data-loss by design over back-pressure).
pub(crate) const EVENT_BUS_CAPACITY: usize = 256;

/// Everything a served connection reads: the running configuration and the file
/// a write persists to, the MCP pool the `token.*` crossing reads through, and
/// the identity and lifecycle signals the crossings report.
///
/// It carries the extracted project summary and the pool rather than the whole
/// `AppState`, so the control plane depends only on what its crossings use.
pub(crate) struct ControlContext {
    /// The resolved configuration the server is running with.
    pub config: Arc<TribalConfig>,
    /// The YAML file `config.set` writes.
    pub config_path: PathBuf,
    /// The MCP read-path pool, for principal resolution and `token.list`.
    pub pool: PgPool,
    /// The process event bus. Each connection subscribes to fan events out to
    /// its client; `config.set` and the file watchers publish onto it.
    pub events: broadcast::Sender<ControlEvent>,
    /// The project this serve resolved, absent when none was.
    pub project: Option<ProjectSummary>,
    /// The serve-lifetime token; its cancellation is the worker-liveness and
    /// shutdown signal `server.status` reports and the accept loop stops on.
    pub cancellation_token: CancellationToken,
    /// When this serve began, the base for `uptime_seconds`.
    pub started_at: Instant,
    /// The binary's build version, reported in the handshake, status, and
    /// descriptor.
    pub binary_version: Arc<str>,
    /// The per-serve instance identity, reported in the status and descriptor.
    pub instance_id: Arc<str>,
    /// Whether a supervisor owns this process (governs the future restart
    /// contract; recorded in the descriptor).
    pub supervised: bool,
}
