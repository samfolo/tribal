//! The local control plane: a peer-authenticated Unix-socket JSON-RPC bridge an
//! operator client speaks to a running binary.
//!
//! The wire contract — every request, response, and event DTO — lives in
//! [`tribal_wire::control`]; this module is its transport in the binary. It
//! binds the socket, admits only the local operator (peer-credential UID match)
//! speaking a supported [contract version](tribal_wire::control::CONTROL_CONTRACT_VERSION),
//! frames JSON-RPC over `Content-Length`, dispatches the
//! `config.*`/`server.*`/`logs.*`/`token.*` crossings to the surfaces that
//! answer them, and publishes a runtime
//! descriptor a client discovers the socket through. It is a control plane,
//! sibling to `transport/` — not an MCP [`TransportKind`](tribal_config::TransportKind)
//! — and it never blocks the binary from serving MCP: a plane that cannot bind
//! is logged and skipped.

use std::{path::PathBuf, sync::Arc, time::Instant};

use sqlx::PgPool;
use tokio::sync::{Mutex, broadcast, watch};
use tokio_util::sync::CancellationToken;
use tribal_config::{CliShadow, TransportKind, TribalConfig};
use tribal_domain::EmbeddingProfile;
use tribal_telemetry::{LogFilterHandle, LogRing};
use tribal_wire::control::{ControlEvent, EmbeddingProfileSummary, ProjectSummary};

use crate::startup::SelfWriteSentinel;

mod descriptor;
mod dispatch;
mod error;
mod event;
mod framing;
mod socket;

pub(crate) use socket::spawn_control_plane;

/// The address a listening transport is bound to; `None` for stdio, which binds
/// nothing. Shared by `server.status` and the runtime descriptor so both report
/// the bound address by one rule.
fn listening_bind_address(config: &TribalConfig) -> Option<String> {
    (config.server.transport != TransportKind::Stdio)
        .then(|| config.server.bind_address.clone())
        .flatten()
}

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
    /// The served configuration snapshot: reads borrow the current value, and
    /// a live write swaps it once its side effect is in force.
    pub config: watch::Sender<Arc<TribalConfig>>,
    /// The YAML file `config.set` writes.
    pub config_path: PathBuf,
    /// The command-line overrides this serve launched with, so a `config.set`
    /// to a flag-set key reports the file write as shadowed rather than live.
    pub cli_shadow: CliShadow,
    /// Marks a `config.set`'s own file write so the config-file watcher does not
    /// re-announce it as an out-of-band edit.
    pub self_write: SelfWriteSentinel,
    /// Serialises the `config.set` read-modify-write persist, so two concurrent
    /// writes to different keys cannot lose an update on the shared file.
    pub config_write_lock: Mutex<()>,
    /// The MCP read-path pool, for principal resolution and `token.list`.
    pub pool: PgPool,
    /// Best known active embedding profile state, used to gate genesis-only
    /// writes without a database read on every `config.set`.
    pub embedding_profile: watch::Sender<EmbeddingProfileSnapshot>,
    /// The process event bus. Each connection subscribes to fan events out to
    /// its client; `config.set` and the file watchers publish onto it.
    pub events: broadcast::Sender<ControlEvent>,
    /// The bounded ring of recent log lines the capturing layer fills, read by
    /// `logs.tail`.
    pub log_ring: LogRing,
    /// The subscriber's level-filter handle — the `logging.level` hot-apply
    /// substrate.
    pub log_filter: LogFilterHandle,
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

impl ControlContext {
    /// The configuration snapshot reads serve, as of now.
    pub fn config_snapshot(&self) -> Arc<TribalConfig> {
        self.config.borrow().clone()
    }

    /// The active embedding profile snapshot reads serve, as of now.
    pub fn embedding_profile_snapshot(&self) -> EmbeddingProfileSnapshot {
        self.embedding_profile.borrow().clone()
    }
}

/// Best known active embedding profile state for the control surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EmbeddingProfileSnapshot {
    /// The profile could not be read yet.
    Unknown {
        /// Why the profile state is unknown.
        detail: String,
    },
    /// No active profile exists.
    NoProfile,
    /// The active profile identity.
    Active {
        /// Stable summary of the active profile.
        profile: EmbeddingProfileSummary,
    },
}

impl EmbeddingProfileSnapshot {
    /// Builds a snapshot from a domain profile.
    pub(crate) fn active(profile: &EmbeddingProfile) -> Self {
        Self::Active {
            profile: EmbeddingProfileSummary {
                provider: profile.provider_kind(),
                base_url: profile.normalised_base_url().to_owned(),
                model: profile.model().to_owned(),
                dimensions: profile.dimensions(),
            },
        }
    }
}
