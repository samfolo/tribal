//! The control bridge wire contract: the DTOs a same-machine operator client
//! and a running `tribal` binary exchange over the local control socket.
//!
//! This module authors the operator crossings — config inspection and mutation,
//! server status and lifecycle, log tailing, and token metadata — plus the
//! server-initiated events a subscriber receives. The transport is JSON-RPC 2.0
//! with `Content-Length` framing; these types are its pure-serde payloads, the
//! single source the desktop client's DTOs generate from. The framing codec,
//! the socket, and dispatch live in the binary, never here.
//!
//! Two version pins bound compatibility: the JSON-RPC envelope carries the
//! frozen `"2.0"` marker, and the control contract itself grows only with
//! [`CONTROL_CONTRACT_VERSION`], exchanged at connect so an unknown-version
//! client is refused before it speaks.

mod check;
mod config;
mod envelope;
mod event;
mod logs;
mod models;
mod probe;
mod server;

pub use crate::token::{TokenInfo, TokenList};
pub use check::{CheckName, CheckReport, CheckReportRequest, CheckResult};
pub use config::{
    AudienceTier, ConfigDocument, ConfigFieldMeta, ConfigGetRequest, ConfigPath, ConfigSchema,
    ConfigSetRequest, ConfigValidateRequest, ConfigValidation, ConfigValue, ConfigViolation,
    ConfigWriteOutcome, ReloadClass, WriteEffect,
};
pub use envelope::{
    ClientHello, ControlNotification, ControlRequest, ControlResponse, JsonRpcVersion, RequestId,
    ResponseError, ResponseResult, ServerHello,
};
pub use event::ControlEvent;
pub use logs::{LogLevel, LogLine, LogLines, LogsTailRequest};
pub use models::{KnownModelEntry, ModelsCatalogue};
pub use probe::{
    CredentialProbe, CredentialProbeRequest, DatabaseProbe, DatabaseProbeRequest,
    EmbeddingProfileSummary, GraphEmbeddingProfile,
};
pub use server::{ProjectSummary, RestartOutcome, ServerStatus, StopOutcome, WorkerStatus};

/// The version of the control-bridge wire contract. A client presents it in its
/// [`ClientHello`](envelope::ClientHello) at connect; a mismatch the server does
/// not support is refused before any method is dispatched, and the payload
/// vocabulary grows only when this does.
pub const CONTROL_CONTRACT_VERSION: u16 = 2;

/// Control-plane [`ResponseError::code`] values, in the range JSON-RPC reserves
/// for implementation-defined server errors (`-32000..=-32099`), so a client
/// branches on a specific refusal rather than reading a generic internal error
/// off the reserved `-32603`.
pub mod error_code {
    /// The local principal is unavailable — `tribal setup` has not run — so a
    /// principal-scoped crossing such as `token.list` cannot answer.
    pub const PRINCIPAL_UNAVAILABLE: i32 = -32001;

    /// A `config.set` sent the redaction mask (`********`) as a secret's value.
    /// The write is refused so the mask never overwrites the real secret a
    /// redacted read never revealed.
    pub const SECRET_MASK_REJECTED: i32 = -32002;

    /// A `config.set` targeted `init.embedding.*` while the active embedding
    /// profile snapshot is unknown.
    pub const GENESIS_PROFILE_UNKNOWN: i32 = -32003;

    /// A `config.set` tried to change `init.embedding.*` away from the active
    /// embedding profile after genesis.
    pub const GENESIS_PROFILE_DRIFT: i32 = -32004;

    /// A `config.set` targeted a machine-owned value.
    pub const CONFIG_KEY_NOT_WRITABLE: i32 = -32005;
}
