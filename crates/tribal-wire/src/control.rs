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

mod config;
mod envelope;
mod event;
mod logs;
mod server;
mod token;

pub use config::{
    ConfigDocument, ConfigFieldMeta, ConfigGetRequest, ConfigPath, ConfigSchema, ConfigSetRequest,
    ConfigValidateRequest, ConfigValidation, ConfigValue, ConfigViolation, ConfigWriteOutcome,
    ReloadClass, WriteEffect,
};
pub use envelope::{
    ClientHello, ControlNotification, ControlRequest, ControlResponse, JsonRpcVersion, RequestId,
    ResponseError, ResponseResult, ServerHello,
};
pub use event::ControlEvent;
pub use logs::{LogLevel, LogLine, LogLines, LogsTailRequest};
pub use server::{ProjectSummary, RestartOutcome, ServerStatus, StopOutcome, WorkerStatus};
pub use token::{TokenInfo, TokenList};

/// The version of the control-bridge wire contract. A client presents it in its
/// [`ClientHello`](envelope::ClientHello) at connect; a mismatch the server does
/// not support is refused before any method is dispatched, and the payload
/// vocabulary grows only when this does.
pub const CONTROL_CONTRACT_VERSION: u16 = 1;
