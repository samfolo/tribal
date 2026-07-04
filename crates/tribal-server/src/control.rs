//! The local control plane: a peer-authenticated Unix-socket JSON-RPC bridge an
//! operator client speaks to a running binary.
//!
//! The wire contract — every request, response, and event DTO — lives in
//! [`tribal_wire::control`]; this module is its transport in the binary. It
//! binds the socket, admits only the local operator (peer-credential UID match)
//! speaking a supported [contract version](tribal_wire::control::CONTROL_CONTRACT_VERSION),
//! frames JSON-RPC over `Content-Length`, dispatches the `config.*` crossings to
//! the config surface, and publishes a runtime descriptor a client discovers the
//! socket through. It is a control plane, sibling to `transport/` — not an MCP
//! [`TransportKind`](tribal_config::TransportKind) — and it never blocks the
//! binary from serving MCP: a plane that cannot bind is logged and skipped.

mod descriptor;
mod dispatch;
mod error;
mod framing;
mod socket;

pub(crate) use socket::{ControlContext, spawn_control_plane};
