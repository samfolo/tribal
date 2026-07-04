//! Failures setting up the control socket.
//!
//! These are the errors of *standing up* the plane — resolving a path, binding
//! the socket, writing the descriptor. They surface once, at serve entry, where
//! the control plane is best-effort: a failure is logged and the binary serves
//! MCP without it. Per-connection failures never reach here; a bad frame closes
//! its own connection.

use std::path::PathBuf;

use thiserror::Error;

/// A failure standing up the control socket.
#[derive(Debug, Error)]
pub(crate) enum ControlError {
    /// The derived socket path is longer than the platform's `sun_path` field.
    #[error("control socket path {path} is {length} bytes, over the {limit}-byte platform limit")]
    SocketPathTooLong {
        /// The path that did not fit.
        path: PathBuf,
        /// Its length in bytes.
        length: usize,
        /// The platform's `sun_path` capacity.
        limit: usize,
    },
    /// Another live server already holds the socket.
    #[error("a control server is already serving at {path}")]
    AlreadyServing {
        /// The socket path already in use.
        path: PathBuf,
    },
    /// Binding the Unix socket failed.
    #[error("failed to bind the control socket at {path}: {source}")]
    Bind {
        /// The socket path.
        path: PathBuf,
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// Preparing a directory or writing the descriptor or socket metadata failed.
    #[error("control filesystem operation failed at {path}: {source}")]
    Filesystem {
        /// The path being operated on.
        path: PathBuf,
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
}
