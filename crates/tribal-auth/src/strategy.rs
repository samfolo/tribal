//! How the transport layer provides authentication to the handler.

use crate::context::AuthContext;

/// How the transport layer provides authentication to the handler.
///
/// New transports pick the variant that matches their auth model
/// without requiring changes to handler code. Per-connection
/// transports (e.g. stdio) supply [`AtCreation`](Self::AtCreation);
/// per-request transports (e.g. Streamable HTTP) supply
/// [`PerRequest`](Self::PerRequest) and inject the principal into
/// request context extensions via middleware.
pub enum TransportAuthStrategy {
    /// Principal resolved once at handler creation (e.g. stdio).
    AtCreation(AuthContext),

    /// Principal injected per-request into request context extensions
    /// by the transport middleware (e.g. Streamable HTTP).
    PerRequest,
}
