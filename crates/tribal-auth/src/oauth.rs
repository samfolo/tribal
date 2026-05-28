//! OAuth 2.1 authorisation-server surface.
//!
//! Implements the metadata documents, the authorisation and token
//! endpoints, the CIMD fetcher with SSRF defences, the DCR
//! registration endpoint, and PKCE S256 verification. Successful
//! token issuance writes into the same `auth_tokens` store the
//! bearer middleware reads from; the OAuth flow does not introduce
//! a parallel token plane.

pub mod challenge;
pub mod config;
pub mod metadata;
pub mod pkce;
pub mod router;

pub use challenge::{BearerChallenge, build_bearer_challenge_header};
pub use config::{OAuthRuntimeConfig, OAuthRuntimeConfigError, canonicalise_resource_url};
pub use metadata::{
    AuthorizationServerMetadata, ProtectedResourceMetadata, authorization_server_metadata,
    protected_resource_metadata,
};
pub use pkce::{CodeChallenge, CodeVerifier, PkceParseError};
pub use router::{OAuthRouterState, oauth_router};
