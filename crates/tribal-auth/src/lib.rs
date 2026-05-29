#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Authentication and authorisation for Tribal.
//!
//! Owns the bearer-token resource-server plane (validation, scope
//! enforcement, middleware response shaping) and the OAuth 2.1
//! authorisation-server endpoints (dynamic client registration,
//! /authorize, /token, and the well-known metadata documents). Token
//! issuance from any path produces a row in the existing `auth_tokens`
//! store that the bearer middleware verifies identically to a
//! CLI-minted token.

mod authenticator;
mod context;
mod error;
mod issuance;
mod middleware;
pub mod oauth;
mod principal;
mod strategy;

pub use authenticator::Authenticator;
pub use context::AuthContext;
pub use error::{
    AuthError, DISPLAY_INVALID_TOKEN, DISPLAY_MISSING_TOKEN, DISPLAY_TOKEN_EXPIRED,
    DISPLAY_TOKEN_REVOKED,
};
pub use issuance::issue_token;
pub use middleware::{AuthMiddlewareState, require_bearer_auth};
pub use principal::AuthenticatedPrincipal;
pub use strategy::TransportAuthStrategy;
