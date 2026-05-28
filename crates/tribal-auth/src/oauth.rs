//! OAuth 2.1 authorisation-server surface.
//!
//! Implements the metadata documents, the authorisation and token
//! endpoints, the dynamic client registration endpoint, and PKCE S256
//! verification. Successful token issuance writes into the same
//! `auth_tokens` store the bearer middleware reads from; the OAuth
//! flow does not introduce a parallel token plane.

pub mod authorize;
pub mod challenge;
pub mod config;
pub mod error;
pub mod metadata;
pub mod pkce;
pub mod redirect;
pub mod register;
pub mod router;
pub mod token;

pub use authorize::{AuthorizeQuery, AuthorizeState, handle_authorize};
pub use challenge::{BearerChallenge, build_bearer_challenge_header};
pub use config::{OAuthRuntimeConfig, OAuthRuntimeConfigError, canonicalise_resource_url};
pub use error::{
    ClientMetadataRejection, InternalOperation, InvalidClientReason, InvalidGrantReason,
    InvalidRequestReason, InvalidTargetReason, OAuthError, RedirectUriRejection,
};
pub use metadata::{
    AuthorizationServerMetadata, ProtectedResourceMetadata, authorization_server_metadata,
    protected_resource_metadata,
};
pub use pkce::{CodeChallenge, CodeVerifier, PkceParseError};
pub use redirect::matches_redirect_uri;
pub use register::{RegisterRequest, RegisterResponse, RegisterState, handle_register};
pub use router::{OAuthRouterState, oauth_router};
pub use token::{TokenRequest, TokenResponse, TokenState, handle_token};
