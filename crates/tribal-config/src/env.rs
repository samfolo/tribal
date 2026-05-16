//! Environment variable names used by the Tribal server.
//!
//! Centralised here so that runtime lookups and clap `env` attributes
//! reference the same source of truth.  Clap's `#[arg(env = "...")]`
//! requires a string literal, so each constant has a companion test in
//! the binary crate verifying it matches the attribute.

/// Prefix stripped from environment variables before mapping to config paths.
pub const ENV_PREFIX: &str = "TRIBAL_";

/// Environment variable for the configuration file path.
pub const ENV_CONFIG_PATH: &str = "TRIBAL_CONFIG_PATH";

/// Environment variable for project ID override.
pub const ENV_PROJECT_ID: &str = "TRIBAL_PROJECT_ID";

/// Environment variable for the bearer token used in HTTP/SSE transport.
pub const ENV_AUTH_TOKEN: &str = "TRIBAL_AUTH_TOKEN";

/// Environment variable for the publicly-advertised MCP URL.
///
/// When set, overrides the bind-address-derived URL in HTTP/SSE
/// `mcp-config` snippets. Intended for deployments behind a reverse
/// proxy or load balancer where the URL clients should reach differs
/// from the server's local bind address.
pub const ENV_PUBLIC_MCP_URL: &str = "TRIBAL_PUBLIC_MCP_URL";

/// Standard environment variable for the `OpenAI` API key.
///
/// Consulted as a final fallback when no `TRIBAL_*__API_KEY` or
/// config-file `api_key` is supplied for an `OpenAi`-provider stage.
pub const ENV_OPENAI_API_KEY: &str = "OPENAI_API_KEY";

/// Standard environment variable for the `Anthropic` API key.
///
/// Consulted as a final fallback when no `TRIBAL_*__API_KEY` or
/// config-file `api_key` is supplied for an `Anthropic`-provider stage.
pub const ENV_ANTHROPIC_API_KEY: &str = "ANTHROPIC_API_KEY";
