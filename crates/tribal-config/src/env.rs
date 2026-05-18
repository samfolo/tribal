//! Environment variable names used by the Tribal server.
//!
//! Centralised here so that runtime lookups and clap `env` attributes
//! reference the same source of truth.  Clap's `#[arg(env = "...")]`
//! requires a string literal, so each constant has a companion test in
//! the binary crate verifying it matches the attribute.

/// Prefix stripped from environment variables before mapping to config paths.
pub const ENV_PREFIX: &str = "TRIBAL_";

/// Separator between path segments inside a `TRIBAL_*` env var, as
/// recognised by the figment loader. `database.url` maps to
/// `TRIBAL_DATABASE__URL`.
pub const ENV_NESTED_SEPARATOR: &str = "__";

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

/// Builds the `TRIBAL_*` env var name a figment loader would honour
/// for the given dot-separated config path. `embedding.provider`
/// becomes `TRIBAL_EMBEDDING__PROVIDER`.
///
/// User-facing messages that suggest exporting a particular env var
/// should derive the name through this helper so the literal stays in
/// sync with the loader's mapping rule.
#[must_use]
pub fn env_var_for_path(config_path: &str) -> String {
    format!(
        "{ENV_PREFIX}{}",
        config_path
            .to_uppercase()
            .replace('.', ENV_NESTED_SEPARATOR),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_var_for_path_uppercases_and_substitutes_separator() {
        assert_eq!(
            env_var_for_path("embedding.provider"),
            "TRIBAL_EMBEDDING__PROVIDER",
        );
        assert_eq!(
            env_var_for_path("inference.extraction.provider"),
            "TRIBAL_INFERENCE__EXTRACTION__PROVIDER",
        );
        assert_eq!(
            env_var_for_path("server.transport"),
            "TRIBAL_SERVER__TRANSPORT",
        );
    }
}
