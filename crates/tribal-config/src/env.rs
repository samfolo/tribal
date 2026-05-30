//! Environment variable names used by the Tribal server.
//!
//! Centralised here so that runtime lookups and clap `env` attributes
//! reference the same source of truth.  Clap's `#[arg(env = "...")]`
//! requires a string literal, so each constant has a companion test in
//! the binary crate verifying it matches the attribute.

use tribal_domain::ProviderKind;

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

/// Returns the standard environment variable name carrying the given
/// provider's API key, if one applies.
///
/// Consulted as a final fallback by the config loader when no
/// config-file `api_key` or `TRIBAL_*__API_KEY` env var is supplied.
/// Returns `None` for providers that do not require an API key.
#[must_use]
pub fn standard_env_var_name(kind: ProviderKind) -> Option<&'static str> {
    match kind {
        ProviderKind::Ollama => None,
        ProviderKind::Anthropic => Some(ENV_ANTHROPIC_API_KEY),
        ProviderKind::OpenAi => Some(ENV_OPENAI_API_KEY),
    }
}

/// Returns the publicly-advertised MCP URL override, if one is set.
///
/// Reads [`ENV_PUBLIC_MCP_URL`] once, treating an unset or blank value as
/// absent, so every value derived from it agrees on the URL clients are
/// told to reach.
#[must_use]
pub fn public_mcp_url_override() -> Option<String> {
    std::env::var(ENV_PUBLIC_MCP_URL)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

#[cfg(test)]
// `Jail::expect_with` closures return `Result<(), figment::Error>` (208 bytes),
// which we cannot reduce without wrapping an upstream type.
#[allow(clippy::result_large_err)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_env_var_name() {
        assert_eq!(standard_env_var_name(ProviderKind::Ollama), None);
        assert_eq!(
            standard_env_var_name(ProviderKind::Anthropic),
            Some(ENV_ANTHROPIC_API_KEY),
        );
        assert_eq!(
            standard_env_var_name(ProviderKind::OpenAi),
            Some(ENV_OPENAI_API_KEY),
        );
    }

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

    #[test]
    fn test_public_mcp_url_override_reads_value_and_treats_blank_as_absent() {
        figment::Jail::expect_with(|jail| {
            jail.set_env(ENV_PUBLIC_MCP_URL, "https://tribal.example.com/mcp");
            assert_eq!(
                public_mcp_url_override().as_deref(),
                Some("https://tribal.example.com/mcp"),
            );
            jail.set_env(ENV_PUBLIC_MCP_URL, "   ");
            assert_eq!(
                public_mcp_url_override(),
                None,
                "a blank value is treated as no override",
            );
            Ok(())
        });
    }
}
