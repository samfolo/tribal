//! Minimal config file rendering.
//!
//! Produces a YAML config file containing only the fields needed for a
//! fresh installation. The struct is serialised via `serde_yaml` to
//! guarantee valid YAML output.

use serde::Serialize;

use crate::error::ConfigError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Header comment written at the top of the generated config file.
const CONFIG_HEADER: &str = "# Tribal configuration\n# See documentation for full options.\n";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Minimal config file structure containing only the database URL.
#[derive(Serialize)]
struct MinimalConfig<'a> {
    database: MinimalDatabaseConfig<'a>,
}

/// Database section of the minimal config file.
#[derive(Serialize)]
struct MinimalDatabaseConfig<'a> {
    url: &'a str,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Renders a minimal config file containing only `database.url`.
///
/// The output includes a header comment followed by valid YAML.
///
/// # Errors
///
/// Returns [`ConfigError::Render`] if YAML serialisation fails.
pub fn render_minimal_config(database_url: &str) -> Result<String, ConfigError> {
    let minimal = MinimalConfig {
        database: MinimalDatabaseConfig { url: database_url },
    };

    let body = serde_yaml::to_string(&minimal).map_err(|source| ConfigError::Render {
        source: Box::new(source),
    })?;

    Ok(format!("{CONFIG_HEADER}\n{body}"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_minimal_config_is_valid_yaml() {
        let content = render_minimal_config("postgres://h/db").unwrap();
        let parsed: serde_yaml::Value =
            serde_yaml::from_str(&content).expect("rendered config should be valid YAML");
        let url = parsed
            .get("database")
            .and_then(|d| d.get("url"))
            .and_then(|u| u.as_str());
        assert_eq!(url, Some("postgres://h/db"));
    }

    #[test]
    fn test_render_minimal_config_contains_header() {
        let content = render_minimal_config("postgres://h/db").unwrap();
        assert!(content.starts_with(CONFIG_HEADER));
    }
}
