//! Secret redaction for resolved configuration output.
//!
//! [`SecretField`] is the single source of truth for which
//! configuration fields contain credentials. It owns the YAML
//! dot-path, the struct field mutation (in tests), and the sentinel
//! generation.

use tribal_domain::REDACTED;

#[cfg(test)]
use crate::TribalConfig;

/// Configuration fields that contain sensitive values.
///
/// Each variant maps to exactly one scalar YAML path in the resolved
/// configuration. Adding a variant requires implementing [`path`] and
/// [`plant_sentinel`] — the exhaustive match ensures neither can be
/// forgotten.
#[derive(Debug, Clone, Copy)]
pub enum SecretField {
    DatabaseUrl,
}

impl SecretField {
    /// All secret fields. Used by [`redact_secrets`] and tests.
    pub const ALL: &[Self] = &[Self::DatabaseUrl];

    /// The YAML dot-path for this field in serialised `TribalConfig`.
    #[must_use]
    pub fn path(self) -> &'static str {
        match self {
            Self::DatabaseUrl => "database.url",
        }
    }
}

#[cfg(test)]
impl SecretField {
    /// Sets this field on a config to the given value.
    ///
    /// The exhaustive match ensures a new variant without a
    /// corresponding mutation arm is a compile error.
    fn plant_sentinel(self, config: &mut TribalConfig, value: &str) {
        match self {
            Self::DatabaseUrl => config.database.url = value.to_owned(),
        }
    }

    /// Generates a unique sentinel string for this field.
    fn sentinel(self) -> String {
        format!("SENTINEL_{}", self.path().replace('.', "_").to_uppercase())
    }
}

/// Whether `key` is a configuration path whose value reads back redacted —
/// exactly the paths [`redact_secrets`] masks: a fixed [`SecretField`] leaf or a
/// catalogue entry's `api_key`. A write of the redaction mask to such a key must
/// be refused, never persisted over the real secret a redacted read hid.
#[must_use]
pub fn is_secret_key(key: &str) -> bool {
    SecretField::ALL.iter().any(|field| field.path() == key)
        || (key.starts_with("provider_connections.")
            && key.ends_with(".api_key")
            && key.matches('.').count() == 2)
}

/// Redacts secret values in a YAML string.
///
/// Parses the YAML, walks each [`SecretField`] path, replaces any
/// non-null values with [`REDACTED`], and re-serialises. Returns the
/// input unchanged if parsing fails.
#[must_use]
pub fn redact_secrets(yaml: &str) -> String {
    let Ok(mut value) = serde_yaml::from_str::<serde_yaml::Value>(yaml) else {
        return yaml.to_owned();
    };

    for field in SecretField::ALL {
        redact_path(&mut value, field.path());
    }

    // Provider connections are keyed by arbitrary names, so their
    // secret paths cannot be a static `SecretField` list; walk the map.
    redact_provider_connections(&mut value);

    serde_yaml::to_string(&value).unwrap_or_else(|_| yaml.to_owned())
}

/// Walks a dot-separated path into a YAML value and replaces the leaf
/// with the redaction placeholder if it is a non-null scalar.
fn redact_path(root: &mut serde_yaml::Value, path: &str) {
    let segments: Vec<&str> = path.split('.').collect();
    let mut current = root;

    for segment in &segments[..segments.len() - 1] {
        current = match current.get_mut(*segment) {
            Some(child) => child,
            None => return,
        };
    }

    let Some(leaf_key) = segments.last() else {
        return;
    };

    if let Some(leaf) = current.get_mut(*leaf_key)
        && !leaf.is_null()
        && !leaf.is_mapping()
        && !leaf.is_sequence()
    {
        *leaf = serde_yaml::Value::String(REDACTED.to_owned());
    }
}

/// Redacts every `provider_connections.<name>.api_key`.
fn redact_provider_connections(root: &mut serde_yaml::Value) {
    let Some(catalogue) = root
        .get_mut("provider_connections")
        .and_then(serde_yaml::Value::as_mapping_mut)
    else {
        return;
    };

    for (_name, entry) in catalogue.iter_mut() {
        if let Some(api_key) = entry.get_mut("api_key")
            && !api_key.is_null()
            && !api_key.is_mapping()
            && !api_key.is_sequence()
        {
            *api_key = serde_yaml::Value::String(REDACTED.to_owned());
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redacts_database_url() {
        let yaml = "database:\n  url: postgres://user:pass@host/db\n";
        let redacted = redact_secrets(yaml);
        assert!(redacted.contains("url: '********'"), "got: {redacted}");
        assert!(!redacted.contains("postgres://"), "got: {redacted}");
    }

    #[test]
    fn test_removed_stage_api_keys_are_not_treated_as_live_secrets() {
        let yaml = "\
inference:
  extraction:
    api_key: sk-secret-extraction
  triage:
    api_key: sk-secret-triage
  relation:
    api_key: sk-secret-relation
";
        let redacted = redact_secrets(yaml);
        assert_eq!(redacted.trim(), yaml.trim());
    }

    #[test]
    fn test_redacts_every_provider_connection_api_key() {
        let yaml = "\
provider_connections:
  openai_default:
    provider: openai
    base_url: https://api.openai.com/v1
    api_key: sk-secret-one
  ollama_staged:
    provider: ollama
    base_url: http://localhost:11434
    api_key: sk-secret-two
";
        let redacted = redact_secrets(yaml);
        assert!(!redacted.contains("sk-secret-one"), "got: {redacted}");
        assert!(!redacted.contains("sk-secret-two"), "got: {redacted}");
        // The non-secret endpoint fields survive.
        assert!(redacted.contains("api.openai.com"), "got: {redacted}");
    }

    #[test]
    fn test_preserves_null_values() {
        let yaml = "provider_connections:\n  openai_default:\n    api_key: null\n";
        let redacted = redact_secrets(yaml);
        assert!(redacted.contains("null"), "got: {redacted}");
        assert!(!redacted.contains(REDACTED), "got: {redacted}");
    }

    #[test]
    fn test_redacts_empty_string_value() {
        let yaml = "database:\n  url: ''\n";
        let redacted = redact_secrets(yaml);
        assert!(redacted.contains(REDACTED), "got: {redacted}");
    }

    #[test]
    fn test_preserves_non_secret_fields() {
        let yaml = "server:\n  transport: http\n  bind_address: '127.0.0.1:8725'\n";
        let redacted = redact_secrets(yaml);
        assert!(redacted.contains("http"), "got: {redacted}");
        assert!(redacted.contains("127.0.0.1:8725"), "got: {redacted}");
    }

    #[test]
    fn test_handles_missing_paths_gracefully() {
        let yaml = "server:\n  transport: stdio\n";
        let redacted = redact_secrets(yaml);
        assert_eq!(redacted.trim(), yaml.trim());
    }

    #[test]
    fn test_handles_non_leaf_path_gracefully() {
        // If a secret path points at a mapping instead of a scalar,
        // it should be left untouched.
        let yaml = "database:\n  url:\n    host: localhost\n    port: 5432\n";
        let redacted = redact_secrets(yaml);
        assert!(redacted.contains("localhost"), "got: {redacted}");
        assert!(redacted.contains("5432"), "got: {redacted}");
    }

    /// Serialises a real `TribalConfig` with sentinel values in every
    /// secret field, redacts, and asserts none of the sentinels survive.
    ///
    /// Iterates [`SecretField::ALL`] directly — there is no parallel
    /// list. If a variant is added without a corresponding
    /// [`SecretField::plant_sentinel`] arm, the code fails to compile.
    #[test]
    fn test_redaction_covers_all_secret_fields() {
        let mut config = crate::TribalConfig::default();

        for field in SecretField::ALL {
            field.plant_sentinel(&mut config, &field.sentinel());
        }

        let yaml = serde_yaml::to_string(&config).expect("config serialises");
        let redacted = redact_secrets(&yaml);

        for field in SecretField::ALL {
            assert!(
                !redacted.contains(&field.sentinel()),
                "sentinel for {} leaked through redaction:\n{redacted}",
                field.path(),
            );
        }
    }
}
