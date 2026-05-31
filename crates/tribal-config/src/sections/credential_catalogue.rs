//! The embedding credential catalogue.
//!
//! Named connections, each binding a `(provider_kind, base_url)` endpoint to
//! an API key. The runtime resolves the active embedding profile's credential
//! by matching its `(provider_kind, normalised_base_url)` against an entry, so
//! a corpus that migrates to a new endpoint keeps its credential reachable and
//! two same-kind endpoints can be live at once during a migration.
//!
//! Only the embedding credential lives here, because the embedding provider is
//! corpus state that migrates and the runtime must follow it; inference
//! credentials stay per-stage in ordinary config.
//!
//! Connection names obey `[a-z][a-z0-9_]*`: the env override
//! `TRIBAL_CREDENTIALS__<NAME>__API_KEY` upper-cases the name into a figment
//! double-underscore segment, and a hyphen is not a portable variable-name
//! character. The catalogue stores each `base_url` verbatim and normalises it
//! at resolution time, so an entry that matches a profile in YAML matches it in
//! the registry lookup.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tribal_domain::{ApiKey, ProviderKind, normalise_endpoint_url};

// ---------------------------------------------------------------------------
// CredentialEntry
// ---------------------------------------------------------------------------

/// One named connection: an endpoint plus its credential.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialEntry {
    /// The provider kind this connection serves.
    pub provider_kind: ProviderKind,

    /// The endpoint base URL, stored verbatim and normalised at resolution.
    pub base_url: String,

    /// The API key for the endpoint.
    ///
    /// Optional so a catalogue entry can name an endpoint whose secret is
    /// supplied only through the environment override; an absent or empty key
    /// is the fail-closed condition when the entry backs the active profile.
    #[serde(default)]
    pub api_key: Option<ApiKey>,
}

// ---------------------------------------------------------------------------
// CredentialCatalogue
// ---------------------------------------------------------------------------

/// A catalogue of named embedding connections, keyed by connection name.
///
/// Serialises transparently as a bare YAML map of `name -> entry`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CredentialCatalogue(BTreeMap<String, CredentialEntry>);

impl CredentialCatalogue {
    /// Returns the entry whose `(provider_kind, normalised base URL)` matches
    /// the given endpoint, together with its connection name, if any.
    ///
    /// Each entry's stored `base_url` is normalised for the comparison; an
    /// entry whose `base_url` fails to normalise cannot match a valid target
    /// and is skipped (startup validation rejects such entries).
    #[must_use]
    pub fn resolve(
        &self,
        provider_kind: ProviderKind,
        normalised_base_url: &str,
    ) -> Option<(&str, &CredentialEntry)> {
        self.0.iter().find_map(|(name, entry)| {
            let matches = entry.provider_kind == provider_kind
                && normalise_endpoint_url(&entry.base_url).as_deref().ok()
                    == Some(normalised_base_url);
            matches.then_some((name.as_str(), entry))
        })
    }

    /// Returns the entry for a connection name, if present.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&CredentialEntry> {
        self.0.get(name)
    }

    /// Iterates the catalogue's `(name, entry)` pairs in name order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &CredentialEntry)> {
        self.0.iter().map(|(name, entry)| (name.as_str(), entry))
    }

    /// Returns `true` when the catalogue holds no connections.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of connections in the catalogue.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn catalogue_yaml(body: &str) -> CredentialCatalogue {
        serde_yaml::from_str(body).expect("valid catalogue")
    }

    #[test]
    fn test_default_is_empty() {
        let catalogue = CredentialCatalogue::default();
        assert!(catalogue.is_empty());
        assert_eq!(catalogue.len(), 0);
    }

    #[test]
    fn test_deserialises_named_entries() {
        let catalogue = catalogue_yaml(
            "openai_default:\n  provider_kind: openai\n  base_url: https://api.openai.com/v1\n  api_key: sk-test\n",
        );
        assert_eq!(catalogue.len(), 1);
        let entry = catalogue.get("openai_default").expect("entry present");
        assert_eq!(entry.provider_kind, ProviderKind::OpenAi);
        assert_eq!(entry.base_url, "https://api.openai.com/v1");
        assert_eq!(entry.api_key.as_ref().map(ApiKey::as_str), Some("sk-test"));
    }

    #[test]
    fn test_resolve_matches_on_normalised_endpoint() {
        // Stored verbatim with a trailing slash and default-omitted port; the
        // lookup target is the normalised form.
        let catalogue = catalogue_yaml(
            "ollama_default:\n  provider_kind: ollama\n  base_url: http://localhost:11434/\n",
        );
        let normalised = normalise_endpoint_url("http://localhost:11434").unwrap();
        let (name, entry) = catalogue
            .resolve(ProviderKind::Ollama, &normalised)
            .expect("resolves");
        assert_eq!(name, "ollama_default");
        assert_eq!(entry.provider_kind, ProviderKind::Ollama);
    }

    #[test]
    fn test_resolve_misses_on_wrong_kind_or_endpoint() {
        let catalogue = catalogue_yaml(
            "ollama_default:\n  provider_kind: ollama\n  base_url: http://localhost:11434\n",
        );
        let normalised = normalise_endpoint_url("http://localhost:11434").unwrap();
        // Right endpoint, wrong kind.
        assert!(
            catalogue
                .resolve(ProviderKind::OpenAi, &normalised)
                .is_none()
        );
        // Right kind, wrong endpoint.
        let other = normalise_endpoint_url("http://localhost:9999").unwrap();
        assert!(catalogue.resolve(ProviderKind::Ollama, &other).is_none());
    }

    #[test]
    fn test_rejects_unknown_entry_field() {
        assert!(
            serde_yaml::from_str::<CredentialCatalogue>(
                "x:\n  provider_kind: ollama\n  base_url: http://h\n  bogus: 1\n",
            )
            .is_err()
        );
    }
}
