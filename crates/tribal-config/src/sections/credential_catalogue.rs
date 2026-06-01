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
use thiserror::Error;
use tribal_domain::{ApiKey, ProviderKind, normalise_endpoint_url};

// ---------------------------------------------------------------------------
// Credential resolution error
// ---------------------------------------------------------------------------

/// A provider that requires an API key has none usable in the catalogue.
///
/// `kind` distinguishes the two fail-closed shapes the diagnostic must name
/// differently: no catalogue entry matches the endpoint at all, versus an entry
/// matches but its `api_key` is absent or blank.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{provider} at {base_url} requires an API key, but {kind}")]
pub struct MissingApiKey {
    /// The provider kind whose endpoint lacks a key.
    pub provider: ProviderKind,
    /// The normalised endpoint the lookup targeted.
    pub base_url: String,
    /// Which fail-closed shape this is.
    pub kind: MissingApiKeyKind,
}

/// Which fail-closed shape a [`MissingApiKey`] represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingApiKeyKind {
    /// No catalogue entry matches the endpoint's `(provider_kind, base_url)`.
    NoMatchingEntry,
    /// A catalogue entry matches, but its `api_key` is absent or whitespace.
    EmptyKey,
}

impl std::fmt::Display for MissingApiKeyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoMatchingEntry => f.write_str("no catalogue entry matches"),
            Self::EmptyKey => f.write_str("the matching catalogue entry has an empty key"),
        }
    }
}

// ---------------------------------------------------------------------------
// Connection name grammar
// ---------------------------------------------------------------------------

/// Returns `true` when `name` is a valid connection name: `[a-z][a-z0-9_]*`.
///
/// The grammar excludes hyphens because the env override
/// `TRIBAL_CREDENTIALS__<NAME>__API_KEY` upper-cases the name into a figment
/// double-underscore segment, and a hyphen is not a portable variable-name
/// character.
#[must_use]
pub fn is_valid_connection_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

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

impl CredentialEntry {
    /// Reports whether this entry names the given endpoint. An entry whose
    /// stored `base_url` fails to normalise cannot match a valid target (startup
    /// validation rejects such entries).
    fn matches_endpoint(&self, provider_kind: ProviderKind, normalised_base_url: &str) -> bool {
        self.provider_kind == provider_kind
            && normalise_endpoint_url(&self.base_url).as_deref().ok() == Some(normalised_base_url)
    }
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
            entry
                .matches_endpoint(provider_kind, normalised_base_url)
                .then_some((name.as_str(), entry))
        })
    }

    /// Resolves the API key for an endpoint, failing closed.
    ///
    /// Returns the matched key (the empty string for a provider that needs
    /// none), or [`MissingApiKey`] when a provider that requires a key has
    /// neither a catalogue entry nor an environment-supplied one. This is the
    /// single fail-closed rule shared by server boot (the active profile) and
    /// the reindex worker (a building target).
    ///
    /// # Errors
    ///
    /// Returns [`MissingApiKey`] when [`ProviderKind::requires_api_key`] holds
    /// and the resolved key is empty.
    pub fn resolve_api_key(
        &self,
        provider_kind: ProviderKind,
        normalised_base_url: &str,
    ) -> Result<&str, MissingApiKey> {
        let matched = self.resolve(provider_kind, normalised_base_url);
        let api_key = matched
            .and_then(|(_, entry)| entry.api_key.as_ref())
            .map_or("", ApiKey::as_str);

        if provider_kind.requires_api_key() && api_key.is_empty() {
            // Distinguish "no entry to fill" from "entry present, key blank" so
            // the fail-closed diagnostic names the right remediation: add the
            // connection versus set the key on the existing one.
            let kind = if matched.is_some() {
                MissingApiKeyKind::EmptyKey
            } else {
                MissingApiKeyKind::NoMatchingEntry
            };
            return Err(MissingApiKey {
                provider: provider_kind,
                base_url: normalised_base_url.to_owned(),
                kind,
            });
        }

        Ok(api_key)
    }

    /// Returns the entry for a connection name, if present.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&CredentialEntry> {
        self.0.get(name)
    }

    /// Inserts or replaces a named connection, returning any prior entry.
    ///
    /// Used to assemble a catalogue programmatically (for example `tribal
    /// bootstrap` writing the default connection); names should obey the
    /// `[a-z][a-z0-9_]*` grammar that [`is_valid_connection_name`] enforces.
    pub fn insert(&mut self, name: String, entry: CredentialEntry) -> Option<CredentialEntry> {
        self.0.insert(name, entry)
    }

    /// Supplies a key to the entry matching `(provider_kind, normalised base
    /// URL)` only when that entry currently has none, leaving an explicitly
    /// configured key untouched.
    ///
    /// This is the genesis convenience: `tribal bootstrap` writes a keyless
    /// `<provider>_default` skeleton, and the bare provider environment
    /// variable (for example `OPENAI_API_KEY`) supplies its key at runtime
    /// when neither an in-file value nor `TRIBAL_CREDENTIALS__<NAME>__API_KEY`
    /// did. An endpoint with no entry stays the fail-closed condition.
    pub fn fill_missing_key(
        &mut self,
        provider_kind: ProviderKind,
        normalised_base_url: &str,
        api_key: ApiKey,
    ) {
        for entry in self.0.values_mut() {
            if entry.api_key.is_none() && entry.matches_endpoint(provider_kind, normalised_base_url)
            {
                entry.api_key = Some(api_key);
                return;
            }
        }
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
    fn test_resolve_api_key_returns_the_matched_key() {
        let catalogue = catalogue_yaml(
            "openai_default:\n  provider_kind: openai\n  base_url: https://api.openai.com/v1\n  api_key: sk-test\n",
        );
        let normalised = normalise_endpoint_url("https://api.openai.com/v1").unwrap();
        assert_eq!(
            catalogue
                .resolve_api_key(ProviderKind::OpenAi, &normalised)
                .expect("resolves"),
            "sk-test",
        );
    }

    #[test]
    fn test_resolve_api_key_fails_closed_with_no_matching_entry() {
        let catalogue = CredentialCatalogue::default();
        let normalised = normalise_endpoint_url("https://api.openai.com/v1").unwrap();
        assert_eq!(
            catalogue.resolve_api_key(ProviderKind::OpenAi, &normalised),
            Err(MissingApiKey {
                provider: ProviderKind::OpenAi,
                base_url: normalised,
                kind: MissingApiKeyKind::NoMatchingEntry,
            }),
        );
    }

    #[test]
    fn test_resolve_api_key_fails_closed_when_the_matching_entry_has_no_key() {
        // The endpoint matches an entry, but that entry carries no `api_key`.
        let catalogue = catalogue_yaml(
            "openai_default:\n  provider_kind: openai\n  base_url: https://api.openai.com/v1\n",
        );
        let normalised = normalise_endpoint_url("https://api.openai.com/v1").unwrap();
        assert_eq!(
            catalogue.resolve_api_key(ProviderKind::OpenAi, &normalised),
            Err(MissingApiKey {
                provider: ProviderKind::OpenAi,
                base_url: normalised,
                kind: MissingApiKeyKind::EmptyKey,
            }),
        );
    }

    #[test]
    fn test_resolve_api_key_allows_a_keyless_local_provider() {
        let catalogue = CredentialCatalogue::default();
        let normalised = normalise_endpoint_url("http://localhost:11434").unwrap();
        assert_eq!(
            catalogue
                .resolve_api_key(ProviderKind::Ollama, &normalised)
                .expect("resolves"),
            "",
        );
    }

    #[test]
    fn test_connection_name_grammar() {
        // A trailing underscore is permitted; only a leading non-letter,
        // uppercase, hyphen, or other punctuation is rejected.
        for valid in ["openai_default", "o", "ollama2", "a_b_c", "x9", "trailing_"] {
            assert!(is_valid_connection_name(valid), "{valid:?} should be valid");
        }
        for invalid in ["", "Openai", "open-ai", "9start", "_lead", "has space"] {
            assert!(
                !is_valid_connection_name(invalid),
                "{invalid:?} should be invalid"
            );
        }
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
