//! Config file rendering for first-run persistence.
//!
//! Two rendering modes target different first-run flows:
//!
//! - [`render_minimal_config`] for `tribal setup` — emits only the
//!   resolved database URL.
//! - [`render_persisted_config`] for `tribal bootstrap` — emits the
//!   resolved database URL plus every field the user pinned through a
//!   bootstrap CLI flag, sourcing each value from the resolved config so
//!   the cascade's view stays the source of truth.
//!
//! The persistence shape *is* a [`CliOverrides`] — the same type used at
//! the highest precedence of the figment cascade. The [`Persisted`] trait
//! maps each populated slot from "the user supplied this flag" to "the
//! resolved value for that field"; the renderer then serialises the
//! resulting [`CliOverrides`] via `serde_yaml`, with `skip_serializing_if`
//! omitting the unpopulated slots.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::{
    CliOverrides, DatabaseCliOverrides, EmbeddingCliOverrides, InferenceCliOverrides,
    InferenceStageCliOverrides, InitCliOverrides, PersistedCredentialEntry, TelemetryCliOverrides,
    TribalConfig,
    error::ConfigError,
    sections::{
        InferenceConfig, InitConfig, InitEmbeddingConfig, StageInferenceConfig, TelemetryConfig,
    },
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Header comment written at the top of the generated config file.
const CONFIG_HEADER: &str = "# Tribal configuration\n# See documentation for full options.\n";

// ---------------------------------------------------------------------------
// Persisted trait
// ---------------------------------------------------------------------------

/// Projects an override slot onto its persistable on-disk shape, sourcing
/// values from a resolved config slice.
///
/// `Some(_)` in the input means "the user supplied this flag"; `Some(_)`
/// in the output means "this field is part of the persisted snapshot,
/// pinned to the resolved value". `None` propagates unchanged.
///
/// Implementors must destructure `self` exhaustively (no `..`) so adding
/// a new field to the override type forces a compile-time decision about
/// whether the new field should persist.
pub(crate) trait Persisted {
    /// The resolved-config slice this override pairs with.
    type Config;

    /// Returns the override with each populated slot replaced by its
    /// resolved value from `config`.
    fn persisted(&self, config: &Self::Config) -> Self;
}

// ---------------------------------------------------------------------------
// Persisted impls
// ---------------------------------------------------------------------------

impl Persisted for EmbeddingCliOverrides {
    type Config = InitEmbeddingConfig;

    fn persisted(&self, config: &InitEmbeddingConfig) -> Self {
        let Self { provider, model } = self;
        Self {
            provider: provider.map(|_| config.provider),
            model: model.as_ref().map(|_| config.model.clone()),
        }
    }
}

impl Persisted for InitCliOverrides {
    type Config = InitConfig;

    fn persisted(&self, config: &InitConfig) -> Self {
        let Self { embedding } = self;
        Self {
            embedding: embedding.as_ref().map(|o| o.persisted(&config.embedding)),
        }
    }
}

impl Persisted for InferenceStageCliOverrides {
    type Config = StageInferenceConfig;

    fn persisted(&self, config: &StageInferenceConfig) -> Self {
        let Self { provider, model } = self;
        Self {
            provider: provider.map(|_| config.provider),
            model: model.as_ref().map(|_| config.model.clone()),
        }
    }
}

impl Persisted for InferenceCliOverrides {
    type Config = InferenceConfig;

    fn persisted(&self, config: &InferenceConfig) -> Self {
        let Self {
            extraction,
            triage,
            relation,
        } = self;
        Self {
            extraction: extraction.as_ref().map(|o| o.persisted(&config.extraction)),
            triage: triage.as_ref().map(|o| o.persisted(&config.triage)),
            relation: relation.as_ref().map(|o| o.persisted(&config.relation)),
        }
    }
}

impl Persisted for TelemetryCliOverrides {
    type Config = TelemetryConfig;

    fn persisted(&self, config: &TelemetryConfig) -> Self {
        let Self { otlp_endpoint } = self;
        Self {
            otlp_endpoint: otlp_endpoint
                .as_ref()
                .and_then(|_| config.otlp_endpoint.clone()),
        }
    }
}

impl Persisted for CliOverrides {
    type Config = TribalConfig;

    fn persisted(&self, config: &TribalConfig) -> Self {
        // `server` and `database` carry bespoke persistence rules — the
        // destructure binds them so a new top-level section forces this
        // match to be revisited.
        let Self {
            server: _,
            database: _,
            init,
            inference,
            telemetry,
            credentials: _,
        } = self;
        Self {
            // Server fields belong to the runtime invocation, never the
            // on-disk config.
            server: None,
            // The database URL is always persisted so the file is
            // round-trippable through `load_config` without further
            // flags.
            database: Some(DatabaseCliOverrides {
                url: Some(config.database.url.clone()),
            }),
            init: init.as_ref().map(|o| o.persisted(&config.init)),
            inference: inference.as_ref().map(|o| o.persisted(&config.inference)),
            telemetry: telemetry.as_ref().map(|o| o.persisted(&config.telemetry)),
            // When the genesis embedding identity is pinned, persist the
            // matching catalogue connection skeleton so the runtime has an
            // entry to resolve the credential into.
            credentials: init
                .as_ref()
                .and_then(|_| persisted_genesis_credentials(config)),
        }
    }
}

/// Synthesises the genesis `<provider>_default` connection skeleton when the
/// seeded embedding provider needs a key, so `tribal bootstrap` writes a
/// catalogue entry the runtime can resolve the credential into. The key is
/// supplied at runtime from the environment, never persisted.
fn persisted_genesis_credentials(
    config: &TribalConfig,
) -> Option<BTreeMap<String, PersistedCredentialEntry>> {
    let init = &config.init.embedding;
    if !init.provider.requires_api_key() {
        return None;
    }
    let base_url = init
        .base_url
        .clone()
        .unwrap_or_else(|| init.provider.default_base_url().to_owned());
    let name = format!("{}_default", init.provider.as_str());
    Some(BTreeMap::from([(
        name,
        PersistedCredentialEntry {
            provider_kind: init.provider,
            base_url,
        },
    )]))
}

// ---------------------------------------------------------------------------
// Strategy dispatch
// ---------------------------------------------------------------------------

/// Caller-selected strategy for rendering a config file at install time.
///
/// New variants force every dispatch site to handle them via the
/// exhaustive match in [`ConfigPersistence::render`].
#[derive(Clone, Copy)]
pub enum ConfigPersistence<'a> {
    /// Render only `database.url`.
    Minimal,
    /// Render `database.url` plus every flag pinned in the overrides.
    Persisted(&'a CliOverrides),
}

impl ConfigPersistence<'_> {
    /// Renders the YAML content for this strategy against `config`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Render`] if YAML serialisation fails.
    pub fn render(&self, config: &TribalConfig) -> Result<String, ConfigError> {
        match self {
            Self::Minimal => render_minimal_config(&config.database.url),
            Self::Persisted(overrides) => render_persisted_config(config, overrides),
        }
    }
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
    let minimal = CliOverrides {
        database: Some(DatabaseCliOverrides {
            url: Some(database_url.to_owned()),
        }),
        ..CliOverrides::default()
    };
    render(&minimal)
}

/// Renders the persistable subset of a resolved config for first-run
/// bootstrap.
///
/// `database.url` is always emitted using the resolved value from
/// `config`. Every other field is emitted only when the corresponding
/// override slot is populated — that is, only when the user supplied a
/// flag that pins the value.
///
/// # Errors
///
/// Returns [`ConfigError::Render`] if YAML serialisation fails.
pub fn render_persisted_config(
    config: &TribalConfig,
    overrides: &CliOverrides,
) -> Result<String, ConfigError> {
    render(&overrides.persisted(config))
}

// ---------------------------------------------------------------------------
// YAML serialisation
// ---------------------------------------------------------------------------

fn render<T: Serialize>(view: &T) -> Result<String, ConfigError> {
    let body = serde_yaml::to_string(view).map_err(|source| ConfigError::Render {
        source: Box::new(source),
    })?;
    Ok(format!("{CONFIG_HEADER}\n{body}"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::ProviderKind;

    use super::*;
    use crate::{ServerCliOverrides, sections::TransportKind};

    /// Parses the YAML body following the header.
    fn parse_yaml(content: &str) -> serde_yaml::Value {
        let body = content
            .strip_prefix(CONFIG_HEADER)
            .expect("rendered content should start with header");
        serde_yaml::from_str(body).expect("rendered config should be valid YAML")
    }

    // -- Minimal renderer ----------------------------------------------------

    #[test]
    fn test_render_minimal_config_is_valid_yaml() {
        let content = render_minimal_config("postgres://h/db").unwrap();
        let url = parse_yaml(&content)
            .get("database")
            .and_then(|d| d.get("url"))
            .and_then(|u| u.as_str())
            .map(str::to_owned);
        assert_eq!(url.as_deref(), Some("postgres://h/db"));
    }

    #[test]
    fn test_render_minimal_config_contains_header() {
        let content = render_minimal_config("postgres://h/db").unwrap();
        assert!(content.starts_with(CONFIG_HEADER));
    }

    // -- Persisted renderer --------------------------------------------------

    #[test]
    fn test_render_persisted_config_emits_only_database_when_no_overrides() {
        let config = TribalConfig::minimum_valid("postgres://h/db");
        let overrides = CliOverrides::default();

        let parsed = parse_yaml(&render_persisted_config(&config, &overrides).unwrap());
        let map = parsed.as_mapping().expect("top-level mapping");
        assert_eq!(map.len(), 1, "only `database` should appear: {parsed:?}");
        assert_eq!(
            parsed
                .get("database")
                .and_then(|d| d.get("url"))
                .and_then(|u| u.as_str()),
            Some("postgres://h/db"),
        );
    }

    /// Builds a `CliOverrides` whose genesis embedding identity is pinned.
    fn embedding_overrides(provider: Option<ProviderKind>, model: Option<String>) -> CliOverrides {
        CliOverrides {
            init: Some(InitCliOverrides {
                embedding: Some(EmbeddingCliOverrides { provider, model }),
            }),
            ..CliOverrides::default()
        }
    }

    #[test]
    fn test_render_persisted_config_emits_only_supplied_embedding_fields() {
        let mut config = TribalConfig::minimum_valid("postgres://h/db");
        config.init.embedding.provider = ProviderKind::OpenAi;
        config.init.embedding.model = "text-embedding-3-small".into();

        let overrides = embedding_overrides(Some(ProviderKind::OpenAi), None);

        let parsed = parse_yaml(&render_persisted_config(&config, &overrides).unwrap());
        let embedding = parsed
            .get("init")
            .and_then(|i| i.get("embedding"))
            .expect("init.embedding subtree");
        assert_eq!(
            embedding.get("provider").and_then(|p| p.as_str()),
            Some("openai"),
        );
        assert!(
            embedding.get("model").is_none(),
            "model should be omitted when no flag supplied: {parsed:?}",
        );
    }

    #[test]
    fn test_render_persisted_config_emits_full_embedding_when_both_flags_supplied() {
        let mut config = TribalConfig::minimum_valid("postgres://h/db");
        config.init.embedding.provider = ProviderKind::OpenAi;
        config.init.embedding.model = "text-embedding-3-small".into();

        let overrides = embedding_overrides(
            Some(ProviderKind::OpenAi),
            Some("text-embedding-3-small".into()),
        );

        let parsed = parse_yaml(&render_persisted_config(&config, &overrides).unwrap());
        let embedding = parsed
            .get("init")
            .and_then(|i| i.get("embedding"))
            .expect("init.embedding subtree");
        assert_eq!(
            embedding.get("provider").and_then(|p| p.as_str()),
            Some("openai"),
        );
        assert_eq!(
            embedding.get("model").and_then(|m| m.as_str()),
            Some("text-embedding-3-small"),
        );
    }

    #[test]
    fn test_render_persisted_config_emits_keyless_genesis_credential_skeleton() {
        let mut config = TribalConfig::minimum_valid("postgres://h/db");
        config.init.embedding.provider = ProviderKind::OpenAi;

        let overrides = embedding_overrides(Some(ProviderKind::OpenAi), None);

        let parsed = parse_yaml(&render_persisted_config(&config, &overrides).unwrap());
        let entry = parsed
            .get("credentials")
            .and_then(|c| c.get("openai_default"))
            .expect("openai_default connection skeleton");
        assert_eq!(
            entry.get("provider_kind").and_then(|p| p.as_str()),
            Some("openai"),
        );
        assert!(entry.get("base_url").and_then(|u| u.as_str()).is_some());
        assert!(
            entry.get("api_key").is_none(),
            "the key is never persisted: {parsed:?}",
        );
    }

    #[test]
    fn test_render_persisted_config_omits_credential_skeleton_for_keyless_provider() {
        let mut config = TribalConfig::minimum_valid("postgres://h/db");
        config.init.embedding.provider = ProviderKind::Ollama;

        let overrides = embedding_overrides(Some(ProviderKind::Ollama), None);

        let parsed = parse_yaml(&render_persisted_config(&config, &overrides).unwrap());
        assert!(
            parsed.get("credentials").is_none(),
            "a keyless provider needs no catalogue entry: {parsed:?}",
        );
    }

    #[test]
    fn test_render_persisted_config_emits_single_inference_stage() {
        let mut config = TribalConfig::minimum_valid("postgres://h/db");
        config.inference.triage.provider = ProviderKind::OpenAi;
        config.inference.triage.model = "gpt-4o-mini".into();

        let overrides = CliOverrides {
            inference: Some(InferenceCliOverrides {
                extraction: None,
                triage: Some(InferenceStageCliOverrides {
                    provider: Some(ProviderKind::OpenAi),
                    model: Some("gpt-4o-mini".into()),
                }),
                relation: None,
            }),
            ..CliOverrides::default()
        };

        let parsed = parse_yaml(&render_persisted_config(&config, &overrides).unwrap());
        let inference = parsed.get("inference").expect("inference subtree");
        assert!(inference.get("extraction").is_none());
        assert!(inference.get("relation").is_none());
        let triage = inference.get("triage").expect("triage stage");
        assert_eq!(
            triage.get("provider").and_then(|p| p.as_str()),
            Some("openai"),
        );
        assert_eq!(
            triage.get("model").and_then(|m| m.as_str()),
            Some("gpt-4o-mini"),
        );
    }

    #[test]
    fn test_render_persisted_config_emits_telemetry_endpoint() {
        let mut config = TribalConfig::minimum_valid("postgres://h/db");
        config.telemetry.otlp_endpoint = Some("http://collector.internal:4317".into());

        let overrides = CliOverrides {
            telemetry: Some(TelemetryCliOverrides {
                otlp_endpoint: Some("http://collector.internal:4317".into()),
            }),
            ..CliOverrides::default()
        };

        let parsed = parse_yaml(&render_persisted_config(&config, &overrides).unwrap());
        assert_eq!(
            parsed
                .get("telemetry")
                .and_then(|t| t.get("otlp_endpoint"))
                .and_then(|e| e.as_str()),
            Some("http://collector.internal:4317"),
        );
    }

    #[test]
    fn test_render_persisted_config_drops_server_overrides() {
        let config = TribalConfig::minimum_valid("postgres://h/db");
        let overrides = CliOverrides {
            // server overrides belong to the runtime invocation and must
            // never reach the on-disk config.
            server: Some(ServerCliOverrides {
                transport: Some(TransportKind::Http),
                bind_address: Some("0.0.0.0:8725".into()),
            }),
            ..CliOverrides::default()
        };

        let parsed = parse_yaml(&render_persisted_config(&config, &overrides).unwrap());
        let map = parsed.as_mapping().expect("top-level mapping");
        assert_eq!(map.len(), 1, "only `database` should appear: {parsed:?}");
        assert!(parsed.get("server").is_none());
    }

    #[test]
    fn test_render_persisted_config_database_value_comes_from_config_not_override() {
        let config = TribalConfig::minimum_valid("postgres://from-config/db");
        let overrides = CliOverrides {
            database: Some(DatabaseCliOverrides {
                url: Some("postgres://from-override/db".into()),
            }),
            ..CliOverrides::default()
        };

        // The override value is ignored — `database.url` is sourced from
        // the resolved config, so the cascade's final view always wins.
        let parsed = parse_yaml(&render_persisted_config(&config, &overrides).unwrap());
        assert_eq!(
            parsed
                .get("database")
                .and_then(|d| d.get("url"))
                .and_then(|u| u.as_str()),
            Some("postgres://from-config/db"),
        );
    }

    #[test]
    fn test_render_persisted_config_round_trips_through_loader() {
        // What we render must parse back through `TribalConfig` without
        // tripping `deny_unknown_fields` on any section.
        let mut config = TribalConfig::minimum_valid("postgres://h/db");
        config.init.embedding.provider = ProviderKind::OpenAi;
        config.init.embedding.model = "text-embedding-3-small".into();

        let overrides = embedding_overrides(
            Some(ProviderKind::OpenAi),
            Some("text-embedding-3-small".into()),
        );

        let rendered = render_persisted_config(&config, &overrides).unwrap();
        let parsed: TribalConfig =
            serde_yaml::from_str(rendered.strip_prefix(CONFIG_HEADER).unwrap())
                .expect("rendered config must round-trip through TribalConfig");
        assert_eq!(parsed.database.url, "postgres://h/db");
        assert_eq!(parsed.init.embedding.provider, ProviderKind::OpenAi);
        assert_eq!(parsed.init.embedding.model, "text-embedding-3-small");
        // The synthesised skeleton round-trips into a keyless catalogue entry.
        let normalised =
            tribal_domain::normalise_endpoint_url(ProviderKind::OpenAi.default_base_url()).unwrap();
        assert!(
            parsed
                .credentials
                .resolve(ProviderKind::OpenAi, &normalised)
                .is_some(),
            "the genesis connection must round-trip into the catalogue",
        );
    }
}
