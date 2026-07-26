//! Exhaustive validation of the merged configuration.
//!
//! All invariant violations are collected into a [`Diagnostics`] before
//! returning, so the operator sees every problem at once rather than
//! fixing them one at a time.

use std::net::SocketAddr;

use tracing_subscriber::EnvFilter;
use tribal_domain::{MAX_EMBEDDING_DIMENSIONS, ProviderKind, TransportKind};
use url::Url;

use crate::{
    MAX_LIFECYCLE_DURATION_MS, MAX_OVERFETCH_MULTIPLIER, MAX_TTL_HOURS,
    error::ConfigError,
    sections::{
        MAX_AUTHORIZATION_CODE_TTL_SECONDS, MAX_OAUTH_ACCESS_TOKEN_TTL_HOURS,
        MIN_AUTHORIZATION_CODE_TTL_SECONDS, ProviderConnectionViolation, TribalConfig,
    },
};

mod diagnostics;
mod error;

pub use diagnostics::Diagnostics;
pub use error::{
    ComputedFloor, ConfigPath, Endpoint, FieldValue, Inclusion, NumericRange, OrderRelation,
    ProviderStage, ValidationError,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PROVIDER_BASE_URL_REQUIREMENT: &str = "must omit the provider-owned request-path prefix";

/// Additional connections the worker pool requires beyond the concurrent task
/// count (heartbeat, reclaim, poll, and one spare).
const POOL_CONNECTION_OVERHEAD: u64 = 4;

/// Permitted range for cosine-similarity thresholds.  Shared by every
/// similarity-bearing config field; defining it once keeps the wording
/// uniform when [`ValidationError::OutOfRange`] renders.
pub(crate) const SIMILARITY_RANGE: NumericRange = NumericRange {
    low: Endpoint::open(0.0),
    high: Endpoint::closed(1.0),
};

/// Permitted range for per-stage sampling temperature. A gross-error guard
/// covering the widest provider maximum; per-model field admissibility is the
/// capability layer's concern, and a provider rejects a value above its own
/// tighter limit at request time.
pub(crate) const TEMPERATURE_RANGE: NumericRange = NumericRange {
    low: Endpoint::closed(0.0),
    high: Endpoint::closed(2.0),
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Validates the merged configuration.
///
/// Collects all errors and returns them together.
///
/// # Errors
///
/// Returns [`ConfigError::ValidationFailed`] if any field violates its
/// constraint.
pub fn validate(config: &TribalConfig) -> Result<(), ConfigError> {
    let mut diags = Diagnostics::default();

    validate_database(config, &mut diags);
    validate_server(config, &mut diags);
    validate_auth(config, &mut diags);
    validate_oauth(config, &mut diags);
    validate_worker(config, &mut diags);
    validate_agents(config, &mut diags);
    validate_pool_sizing(config, &mut diags);
    validate_init(config, &mut diags);
    validate_inference(config, &mut diags);
    validate_provider_connections(config, &mut diags);
    validate_provider_limits(config, &mut diags);
    validate_discovery(config, &mut diags);
    validate_exploration(config, &mut diags);
    validate_logging(config, &mut diags);
    validate_telemetry(config, &mut diags);

    if diags.is_empty() {
        Ok(())
    } else {
        Err(ConfigError::ValidationFailed { diagnostics: diags })
    }
}

/// Collects non-fatal configuration advisories.
///
/// Unlike [`validate`], these never block startup. They surface inert or
/// surprising combinations that validation admits but the operator may not
/// have intended (for example a verifier configured under the one-shot
/// executor, where it never runs). A caller logs each as a warning.
#[must_use]
pub fn config_warnings(config: &TribalConfig) -> Vec<&'static str> {
    config.agents.advisories()
}

// ---------------------------------------------------------------------------
// Section validators
// ---------------------------------------------------------------------------

fn validate_database(config: &TribalConfig, diags: &mut Diagnostics) {
    if config.database.url.is_empty() {
        diags.push(ValidationError::Empty {
            field: ConfigPath::from_static("database.url"),
        });
    }

    if config.database.pool_mcp_max_connections == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "database.pool_mcp_max_connections",
        )));
    }

    if config.database.pool_worker_max_connections == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "database.pool_worker_max_connections",
        )));
    }
}

fn validate_server(config: &TribalConfig, diags: &mut Diagnostics) {
    if config.server.transport == TransportKind::Stdio && config.server.bind_address.is_some() {
        diags.push(ValidationError::BindAddressStdioConflict);
    }

    if let Some(ref addr) = config.server.bind_address
        && addr.parse::<SocketAddr>().is_err()
    {
        diags.push(ValidationError::BindAddressMalformed {
            value: addr.clone(),
        });
    }

    if config.server.shutdown_deadline_ms == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "server.shutdown_deadline_ms",
        )));
    }

    if config.server.job_state_ttl_seconds == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "server.job_state_ttl_seconds",
        )));
    }

    if config.server.job_state_hard_ttl_seconds == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "server.job_state_hard_ttl_seconds",
        )));
    } else if config.server.job_state_ttl_seconds > 0
        && config.server.job_state_hard_ttl_seconds < config.server.job_state_ttl_seconds
    {
        diags.push(ValidationError::FieldOrdering {
            subject: FieldValue {
                field: ConfigPath::from_static("server.job_state_hard_ttl_seconds"),
                value: config.server.job_state_hard_ttl_seconds,
            },
            bound: FieldValue {
                field: ConfigPath::from_static("server.job_state_ttl_seconds"),
                value: config.server.job_state_ttl_seconds,
            },
            relation: OrderRelation::AtLeast,
        });
    }

    let sse = &config.server.sse;

    if sse.max_connection_age_ms == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "server.sse.max_connection_age_ms",
        )));
    } else if sse.max_connection_age_ms > MAX_LIFECYCLE_DURATION_MS {
        diags.push(ValidationError::AboveMax {
            field: ConfigPath::from_static("server.sse.max_connection_age_ms"),
            value: sse.max_connection_age_ms,
            limit: MAX_LIFECYCLE_DURATION_MS,
        });
    }

    if sse.idle_timeout_ms == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "server.sse.idle_timeout_ms",
        )));
    } else if sse.idle_timeout_ms > MAX_LIFECYCLE_DURATION_MS {
        diags.push(ValidationError::AboveMax {
            field: ConfigPath::from_static("server.sse.idle_timeout_ms"),
            value: sse.idle_timeout_ms,
            limit: MAX_LIFECYCLE_DURATION_MS,
        });
    }

    if sse.keepalive_interval_ms == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "server.sse.keepalive_interval_ms",
        )));
    } else if sse.keepalive_interval_ms > MAX_LIFECYCLE_DURATION_MS {
        diags.push(ValidationError::AboveMax {
            field: ConfigPath::from_static("server.sse.keepalive_interval_ms"),
            value: sse.keepalive_interval_ms,
            limit: MAX_LIFECYCLE_DURATION_MS,
        });
    } else if sse.idle_timeout_ms > 0 && sse.keepalive_interval_ms >= sse.idle_timeout_ms {
        diags.push(ValidationError::FieldOrdering {
            subject: FieldValue {
                field: ConfigPath::from_static("server.sse.keepalive_interval_ms"),
                value: sse.keepalive_interval_ms,
            },
            bound: FieldValue {
                field: ConfigPath::from_static("server.sse.idle_timeout_ms"),
                value: sse.idle_timeout_ms,
            },
            relation: OrderRelation::LessThan,
        });
    }
}

fn validate_auth(config: &TribalConfig, diags: &mut Diagnostics) {
    let ttl = config.auth.token_ttl_hours;
    if ttl == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "auth.token_ttl_hours",
        )));
    } else if ttl > MAX_TTL_HOURS {
        diags.push(ValidationError::AboveMax {
            field: ConfigPath::from_static("auth.token_ttl_hours"),
            value: ttl,
            limit: MAX_TTL_HOURS,
        });
    }
}

fn validate_oauth(config: &TribalConfig, diags: &mut Diagnostics) {
    let access_ttl = config.oauth.access_token_ttl_hours;
    if access_ttl == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "oauth.access_token_ttl_hours",
        )));
    } else if access_ttl > MAX_OAUTH_ACCESS_TOKEN_TTL_HOURS {
        diags.push(ValidationError::AboveMax {
            field: ConfigPath::from_static("oauth.access_token_ttl_hours"),
            value: access_ttl,
            limit: MAX_OAUTH_ACCESS_TOKEN_TTL_HOURS,
        });
    }

    let code_ttl = config.oauth.authorization_code_ttl_seconds;
    if code_ttl < MIN_AUTHORIZATION_CODE_TTL_SECONDS {
        diags.push(ValidationError::BelowMin {
            field: ConfigPath::from_static("oauth.authorization_code_ttl_seconds"),
            value: code_ttl,
            min: MIN_AUTHORIZATION_CODE_TTL_SECONDS,
        });
    } else if code_ttl > MAX_AUTHORIZATION_CODE_TTL_SECONDS {
        diags.push(ValidationError::AboveMax {
            field: ConfigPath::from_static("oauth.authorization_code_ttl_seconds"),
            value: code_ttl,
            limit: MAX_AUTHORIZATION_CODE_TTL_SECONDS,
        });
    }

    // Fail fast at load time on a malformed or unsupported issuer/resource
    // URL, rather than only when the runtime config is built at serve. The
    // advertised MCP URL is the third routability input, so it carries the
    // same load-time guard: a malformed value must not silently classify
    // as loopback and reopen DCR's `/register`.
    validate_issuer_url(config.oauth.issuer_url.as_deref(), diags);
    validate_resource_url(config.oauth.resource_url.as_deref(), diags);
    validate_public_mcp_url(config.server.public_mcp_url.as_deref(), diags);
}

/// Required form of `oauth.issuer_url`.
const ISSUER_ORIGIN_REQUIREMENT: &str = "must be an origin URL with no path, query, or fragment";

/// Required form of `oauth.resource_url` (RFC 8707).
const RESOURCE_FRAGMENT_REQUIREMENT: &str = "must not contain a fragment";

/// Validates `oauth.issuer_url`: when set, it must parse and be an origin
/// (no path, query, or fragment).
///
/// The authorisation-server metadata endpoints are appended to the issuer
/// and served at absolute root paths, so a sub-path issuer would advertise
/// endpoints the router does not serve. An unset field is admissible (the
/// consumer derives it from the bind address).
fn validate_issuer_url(value: Option<&str>, diags: &mut Diagnostics) {
    let Some(raw) = value.filter(|raw| !raw.is_empty()) else {
        return;
    };
    let field = ConfigPath::from_static("oauth.issuer_url");
    let Ok(url) = Url::parse(raw) else {
        diags.push(ValidationError::UrlMalformed {
            field,
            value: raw.to_owned(),
        });
        return;
    };
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        diags.push(ValidationError::UrlUnsupportedForm {
            field,
            value: raw.to_owned(),
            requirement: ISSUER_ORIGIN_REQUIREMENT,
        });
    }
}

/// Validates `oauth.resource_url`: when set, it must parse and carry no
/// fragment (RFC 8707 forbids a fragment on a resource indicator). An
/// unset field is admissible (the consumer derives it from the bind
/// address).
fn validate_resource_url(value: Option<&str>, diags: &mut Diagnostics) {
    let Some(raw) = value.filter(|raw| !raw.is_empty()) else {
        return;
    };
    let field = ConfigPath::from_static("oauth.resource_url");
    let Ok(url) = Url::parse(raw) else {
        diags.push(ValidationError::UrlMalformed {
            field,
            value: raw.to_owned(),
        });
        return;
    };
    if url.fragment().is_some() {
        diags.push(ValidationError::UrlUnsupportedForm {
            field,
            value: raw.to_owned(),
            requirement: RESOURCE_FRAGMENT_REQUIREMENT,
        });
    }
}

/// The form `server.public_mcp_url` must take: an `http`/`https` URL with a
/// host and no fragment. A path such as `/mcp` is preserved.
pub const PUBLIC_MCP_URL_REQUIREMENT: &str = "must be an http(s) URL with a host and no fragment";

/// Returns `true` when `raw` is a usable public MCP endpoint per
/// [`PUBLIC_MCP_URL_REQUIREMENT`]. Shared by load-time validation and the
/// non-validating `mcp-config` renderer so both reject the same shapes.
#[must_use]
pub fn is_valid_public_mcp_url(raw: &str) -> bool {
    Url::parse(raw).is_ok_and(|url| {
        matches!(url.scheme(), "http" | "https") && url.host().is_some() && url.fragment().is_none()
    })
}

/// Validates `server.public_mcp_url`: when set, it must be a usable public
/// MCP endpoint (see [`PUBLIC_MCP_URL_REQUIREMENT`]).
///
/// The advertised endpoint is one of the routability inputs and is also
/// emitted verbatim into the wire-up snippet, so a malformed or non-endpoint
/// value is rejected at load rather than silently classifying as loopback
/// (which would reopen DCR's unauthenticated `/register`) or shipping a
/// broken URL to the client.
fn validate_public_mcp_url(value: Option<&str>, diags: &mut Diagnostics) {
    let Some(raw) = value.filter(|raw| !raw.is_empty()) else {
        return;
    };
    let field = ConfigPath::from_static("server.public_mcp_url");
    if Url::parse(raw).is_err() {
        diags.push(ValidationError::UrlMalformed {
            field,
            value: raw.to_owned(),
        });
    } else if !is_valid_public_mcp_url(raw) {
        diags.push(ValidationError::UrlUnsupportedForm {
            field,
            value: raw.to_owned(),
            requirement: PUBLIC_MCP_URL_REQUIREMENT,
        });
    }
}

/// Validates a model-ID field: it must be a non-empty token with no whitespace.
fn validate_model_id(field: ConfigPath, model: &str, diags: &mut Diagnostics) {
    if model.is_empty() {
        diags.push(ValidationError::Empty { field });
    } else if model.chars().any(char::is_whitespace) {
        diags.push(ValidationError::ContainsWhitespace { field });
    }
}

fn validate_init(config: &TribalConfig, diags: &mut Diagnostics) {
    let init = &config.init.embedding;

    validate_model_id(
        ConfigPath::from_static("init.embedding.model"),
        &init.model,
        diags,
    );

    // `dimensions` is optional: `None` resolves through the embedding
    // service's native-dimension chain at provisioning. An explicit value is
    // bounded by the same `1..=MAX_EMBEDDING_DIMENSIONS` window the storage
    // CHECK enforces, so a grossly out-of-range seed is caught at config time
    // rather than far from the source at provisioning.
    match init.dimensions {
        Some(0) => diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "init.embedding.dimensions",
        ))),
        Some(dimensions) if dimensions > MAX_EMBEDDING_DIMENSIONS => {
            diags.push(ValidationError::AboveMax {
                field: ConfigPath::from_static("init.embedding.dimensions"),
                value: u64::from(dimensions),
                limit: u64::from(MAX_EMBEDDING_DIMENSIONS),
            });
        }
        Some(_) | None => {}
    }
}

fn validate_worker(config: &TribalConfig, diags: &mut Diagnostics) {
    config.worker.validate(diags);
}

fn validate_agents(config: &TribalConfig, diags: &mut Diagnostics) {
    config.agents.validate(diags);
}

fn validate_pool_sizing(config: &TribalConfig, diags: &mut Diagnostics) {
    let available = u64::from(config.database.pool_worker_max_connections);
    let max_concurrent_tasks =
        u64::try_from(config.worker.max_concurrent_tasks).unwrap_or(u64::MAX);
    let required = max_concurrent_tasks.saturating_add(POOL_CONNECTION_OVERHEAD);

    if available < required {
        diags.push(ValidationError::DerivedFloor {
            field: ConfigPath::from_static("database.pool_worker_max_connections"),
            value: available,
            floor: ComputedFloor {
                value: required,
                addend: ConfigPath::from_static("worker.max_concurrent_tasks"),
                overhead: POOL_CONNECTION_OVERHEAD,
            },
        });
    }
}

fn validate_inference(config: &TribalConfig, diags: &mut Diagnostics) {
    // Range checks apply only to set values; `None` means provider default
    // and is always admissible.
    for (stage, cfg) in config.inference.stages() {
        let prefix = stage.config_path();
        validate_model_id(ConfigPath::child(prefix, "model"), &cfg.model, diags);

        if let Some(temperature) = cfg.temperature
            && !TEMPERATURE_RANGE.contains(temperature)
        {
            diags.push(ValidationError::OutOfRange {
                field: ConfigPath::child(prefix, "temperature"),
                value: temperature,
                range: TEMPERATURE_RANGE,
            });
        }

        if cfg.max_tokens == Some(0) {
            diags.push(ValidationError::must_be_positive(ConfigPath::child(
                prefix,
                "max_tokens",
            )));
        }
    }
}

fn validate_provider_connections(config: &TribalConfig, diags: &mut Diagnostics) {
    let stages = config.inference.stages().collect::<Vec<_>>();
    for violation in config
        .provider_connections
        .violations(&stages, &config.init.embedding)
    {
        match violation {
            ProviderConnectionViolation::MissingReference { connection, usage } => {
                diags.push(ValidationError::ProviderConnectionMissing {
                    field: usage.connection_path(),
                    connection,
                });
            }
            ProviderConnectionViolation::UnsupportedCapability {
                connection,
                provider,
                usage,
            } => diags.push(ValidationError::ProviderConnectionUnsupported {
                field: usage.connection_path(),
                connection,
                provider,
                capability: usage.capability(),
            }),
            ProviderConnectionViolation::MissingCredential {
                connection,
                provider,
            } => diags.push(ValidationError::ProviderConnectionCredentialMissing {
                connection,
                provider,
            }),
            ProviderConnectionViolation::InvalidEndpoint { connection, value } => {
                diags.push(ValidationError::UrlMalformed {
                    field: ConfigPath::child("provider_connections", connection.as_str())
                        .extend("base_url"),
                    value,
                });
            }
            ProviderConnectionViolation::EndpointIncludesRequestPrefix { connection, value } => {
                diags.push(ValidationError::UrlUnsupportedForm {
                    field: ConfigPath::child("provider_connections", connection.as_str())
                        .extend("base_url"),
                    value,
                    requirement: PROVIDER_BASE_URL_REQUIREMENT,
                });
            }
            ProviderConnectionViolation::DuplicateEndpoint {
                first,
                second,
                provider,
                normalised_base_url,
            } => diags.push(ValidationError::DuplicateProviderConnectionEndpoint {
                first,
                second,
                provider,
                normalised_base_url,
            }),
        }
    }

    let genesis = &config.init.embedding;
    if config
        .provider_connections
        .get(genesis.connection.as_str())
        .is_some_and(|connection| connection.provider() == ProviderKind::Platform)
    {
        diags.push(ValidationError::PlatformProviderNotLocal {
            field: ConfigPath::from_static("init.embedding.connection"),
            connection: genesis.connection.clone(),
        });
    }
}

fn validate_provider_limits(config: &TribalConfig, diags: &mut Diagnostics) {
    let task_timeout = config.worker.task_timeout_ms;

    for (provider, limits) in &config.limits.providers {
        let provider_prefix = format!("limits.providers.{provider}");

        if limits.max_in_flight == 0 {
            diags.push(ValidationError::must_be_positive(ConfigPath::child(
                &provider_prefix,
                "max_in_flight",
            )));
        }

        let request_timeout = limits.request_timeout_ms;

        if request_timeout == 0 {
            diags.push(ValidationError::must_be_positive(ConfigPath::child(
                &provider_prefix,
                "request_timeout_ms",
            )));
        } else if request_timeout >= task_timeout {
            diags.push(ValidationError::FieldOrdering {
                subject: FieldValue {
                    field: ConfigPath::child(&provider_prefix, "request_timeout_ms"),
                    value: request_timeout,
                },
                bound: FieldValue {
                    field: ConfigPath::from_static("worker.task_timeout_ms"),
                    value: task_timeout,
                },
                relation: OrderRelation::LessThan,
            });
        }
    }
}

fn validate_discovery(config: &TribalConfig, diags: &mut Diagnostics) {
    if config.discovery.default_limit == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "discovery.default_limit",
        )));
    }

    if config.discovery.max_limit == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "discovery.max_limit",
        )));
    }

    if config.discovery.default_limit > config.discovery.max_limit {
        diags.push(ValidationError::FieldOrdering {
            subject: FieldValue {
                field: ConfigPath::from_static("discovery.default_limit"),
                value: u64::from(config.discovery.default_limit),
            },
            bound: FieldValue {
                field: ConfigPath::from_static("discovery.max_limit"),
                value: u64::from(config.discovery.max_limit),
            },
            relation: OrderRelation::AtMost,
        });
    }

    if config.discovery.overfetch_multiplier == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "discovery.overfetch_multiplier",
        )));
    } else if config.discovery.overfetch_multiplier > MAX_OVERFETCH_MULTIPLIER {
        diags.push(ValidationError::AboveMax {
            field: ConfigPath::from_static("discovery.overfetch_multiplier"),
            value: u64::from(config.discovery.overfetch_multiplier),
            limit: u64::from(MAX_OVERFETCH_MULTIPLIER),
        });
    }

    let threshold = config.discovery.similarity_threshold;
    if !SIMILARITY_RANGE.contains(threshold) {
        diags.push(ValidationError::OutOfRange {
            field: ConfigPath::from_static("discovery.similarity_threshold"),
            value: threshold,
            range: SIMILARITY_RANGE,
        });
    }
}

fn validate_exploration(config: &TribalConfig, diags: &mut Diagnostics) {
    if config.exploration.default_depth == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "exploration.default_depth",
        )));
    }

    if config.exploration.max_depth == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "exploration.max_depth",
        )));
    }

    if config.exploration.default_depth > config.exploration.max_depth {
        diags.push(ValidationError::FieldOrdering {
            subject: FieldValue {
                field: ConfigPath::from_static("exploration.default_depth"),
                value: u64::from(config.exploration.default_depth),
            },
            bound: FieldValue {
                field: ConfigPath::from_static("exploration.max_depth"),
                value: u64::from(config.exploration.max_depth),
            },
            relation: OrderRelation::AtMost,
        });
    }

    if config.exploration.default_limit == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "exploration.default_limit",
        )));
    }

    if config.exploration.max_limit == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "exploration.max_limit",
        )));
    }

    if config.exploration.default_limit > config.exploration.max_limit {
        diags.push(ValidationError::FieldOrdering {
            subject: FieldValue {
                field: ConfigPath::from_static("exploration.default_limit"),
                value: u64::from(config.exploration.default_limit),
            },
            bound: FieldValue {
                field: ConfigPath::from_static("exploration.max_limit"),
                value: u64::from(config.exploration.max_limit),
            },
            relation: OrderRelation::AtMost,
        });
    }
}

fn validate_logging(config: &TribalConfig, diags: &mut Diagnostics) {
    // The subscriber adopts `logging.level` as an `EnvFilter` directive; one
    // that cannot parse is refused here, before it can persist or reload.
    if EnvFilter::try_new(&config.logging.level).is_err() {
        diags.push(ValidationError::LogFilterMalformed {
            value: config.logging.level.clone(),
        });
    }
}

fn validate_telemetry(config: &TribalConfig, diags: &mut Diagnostics) {
    // `file_export` defaults to false, so an operator must explicitly set
    // it.  Requiring `enabled` for file export catches misconfiguration.
    //
    // `console_export` is intentionally not validated here: it defaults to
    // true via serde, so any config setting only `enabled = false` would
    // fail validation without the operator ever mentioning console_export.
    if config.telemetry.file_export && !config.telemetry.enabled {
        diags.push(ValidationError::TelemetryFileExportRequiresEnabled);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::{ProviderConnectionName, ProviderKind};

    use super::*;
    use crate::sections::{
        ClientRegistrationMode, DEFAULT_BIND_ADDRESS, client_registration_mode,
        oauth_onboarding_is_url_only, oauth_surface_is_routable,
    };

    fn valid_config() -> TribalConfig {
        TribalConfig::minimum_valid("postgres://localhost/tribal")
    }

    fn connection_name(value: &str) -> ProviderConnectionName {
        ProviderConnectionName::parse(value).unwrap()
    }

    fn openai(base_url: &str, api_key: Option<&str>) -> crate::ProviderConnectionConfig {
        crate::ProviderConnectionConfig::OpenAi {
            base_url: base_url.to_owned(),
            api_key: api_key.map(|value| value.parse().unwrap()),
        }
    }

    /// Returns the diagnostics from a failed [`validate`] run, panicking
    /// if the run succeeded or produced a non-validation error.
    fn diagnostics_for(config: &TribalConfig) -> Diagnostics {
        match validate(config) {
            Err(ConfigError::ValidationFailed { diagnostics }) => diagnostics,
            other => panic!("expected ValidationFailed, got {other:?}"),
        }
    }

    /// Returns true if `diags` contains a [`ValidationError`] matching
    /// `pred`.
    fn any<P: Fn(&ValidationError) -> bool>(diags: &Diagnostics, pred: P) -> bool {
        diags.iter().any(pred)
    }

    // -- valid -------------------------------------------------------------

    #[test]
    fn test_validate_accepts_valid_config() {
        assert!(validate(&valid_config()).is_ok());
    }

    // -- database ----------------------------------------------------------

    #[test]
    fn test_validate_rejects_empty_database_url() {
        let config = TribalConfig::default();
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::Empty { field } if field.as_str() == "database.url",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_pool_connections() {
        let mut config = valid_config();
        config.database.pool_mcp_max_connections = 0;
        config.database.pool_worker_max_connections = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "database.pool_mcp_max_connections",
        )));
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "database.pool_worker_max_connections",
        )));
    }

    // -- server ------------------------------------------------------------

    #[test]
    fn test_validate_rejects_bind_with_stdio() {
        let mut config = valid_config();
        config.server.transport = TransportKind::Stdio;
        config.server.bind_address = Some(DEFAULT_BIND_ADDRESS.into());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BindAddressStdioConflict,
        )));
    }

    #[test]
    fn test_validate_rejects_invalid_bind_address() {
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("not-an-address".into());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BindAddressMalformed { value }
                if value == "not-an-address",
        )));
    }

    #[test]
    fn test_client_registration_is_derived_from_transport_and_routability() {
        let mut config = valid_config();
        assert_eq!(
            client_registration_mode(&config),
            ClientRegistrationMode::NoNetworkTransport,
        );

        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("127.0.0.1:8725".into());
        assert_eq!(
            client_registration_mode(&config),
            ClientRegistrationMode::Automatic,
        );

        config.server.public_mcp_url = Some("https://tribal.example.com/mcp".into());
        assert_eq!(
            client_registration_mode(&config),
            ClientRegistrationMode::RoutableOauthSurface,
        );
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_oauth_surface_routable_via_public_mcp_url() {
        // A loopback bind with no explicit OAuth URLs is loopback on its
        // own, but a routable advertised URL (the reverse-proxy case)
        // makes the surface routable, the signal that refuses open DCR.
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("127.0.0.1:8725".into());
        config.oauth.issuer_url = None;
        config.oauth.resource_url = None;
        config.server.public_mcp_url = Some("https://tribal.example.com/mcp".into());
        assert!(oauth_surface_is_routable(&config));
        config.server.public_mcp_url = None;
        assert!(
            !oauth_surface_is_routable(&config),
            "without an advertised URL the same config is loopback",
        );
    }

    #[test]
    fn test_oauth_surface_loopback_public_mcp_url_stays_loopback() {
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("127.0.0.1:8725".into());
        config.oauth.issuer_url = None;
        config.oauth.resource_url = None;
        config.server.public_mcp_url = Some("http://127.0.0.1:8725/mcp".into());
        assert!(!oauth_surface_is_routable(&config));
    }

    #[test]
    fn test_validate_rejects_malformed_public_mcp_url() {
        let mut config = valid_config();
        config.server.public_mcp_url = Some("not a url".to_owned());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::UrlMalformed { field, .. }
                if field.as_str() == "server.public_mcp_url",
        )));
    }

    #[test]
    fn test_oauth_surface_routable_on_malformed_public_mcp_url() {
        // Defence-in-depth for non-validating callers: a present-but-
        // unparseable advertised URL classifies as routable so DCR is
        // refused rather than left open on a malformed value.
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("127.0.0.1:8725".into());
        config.oauth.issuer_url = None;
        config.oauth.resource_url = None;
        config.server.public_mcp_url = Some("not a url".into());
        assert!(oauth_surface_is_routable(&config));
    }

    #[test]
    fn test_oauth_surface_wildcard_bind_behind_loopback_port_is_loopback() {
        // The Docker compose shape: bound to 0.0.0.0 inside the container,
        // reached on a loopback host port mapping, with TRIBAL_PUBLIC_MCP_URL
        // set to a loopback advertised URL. That explicit loopback override
        // keeps the surface loopback despite the wildcard bind, so
        // `valid_token_exists` skips rather than warns and DCR stays allowed.
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("0.0.0.0:8725".into());
        config.oauth.issuer_url = None;
        config.oauth.resource_url = None;
        config.server.public_mcp_url = Some("http://127.0.0.1:8725/mcp".into());
        assert!(!oauth_surface_is_routable(&config));
    }

    #[test]
    fn test_oauth_surface_routable_on_hostless_public_mcp_url() {
        // Defence-in-depth for non-validating callers: a parseable but
        // hostless advertised URL (mailto:/file: style) has no loopback
        // guarantee, so it classifies as routable (fail closed) rather than
        // reopening DCR on a wildcard bind. Load-time validation rejects it
        // first; this guards the renderer path that skips validation.
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("0.0.0.0:8725".into());
        config.oauth.issuer_url = None;
        config.oauth.resource_url = None;
        config.server.public_mcp_url = Some("mailto:ops@example.com".into());
        assert!(oauth_surface_is_routable(&config));
    }

    #[test]
    fn test_validate_accepts_wildcard_bind_with_loopback_public_mcp_url() {
        // The validate-side of the Docker shape: a wildcard bind with an
        // explicit loopback advertised URL is the trusted-exposure override,
        // so open DCR is allowed.
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("0.0.0.0:8725".into());
        config.server.public_mcp_url = Some("http://127.0.0.1:8725/mcp".into());
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_onboarding_url_only_on_loopback_network_transport() {
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("127.0.0.1:8725".into());
        assert!(oauth_onboarding_is_url_only(&config));
    }

    #[test]
    fn test_onboarding_not_url_only_without_network_transport() {
        let mut config = valid_config();
        config.server.transport = TransportKind::Stdio;
        assert!(!oauth_onboarding_is_url_only(&config));
    }

    #[test]
    fn test_onboarding_not_url_only_when_routable() {
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.server.public_mcp_url = Some("https://tribal.example.com/mcp".into());
        assert!(!oauth_onboarding_is_url_only(&config));
    }

    #[test]
    fn test_validate_rejects_non_http_public_mcp_url() {
        let mut config = valid_config();
        config.server.public_mcp_url = Some("file:///tmp/mcp".to_owned());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::UrlUnsupportedForm { field, .. }
                if field.as_str() == "server.public_mcp_url",
        )));
    }

    #[test]
    fn test_validate_rejects_public_mcp_url_with_fragment() {
        let mut config = valid_config();
        config.server.public_mcp_url = Some("https://tribal.example.com/mcp#frag".to_owned());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::UrlUnsupportedForm { field, .. }
                if field.as_str() == "server.public_mcp_url",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_shutdown_deadline() {
        let mut config = valid_config();
        config.server.shutdown_deadline_ms = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "server.shutdown_deadline_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_job_state_ttl() {
        let mut config = valid_config();
        config.server.job_state_ttl_seconds = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "server.job_state_ttl_seconds",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_job_state_hard_ttl() {
        let mut config = valid_config();
        config.server.job_state_hard_ttl_seconds = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "server.job_state_hard_ttl_seconds",
        )));
    }

    #[test]
    fn test_validate_rejects_hard_ttl_less_than_terminal_ttl() {
        let mut config = valid_config();
        config.server.job_state_ttl_seconds = 600;
        config.server.job_state_hard_ttl_seconds = 300;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::FieldOrdering {
                subject,
                bound,
                relation: OrderRelation::AtLeast,
            } if subject.field.as_str() == "server.job_state_hard_ttl_seconds"
                && subject.value == 300
                && bound.field.as_str() == "server.job_state_ttl_seconds"
                && bound.value == 600,
        )));
    }

    #[test]
    fn test_validate_accepts_hard_ttl_equal_to_terminal_ttl() {
        let mut config = valid_config();
        config.server.job_state_ttl_seconds = 300;
        config.server.job_state_hard_ttl_seconds = 300;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_zero_sse_keepalive_interval() {
        let mut config = valid_config();
        config.server.sse.keepalive_interval_ms = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "server.sse.keepalive_interval_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_sse_idle_timeout() {
        let mut config = valid_config();
        config.server.sse.idle_timeout_ms = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "server.sse.idle_timeout_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_sse_max_connection_age() {
        let mut config = valid_config();
        config.server.sse.max_connection_age_ms = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "server.sse.max_connection_age_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_keepalive_gte_idle_timeout() {
        let mut config = valid_config();
        config.server.sse.keepalive_interval_ms = 300_000;
        config.server.sse.idle_timeout_ms = 300_000;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::FieldOrdering {
                subject,
                bound,
                relation: OrderRelation::LessThan,
            } if subject.field.as_str() == "server.sse.keepalive_interval_ms"
                && bound.field.as_str() == "server.sse.idle_timeout_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_excessive_max_connection_age() {
        let mut config = valid_config();
        config.server.sse.max_connection_age_ms = MAX_LIFECYCLE_DURATION_MS + 1;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, limit, .. }
                if field.as_str() == "server.sse.max_connection_age_ms"
                    && *limit == MAX_LIFECYCLE_DURATION_MS,
        )));
    }

    #[test]
    fn test_validate_rejects_excessive_idle_timeout() {
        let mut config = valid_config();
        config.server.sse.idle_timeout_ms = MAX_LIFECYCLE_DURATION_MS + 1;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, limit, .. }
                if field.as_str() == "server.sse.idle_timeout_ms"
                    && *limit == MAX_LIFECYCLE_DURATION_MS,
        )));
    }

    #[test]
    fn test_validate_rejects_excessive_keepalive_interval() {
        let mut config = valid_config();
        config.server.sse.keepalive_interval_ms = MAX_LIFECYCLE_DURATION_MS + 1;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, limit, .. }
                if field.as_str() == "server.sse.keepalive_interval_ms"
                    && *limit == MAX_LIFECYCLE_DURATION_MS,
        )));
    }

    // -- auth --------------------------------------------------------------

    #[test]
    fn test_validate_rejects_zero_token_ttl_hours() {
        let mut config = valid_config();
        config.auth.token_ttl_hours = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "auth.token_ttl_hours",
        )));
    }

    #[test]
    fn test_validate_rejects_excessive_token_ttl_hours() {
        let mut config = valid_config();
        config.auth.token_ttl_hours = u64::MAX;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, limit, .. }
                if field.as_str() == "auth.token_ttl_hours"
                    && *limit == MAX_TTL_HOURS,
        )));
    }

    #[test]
    fn test_validate_accepts_token_ttl_at_max() {
        let mut config = valid_config();
        config.auth.token_ttl_hours = MAX_TTL_HOURS;
        assert!(validate(&config).is_ok());
    }

    // -- oauth -------------------------------------------------------------

    #[test]
    fn test_validate_rejects_zero_oauth_access_ttl() {
        let mut config = valid_config();
        config.oauth.access_token_ttl_hours = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "oauth.access_token_ttl_hours",
        )));
    }

    #[test]
    fn test_validate_rejects_excessive_oauth_access_ttl() {
        let mut config = valid_config();
        config.oauth.access_token_ttl_hours = MAX_OAUTH_ACCESS_TOKEN_TTL_HOURS + 1;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, limit, .. }
                if field.as_str() == "oauth.access_token_ttl_hours"
                    && *limit == MAX_OAUTH_ACCESS_TOKEN_TTL_HOURS,
        )));
    }

    #[test]
    fn test_validate_accepts_oauth_access_ttl_at_max() {
        let mut config = valid_config();
        config.oauth.access_token_ttl_hours = MAX_OAUTH_ACCESS_TOKEN_TTL_HOURS;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_oauth_code_ttl_below_min() {
        let mut config = valid_config();
        config.oauth.authorization_code_ttl_seconds = MIN_AUTHORIZATION_CODE_TTL_SECONDS - 1;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min, .. }
                if field.as_str() == "oauth.authorization_code_ttl_seconds"
                    && *min == MIN_AUTHORIZATION_CODE_TTL_SECONDS,
        )));
    }

    #[test]
    fn test_validate_rejects_oauth_code_ttl_above_max() {
        let mut config = valid_config();
        config.oauth.authorization_code_ttl_seconds = MAX_AUTHORIZATION_CODE_TTL_SECONDS + 1;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, limit, .. }
                if field.as_str() == "oauth.authorization_code_ttl_seconds"
                    && *limit == MAX_AUTHORIZATION_CODE_TTL_SECONDS,
        )));
    }

    #[test]
    fn test_validate_accepts_oauth_code_ttl_at_bounds() {
        let mut config = valid_config();
        config.oauth.authorization_code_ttl_seconds = MIN_AUTHORIZATION_CODE_TTL_SECONDS;
        assert!(validate(&config).is_ok());
        config.oauth.authorization_code_ttl_seconds = MAX_AUTHORIZATION_CODE_TTL_SECONDS;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_malformed_oauth_issuer_url() {
        let mut config = valid_config();
        config.oauth.issuer_url = Some("not a url".to_owned());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::UrlMalformed { field, .. }
                if field.as_str() == "oauth.issuer_url",
        )));
    }

    #[test]
    fn test_validate_rejects_malformed_oauth_resource_url() {
        let mut config = valid_config();
        config.oauth.resource_url = Some(":::not-a-url".to_owned());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::UrlMalformed { field, .. }
                if field.as_str() == "oauth.resource_url",
        )));
    }

    #[test]
    fn test_validate_rejects_oauth_issuer_with_path() {
        let mut config = valid_config();
        config.oauth.issuer_url = Some("https://auth.example.com/tribal".to_owned());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::UrlUnsupportedForm { field, .. }
                if field.as_str() == "oauth.issuer_url",
        )));
    }

    #[test]
    fn test_validate_rejects_oauth_resource_with_fragment() {
        let mut config = valid_config();
        config.oauth.resource_url = Some("https://auth.example.com/mcp#frag".to_owned());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::UrlUnsupportedForm { field, .. }
                if field.as_str() == "oauth.resource_url",
        )));
    }

    #[test]
    fn test_validate_accepts_valid_oauth_urls() {
        let mut config = valid_config();
        config.oauth.issuer_url = Some("https://auth.example.com".to_owned());
        config.oauth.resource_url = Some("https://auth.example.com/mcp".to_owned());
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_accepts_unset_oauth_urls() {
        // Unset URLs are admissible: the consumer derives them from the
        // bind address at startup.
        let mut config = valid_config();
        config.oauth.issuer_url = None;
        config.oauth.resource_url = None;
        assert!(validate(&config).is_ok());
    }

    // -- embedding ---------------------------------------------------------

    #[test]
    fn test_validate_rejects_zero_embedding_dimensions() {
        let mut config = valid_config();
        config.init.embedding.dimensions = Some(0);
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "init.embedding.dimensions",
        )));
    }

    #[test]
    fn test_validate_accepts_unset_embedding_dimensions() {
        let mut config = valid_config();
        // `None` resolves through the native-dimension chain at provisioning.
        config.init.embedding.dimensions = None;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_above_max_embedding_dimensions() {
        let mut config = valid_config();
        config.init.embedding.dimensions = Some(MAX_EMBEDDING_DIMENSIONS + 1);
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, value, limit }
                if field.as_str() == "init.embedding.dimensions"
                    && *value == u64::from(MAX_EMBEDDING_DIMENSIONS) + 1
                    && *limit == u64::from(MAX_EMBEDDING_DIMENSIONS),
        )));
    }

    #[test]
    fn test_validate_accepts_max_embedding_dimensions() {
        let mut config = valid_config();
        // The ceiling itself is admissible; only strictly-above is rejected.
        config.init.embedding.dimensions = Some(MAX_EMBEDDING_DIMENSIONS);
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_accepts_well_formed_provider_connection() {
        let mut config = valid_config();
        config.provider_connections.insert(
            connection_name("openai_default"),
            openai("https://api.openai.com", Some("sk-test")),
        );
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_provider_request_prefix_in_base_url() {
        let mut config = valid_config();
        config.provider_connections.insert(
            connection_name("openai_default"),
            openai("https://gateway.example/openai/v1/", Some("sk-test")),
        );
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |diagnostic| matches!(
            diagnostic,
            ValidationError::UrlUnsupportedForm { field, requirement, .. }
                if field.as_str() == "provider_connections.openai_default.base_url"
                    && *requirement == PROVIDER_BASE_URL_REQUIREMENT,
        )));
    }

    #[test]
    fn test_validate_rejects_ollama_request_prefix_in_base_url() {
        let mut config = valid_config();
        config.provider_connections.insert(
            connection_name("ollama_default"),
            crate::ProviderConnectionConfig::Ollama {
                base_url: "http://localhost:11434/api".to_owned(),
            },
        );
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |diagnostic| matches!(
            diagnostic,
            ValidationError::UrlUnsupportedForm { field, requirement, .. }
                if field.as_str() == "provider_connections.ollama_default.base_url"
                    && *requirement == PROVIDER_BASE_URL_REQUIREMENT,
        )));
    }

    #[test]
    fn test_invalid_connection_name_fails_deserialisation() {
        assert!(
            serde_yaml::from_str::<crate::ProviderConnections>(
                "open-ai:\n  provider: ollama\n  base_url: http://localhost:11434\n",
            )
            .is_err()
        );
    }

    #[test]
    fn test_validate_rejects_duplicate_provider_endpoint() {
        let mut config = valid_config();
        config.provider_connections.insert(
            connection_name("ollama_secondary"),
            crate::ProviderConnectionConfig::Ollama {
                base_url: "http://localhost:11434/".to_owned(),
            },
        );
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::DuplicateProviderConnectionEndpoint { .. },
        )));
    }

    #[test]
    fn test_validate_rejects_unparseable_provider_base_url() {
        let mut config = valid_config();
        config.provider_connections.insert(
            connection_name("bad"),
            crate::ProviderConnectionConfig::Ollama {
                base_url: "not a url".to_owned(),
            },
        );
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::UrlMalformed { field, .. }
                if field.as_str() == "provider_connections.bad.base_url",
        )));
    }

    #[test]
    fn test_validate_rejects_anthropic_embedding_connection() {
        let mut config = valid_config();
        config.provider_connections.insert(
            connection_name("anthropic_default"),
            crate::ProviderConnectionConfig::Anthropic {
                base_url: "https://api.anthropic.com".to_owned(),
                api_key: Some("sk-test".parse().unwrap()),
            },
        );
        config.init.embedding.connection = connection_name("anthropic_default");
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::ProviderConnectionUnsupported {
                provider: ProviderKind::Anthropic,
                ..
            },
        )));
    }

    // -- pool sizing -------------------------------------------------------

    #[test]
    fn test_validate_pool_sizing_floor() {
        let mut config = valid_config();
        config.worker.max_concurrent_tasks = 10;
        config.database.pool_worker_max_connections = 10;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::DerivedFloor { field, value, floor }
                if field.as_str() == "database.pool_worker_max_connections"
                    && *value == 10
                    && floor.value == 14
                    && floor.addend.as_str() == "worker.max_concurrent_tasks"
                    && floor.overhead == POOL_CONNECTION_OVERHEAD,
        )));
    }

    // -- provider limits ---------------------------------------------------

    #[test]
    fn test_validate_rejects_zero_max_in_flight() {
        let mut config = valid_config();
        config
            .limits
            .providers
            .get_mut(&ProviderKind::Ollama)
            .unwrap()
            .max_in_flight = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "limits.providers.ollama.max_in_flight",
        )));
    }

    #[test]
    fn test_validate_rejects_request_timeout_exceeding_task_timeout() {
        let mut config = valid_config();
        config.worker.task_timeout_ms = 100_000;
        config
            .limits
            .providers
            .get_mut(&ProviderKind::Ollama)
            .unwrap()
            .request_timeout_ms = 100_000;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::FieldOrdering {
                subject,
                bound,
                relation: OrderRelation::LessThan,
            } if subject.field.as_str() == "limits.providers.ollama.request_timeout_ms"
                && bound.field.as_str() == "worker.task_timeout_ms",
        )));
    }

    // -- provider references ----------------------------------------------

    #[test]
    fn test_validate_rejects_missing_key_for_cloud_connection() {
        let mut config = valid_config();
        config.provider_connections.insert(
            connection_name("openai_default"),
            openai("https://api.openai.com", None),
        );
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::ProviderConnectionCredentialMissing {
                provider: ProviderKind::OpenAi,
                ..
            },
        )));
    }

    #[test]
    fn test_validate_emits_one_missing_reference_per_stage() {
        let mut config = valid_config();
        let missing = connection_name("missing");
        config.inference.extraction.connection = missing.clone();
        config.inference.triage.connection = missing.clone();
        config.inference.relation.connection = missing;

        let diags = diagnostics_for(&config);
        for path in [
            "inference.extraction.connection",
            "inference.triage.connection",
            "inference.relation.connection",
        ] {
            assert!(
                any(&diags, |d| matches!(
                    d,
                    ValidationError::ProviderConnectionMissing { field, .. }
                        if field.as_str() == path,
                )),
                "no missing-reference diagnostic for {path}; diagnostics: {diags:?}",
            );
        }
    }

    #[test]
    fn test_validate_rejects_platform_as_embedding_provider() {
        let mut config = valid_config();
        config.provider_connections.insert(
            connection_name("platform_default"),
            crate::ProviderConnectionConfig::Platform {},
        );
        config.init.embedding.connection = connection_name("platform_default");
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::PlatformProviderNotLocal { field, .. }
                if field.as_str() == "init.embedding.connection",
        )));
    }

    #[test]
    fn test_validate_accepts_platform_inference_connection() {
        let mut config = valid_config();
        config.provider_connections.insert(
            connection_name("platform_default"),
            crate::ProviderConnectionConfig::Platform {},
        );
        config.inference.extraction.connection = connection_name("platform_default");
        assert!(validate(&config).is_ok());
    }

    // -- discovery ---------------------------------------------------------

    #[test]
    fn test_validate_rejects_zero_overfetch_multiplier() {
        let mut config = valid_config();
        config.discovery.overfetch_multiplier = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "discovery.overfetch_multiplier",
        )));
    }

    #[test]
    fn test_validate_rejects_overfetch_multiplier_above_max() {
        let mut config = valid_config();
        config.discovery.overfetch_multiplier = MAX_OVERFETCH_MULTIPLIER + 1;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, limit, .. }
                if field.as_str() == "discovery.overfetch_multiplier"
                    && *limit == u64::from(MAX_OVERFETCH_MULTIPLIER),
        )));
    }

    #[test]
    fn test_validate_accepts_overfetch_multiplier_at_max() {
        let mut config = valid_config();
        config.discovery.overfetch_multiplier = MAX_OVERFETCH_MULTIPLIER;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_zero_similarity_threshold() {
        let mut config = valid_config();
        config.discovery.similarity_threshold = 0.0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::OutOfRange { field, .. }
                if field.as_str() == "discovery.similarity_threshold",
        )));
    }

    #[test]
    fn test_validate_rejects_similarity_threshold_above_one() {
        let mut config = valid_config();
        config.discovery.similarity_threshold = 1.5;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::OutOfRange { field, .. }
                if field.as_str() == "discovery.similarity_threshold",
        )));
    }

    #[test]
    fn test_validate_accepts_similarity_threshold_at_one() {
        let mut config = valid_config();
        config.discovery.similarity_threshold = 1.0;
        assert!(validate(&config).is_ok());
    }

    // -- inference ---------------------------------------------------------

    #[test]
    fn test_validate_accepts_unset_inference_sampling() {
        // The default config leaves temperature and max_tokens unset.
        assert!(validate(&valid_config()).is_ok());
    }

    #[test]
    fn test_validate_rejects_temperature_above_range() {
        let mut config = valid_config();
        config.inference.extraction.temperature = Some(2.5);
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::OutOfRange { field, .. }
                if field.as_str() == "inference.extraction.temperature",
        )));
    }

    #[test]
    fn test_validate_rejects_negative_temperature() {
        let mut config = valid_config();
        config.inference.triage.temperature = Some(-0.1);
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::OutOfRange { field, .. }
                if field.as_str() == "inference.triage.temperature",
        )));
    }

    #[test]
    fn test_validate_accepts_temperature_at_bounds() {
        let mut config = valid_config();
        config.inference.extraction.temperature = Some(0.0);
        config.inference.triage.temperature = Some(2.0);
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_zero_max_tokens() {
        let mut config = valid_config();
        config.inference.relation.max_tokens = Some(0);
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "inference.relation.max_tokens",
        )));
    }

    #[test]
    fn test_validate_rejects_empty_model() {
        let mut config = valid_config();
        config.inference.extraction.model = String::new();
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::Empty { field } if field.as_str() == "inference.extraction.model",
        )));
    }

    #[test]
    fn test_validate_rejects_whitespace_model() {
        let mut config = valid_config();
        config.inference.triage.model = "gpt 4o".to_owned();
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::ContainsWhitespace { field }
                if field.as_str() == "inference.triage.model",
        )));
    }

    #[test]
    fn test_validate_rejects_empty_embedding_model() {
        let mut config = valid_config();
        config.init.embedding.model = String::new();
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::Empty { field } if field.as_str() == "init.embedding.model",
        )));
    }

    // -- logging -----------------------------------------------------------

    #[test]
    fn test_validate_rejects_an_unparseable_log_filter_directive() {
        let mut config = valid_config();
        config.logging.level = "not valid [[".to_owned();
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::LogFilterMalformed { value } if value == "not valid [[",
        )));
    }

    #[test]
    fn test_validate_accepts_a_per_target_filter_directive() {
        let mut config = valid_config();
        config.logging.level = "info,tribal_db=debug".to_owned();
        assert!(validate(&config).is_ok());
    }

    // -- telemetry ---------------------------------------------------------

    #[test]
    fn test_validate_rejects_file_export_without_enabled() {
        let mut config = valid_config();
        config.telemetry.file_export = true;
        config.telemetry.enabled = false;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::TelemetryFileExportRequiresEnabled,
        )));
    }

    #[test]
    fn test_validate_accepts_file_export_with_enabled() {
        let mut config = valid_config();
        config.telemetry.file_export = true;
        config.telemetry.enabled = true;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_accepts_console_export_without_enabled() {
        let mut config = valid_config();
        config.telemetry.console_export = true;
        config.telemetry.enabled = false;
        assert!(
            validate(&config).is_ok(),
            "console_export defaults to true — should not reject when enabled is false",
        );
    }

    // -- aggregation -------------------------------------------------------

    #[test]
    fn test_validate_collects_multiple_errors() {
        let mut config = TribalConfig::default();
        config.database.pool_mcp_max_connections = 0;
        config.discovery.overfetch_multiplier = 0;
        let diags = diagnostics_for(&config);
        assert!(
            diags.len() >= 3,
            "expected at least 3 diagnostics, got {}: {diags:?}",
            diags.len(),
        );
    }
}
