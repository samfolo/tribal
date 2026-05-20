//! Typed validation errors and supporting types.
//!
//! Variants split into structural classes (positivity, bounds,
//! ordering, ranges, malformed values) and semantic invariants whose
//! identity drives downstream classification (api-key presence,
//! transport conflict, telemetry coupling).  Producer pushes typed
//! variants into a [`Diagnostics`](super::Diagnostics) collector;
//! consumers match exhaustively, no string-prefix dispatch.
//!
//! [`ConfigPath`] is the operator-visible identity of a config field.
//! Each section type implements [`EnumerateFields`] in its own file,
//! contributing its leaf paths under a parent-supplied prefix.
//! [`TribalConfig::enumerate`](crate::sections::TribalConfig) walks
//! the whole tree depth-first.

use std::{borrow::Cow, fmt};

use crate::{
    env::{ENV_NESTED_SEPARATOR, ENV_PREFIX},
    sections::{ProviderKind, TribalConfig},
};

// ---------------------------------------------------------------------------
// ConfigPath
// ---------------------------------------------------------------------------

/// Dot-separated YAML config path (e.g. `embedding.api_key`).
///
/// Wraps `Cow<'static, str>` so both literal static paths (from
/// validator call sites and the [`ApiKeyStage`] accessors) and
/// runtime-composed paths (from [`EnumerateFields`] recursion) share
/// one type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfigPath {
    path: Cow<'static, str>,
}

impl ConfigPath {
    /// Wraps a static path literal.  Used by validator call sites
    /// where the path is known at compile time, and by
    /// [`ApiKeyStage`]'s const path accessors.
    #[must_use]
    pub const fn from_static(path: &'static str) -> Self {
        Self {
            path: Cow::Borrowed(path),
        }
    }

    /// Composes a path by joining `prefix` and `field` with a `.`.
    ///
    /// Used inside [`EnumerateFields`] impls when the prefix is
    /// supplied by the parent section at runtime.
    #[must_use]
    pub fn child(prefix: &str, field: &str) -> Self {
        Self {
            path: Cow::Owned(format!("{prefix}.{field}")),
        }
    }

    /// Returns the underlying dot-separated path.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.path
    }

    /// Returns the `TRIBAL_*` env var name a figment loader would
    /// honour for this path.
    ///
    /// `embedding.api_key` → `TRIBAL_EMBEDDING__API_KEY`.  Single
    /// source of truth for the dot-to-`__` + uppercase transform;
    /// the free-function [`env_var_for_path`](crate::env::env_var_for_path)
    /// delegates here.
    #[must_use]
    pub fn env_var(&self) -> String {
        format!(
            "{ENV_PREFIX}{}",
            self.path.to_uppercase().replace('.', ENV_NESTED_SEPARATOR),
        )
    }

    /// Returns every `ConfigPath` reachable from [`TribalConfig`], in
    /// depth-first declaration order.
    ///
    /// Dynamic paths under HashMap-keyed parents
    /// (`limits.providers.<provider>.<field>`) are not included; the
    /// validator constructs those at the call site.
    #[must_use]
    pub fn all() -> Vec<Self> {
        let mut out = Vec::new();
        TribalConfig::enumerate("", &mut out);
        out
    }
}

impl fmt::Display for ConfigPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.path)
    }
}

// ---------------------------------------------------------------------------
// EnumerateFields trait
// ---------------------------------------------------------------------------

/// Depth-first enumeration of every leaf [`ConfigPath`] under a config
/// section.
///
/// Implementations are hand-written and colocated with each section's
/// struct definition.  A section's `enumerate` either pushes its own
/// leaf fields (`ConfigPath::child(prefix, "field")`) or delegates to
/// child sections by composing the prefix (`Sub::enumerate(&format!
/// ("{prefix}.sub"), out)`).
///
/// The same impl serves every instance of a shared section type — the
/// prefix differs per call site, so e.g. `StageInferenceConfig` has
/// one impl that `InferenceConfig` calls three times with prefixes
/// `inference.extraction`, `inference.triage`, `inference.relation`.
///
/// Drift discipline: each impl pairs with a `#[cfg(test)]` check
/// function in the same file that exercises every relative field
/// access used in the impl's path strings.  Renaming a struct field
/// without updating the impl + check fails the test build.
pub trait EnumerateFields {
    /// Pushes every leaf [`ConfigPath`] under this section into `out`,
    /// joining each leaf's field name to `prefix` with a `.`.
    fn enumerate(prefix: &str, out: &mut Vec<ConfigPath>);
}

// ---------------------------------------------------------------------------
// Supporting structures
// ---------------------------------------------------------------------------

/// A config field paired with its offending value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldValue {
    pub field: ConfigPath,
    pub value: u64,
}

/// Ordering relation between two fields in
/// [`ValidationError::FieldOrdering`].  The subject is the field whose
/// invariant was violated; the bound is the other side of the
/// comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderRelation {
    /// subject ≤ bound — rendered as "at most".
    AtMost,
    /// subject < bound — rendered as "less than".
    LessThan,
    /// subject ≥ bound — rendered as "at least".
    AtLeast,
}

/// One endpoint of a [`NumericRange`].  Independent inclusion lets the
/// range express all four open/closed combinations.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Endpoint {
    pub value: f64,
    pub inclusion: Inclusion,
}

impl Endpoint {
    /// An open (exclusive) endpoint.
    #[must_use]
    pub const fn open(value: f64) -> Self {
        Self {
            value,
            inclusion: Inclusion::Open,
        }
    }

    /// A closed (inclusive) endpoint.
    #[must_use]
    pub const fn closed(value: f64) -> Self {
        Self {
            value,
            inclusion: Inclusion::Closed,
        }
    }
}

/// Whether an [`Endpoint`] includes its boundary value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inclusion {
    Open,
    Closed,
}

/// A numeric range for [`ValidationError::OutOfRange`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NumericRange {
    pub low: Endpoint,
    pub high: Endpoint,
}

impl fmt::Display for NumericRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let left = match self.low.inclusion {
            Inclusion::Open => '(',
            Inclusion::Closed => '[',
        };
        let right = match self.high.inclusion {
            Inclusion::Open => ')',
            Inclusion::Closed => ']',
        };
        write!(
            f,
            "{left}{:.1}, {:.1}{right}",
            self.low.value, self.high.value,
        )
    }
}

/// A floor computed as `addend.value + overhead`, referenced in
/// [`ValidationError::DerivedFloor`].  Operator-visible: the addend is
/// a config path; the overhead is the rendered integer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputedFloor {
    pub value: u64,
    pub addend: FieldValue,
    pub overhead: u64,
}

/// Which inference stage the missing-api-key invariant fired for.
///
/// Four variants enumerate every api-key-bearing config slot:
/// embedding plus the three inference stages.  Each carries
/// accessors that return the canonical [`ConfigPath`] for the
/// stage's api-key and provider fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyStage {
    Embedding,
    Extraction,
    Triage,
    Relation,
}

impl ApiKeyStage {
    /// Config path of the api-key field for this stage.
    #[must_use]
    pub fn api_key_path(self) -> ConfigPath {
        let literal = match self {
            Self::Embedding => "embedding.api_key",
            Self::Extraction => "inference.extraction.api_key",
            Self::Triage => "inference.triage.api_key",
            Self::Relation => "inference.relation.api_key",
        };
        ConfigPath::from_static(literal)
    }

    /// Config path of the provider field for this stage.
    #[must_use]
    pub fn provider_path(self) -> ConfigPath {
        let literal = match self {
            Self::Embedding => "embedding.provider",
            Self::Extraction => "inference.extraction.provider",
            Self::Triage => "inference.triage.provider",
            Self::Relation => "inference.relation.provider",
        };
        ConfigPath::from_static(literal)
    }
}

// ---------------------------------------------------------------------------
// ValidationError
// ---------------------------------------------------------------------------

/// A single configuration-invariant violation.
///
/// Structural variants describe value-shape failures.  Semantic
/// variants describe invariant identities that downstream consumers
/// classify on.
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    // -- Structural -----------------------------------------------------
    /// Field must not be empty (e.g. `database.url`).
    Empty { field: ConfigPath },
    /// Field's integer value is below the minimum.  `min == 1` renders
    /// as "must be greater than zero".
    BelowMin {
        field: ConfigPath,
        value: u64,
        min: u64,
    },
    /// Field's integer value exceeds a system limit.
    AboveMax {
        field: ConfigPath,
        value: u64,
        limit: u64,
    },
    /// Field's float value is outside the permitted range.
    OutOfRange {
        field: ConfigPath,
        value: f64,
        range: NumericRange,
    },
    /// Two fields are in the wrong order.  `subject` is the field
    /// whose invariant was violated; `bound` is the other side.
    FieldOrdering {
        subject: FieldValue,
        bound: FieldValue,
        relation: OrderRelation,
    },
    /// Field's value is below a floor derived from another field plus
    /// a constant overhead.
    DerivedFloor {
        field: ConfigPath,
        value: u64,
        floor: ComputedFloor,
    },
    /// Field's value failed to parse as `expected`; the offending
    /// string is preserved.
    Malformed {
        field: ConfigPath,
        value: String,
        expected: &'static str,
    },

    // -- Semantic -------------------------------------------------------
    /// Cloud-provider stage is missing its api-key.
    MissingApiKey {
        stage: ApiKeyStage,
        provider: ProviderKind,
    },
    /// `server.bind_address` is set while `server.transport` is stdio.
    BindAddressStdioConflict,
    /// `server.bind_address` failed to parse as `<host>:<port>`.
    BindAddressMalformed { value: String },
    /// `embedding.provider` is a provider that does not support
    /// embedding.  Renders the provider name in both clauses for clarity.
    EmbeddingProviderUnsupported { provider: ProviderKind },
    /// `telemetry.file_export` requires `telemetry.enabled = true`.
    TelemetryFileExportRequiresEnabled,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // -- Structural -------------------------------------------------
            Self::Empty { field } => write!(f, "{field} must not be empty"),

            Self::BelowMin { field, min, .. } if *min == 1 => {
                write!(f, "{field} must be greater than zero")
            }
            Self::BelowMin { field, value, min } => {
                write!(f, "{field} ({value}) must be at least {min}")
            }

            Self::AboveMax {
                field,
                value,
                limit,
            } => write!(f, "{field} ({value}) must be at most {limit}"),

            Self::OutOfRange {
                field,
                value,
                range,
            } => write!(f, "{field} ({value}) must be in {range}"),

            Self::FieldOrdering {
                subject,
                bound,
                relation,
            } => {
                let phrase = match relation {
                    OrderRelation::AtMost => "at most",
                    OrderRelation::LessThan => "less than",
                    OrderRelation::AtLeast => "at least",
                };
                write!(
                    f,
                    "{} ({}) must be {phrase} {} ({})",
                    subject.field, subject.value, bound.field, bound.value,
                )
            }

            Self::DerivedFloor {
                field,
                value,
                floor,
            } => write!(
                f,
                "{field} ({value}) must be at least {} ({} + {})",
                floor.value, floor.addend.field, floor.overhead,
            ),

            Self::Malformed {
                field,
                value,
                expected,
            } => write!(f, "{field} is not a valid {expected}: {value}"),

            // -- Semantic ---------------------------------------------------
            Self::MissingApiKey { stage, provider } => write!(
                f,
                "{} is required when {} is {provider}",
                stage.api_key_path(),
                stage.provider_path(),
            ),

            Self::BindAddressStdioConflict => {
                f.write_str("server.bind_address cannot be set when server.transport is stdio")
            }

            Self::BindAddressMalformed { value } => write!(
                f,
                "server.bind_address is not a valid socket address: {value}"
            ),

            Self::EmbeddingProviderUnsupported { provider } => write!(
                f,
                "embedding.provider cannot be {provider}: \
                 {provider} does not provide an embedding API",
            ),

            Self::TelemetryFileExportRequiresEnabled => {
                f.write_str("telemetry.file_export requires telemetry.enabled to be true")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- ConfigPath ---------------------------------------------------------

    #[test]
    fn test_config_path_from_static_renders_inner_path() {
        let p = ConfigPath::from_static("database.url");
        assert_eq!(p.to_string(), "database.url");
        assert_eq!(p.as_str(), "database.url");
    }

    #[test]
    fn test_config_path_child_joins_prefix_and_field() {
        let p = ConfigPath::child("database", "url");
        assert_eq!(p.as_str(), "database.url");
    }

    #[test]
    fn test_config_path_child_supports_multi_segment_prefix() {
        let p = ConfigPath::child("inference.extraction", "api_key");
        assert_eq!(p.as_str(), "inference.extraction.api_key");
    }

    #[test]
    fn test_config_path_env_var_uppercases_and_substitutes_separator() {
        assert_eq!(
            ConfigPath::from_static("database.url").env_var(),
            "TRIBAL_DATABASE__URL",
        );
        assert_eq!(
            ConfigPath::child("inference.triage", "provider").env_var(),
            "TRIBAL_INFERENCE__TRIAGE__PROVIDER",
        );
    }

    #[test]
    fn test_config_path_all_walks_every_section() {
        let all = ConfigPath::all();
        assert!(!all.is_empty(), "all() must return some paths");
        // Representative sampling — exhaustive coverage is each
        // section's own concern via its EnumerateFields impl + check fn.
        let strs: Vec<String> = all.iter().map(|p| p.as_str().to_string()).collect();
        assert!(strs.iter().any(|p| p == "database.url"));
        assert!(strs.iter().any(|p| p == "embedding.api_key"));
        assert!(strs.iter().any(|p| p == "inference.triage.provider"));
        assert!(strs.iter().any(|p| p == "server.sse.idle_timeout_ms"));
        assert!(strs.iter().any(|p| p == "telemetry.file_export"));
    }

    // -- ApiKeyStage --------------------------------------------------------

    #[test]
    fn test_api_key_stage_api_key_paths() {
        assert_eq!(
            ApiKeyStage::Embedding.api_key_path().as_str(),
            "embedding.api_key",
        );
        assert_eq!(
            ApiKeyStage::Extraction.api_key_path().as_str(),
            "inference.extraction.api_key",
        );
        assert_eq!(
            ApiKeyStage::Triage.api_key_path().as_str(),
            "inference.triage.api_key",
        );
        assert_eq!(
            ApiKeyStage::Relation.api_key_path().as_str(),
            "inference.relation.api_key",
        );
    }

    #[test]
    fn test_api_key_stage_provider_paths() {
        assert_eq!(
            ApiKeyStage::Embedding.provider_path().as_str(),
            "embedding.provider",
        );
        assert_eq!(
            ApiKeyStage::Extraction.provider_path().as_str(),
            "inference.extraction.provider",
        );
        assert_eq!(
            ApiKeyStage::Triage.provider_path().as_str(),
            "inference.triage.provider",
        );
        assert_eq!(
            ApiKeyStage::Relation.provider_path().as_str(),
            "inference.relation.provider",
        );
    }

    // -- NumericRange Display ----------------------------------------------

    #[test]
    fn test_numeric_range_open_closed_renders_paren_bracket() {
        let r = NumericRange {
            low: Endpoint::open(0.0),
            high: Endpoint::closed(1.0),
        };
        assert_eq!(r.to_string(), "(0.0, 1.0]");
    }

    #[test]
    fn test_numeric_range_closed_open_renders_bracket_paren() {
        let r = NumericRange {
            low: Endpoint::closed(0.0),
            high: Endpoint::open(1.0),
        };
        assert_eq!(r.to_string(), "[0.0, 1.0)");
    }

    #[test]
    fn test_numeric_range_open_open_renders_paren_paren() {
        let r = NumericRange {
            low: Endpoint::open(0.0),
            high: Endpoint::open(1.0),
        };
        assert_eq!(r.to_string(), "(0.0, 1.0)");
    }

    #[test]
    fn test_numeric_range_closed_closed_renders_bracket_bracket() {
        let r = NumericRange {
            low: Endpoint::closed(0.0),
            high: Endpoint::closed(1.0),
        };
        assert_eq!(r.to_string(), "[0.0, 1.0]");
    }

    // -- ValidationError Display: structural --------------------------------

    #[test]
    fn test_display_empty() {
        let err = ValidationError::Empty {
            field: ConfigPath::from_static("database.url"),
        };
        assert_eq!(err.to_string(), "database.url must not be empty");
    }

    #[test]
    fn test_display_below_min_one_renders_as_positivity() {
        let err = ValidationError::BelowMin {
            field: ConfigPath::from_static("embedding.dimensions"),
            value: 0,
            min: 1,
        };
        assert_eq!(
            err.to_string(),
            "embedding.dimensions must be greater than zero",
        );
    }

    #[test]
    fn test_display_below_min_higher_threshold_includes_value_and_min() {
        let err = ValidationError::BelowMin {
            field: ConfigPath::from_static("some.field"),
            value: 1,
            min: 5,
        };
        assert_eq!(err.to_string(), "some.field (1) must be at least 5");
    }

    #[test]
    fn test_display_above_max_renders_value_and_limit_only() {
        let err = ValidationError::AboveMax {
            field: ConfigPath::from_static("server.sse.idle_timeout_ms"),
            value: 300_001,
            limit: 300_000,
        };
        assert_eq!(
            err.to_string(),
            "server.sse.idle_timeout_ms (300001) must be at most 300000",
        );
    }

    #[test]
    fn test_display_out_of_range_open_closed() {
        let err = ValidationError::OutOfRange {
            field: ConfigPath::from_static("discovery.similarity_threshold"),
            value: 1.5,
            range: NumericRange {
                low: Endpoint::open(0.0),
                high: Endpoint::closed(1.0),
            },
        };
        assert_eq!(
            err.to_string(),
            "discovery.similarity_threshold (1.5) must be in (0.0, 1.0]",
        );
    }

    #[test]
    fn test_display_field_ordering_at_most() {
        let err = ValidationError::FieldOrdering {
            subject: FieldValue {
                field: ConfigPath::from_static("discovery.default_limit"),
                value: 20,
            },
            bound: FieldValue {
                field: ConfigPath::from_static("discovery.max_limit"),
                value: 10,
            },
            relation: OrderRelation::AtMost,
        };
        assert_eq!(
            err.to_string(),
            "discovery.default_limit (20) must be at most discovery.max_limit (10)",
        );
    }

    #[test]
    fn test_display_field_ordering_less_than() {
        let err = ValidationError::FieldOrdering {
            subject: FieldValue {
                field: ConfigPath::from_static("server.sse.keepalive_interval_ms"),
                value: 30_000,
            },
            bound: FieldValue {
                field: ConfigPath::from_static("server.sse.idle_timeout_ms"),
                value: 30_000,
            },
            relation: OrderRelation::LessThan,
        };
        assert_eq!(
            err.to_string(),
            "server.sse.keepalive_interval_ms (30000) must be less than \
             server.sse.idle_timeout_ms (30000)",
        );
    }

    #[test]
    fn test_display_field_ordering_at_least() {
        let err = ValidationError::FieldOrdering {
            subject: FieldValue {
                field: ConfigPath::from_static("server.job_state_hard_ttl_seconds"),
                value: 300,
            },
            bound: FieldValue {
                field: ConfigPath::from_static("server.job_state_ttl_seconds"),
                value: 600,
            },
            relation: OrderRelation::AtLeast,
        };
        assert_eq!(
            err.to_string(),
            "server.job_state_hard_ttl_seconds (300) must be at least \
             server.job_state_ttl_seconds (600)",
        );
    }

    #[test]
    fn test_display_derived_floor() {
        let err = ValidationError::DerivedFloor {
            field: ConfigPath::from_static("database.pool_worker_max_connections"),
            value: 5,
            floor: ComputedFloor {
                value: 12,
                addend: FieldValue {
                    field: ConfigPath::from_static("worker.max_concurrent_tasks"),
                    value: 8,
                },
                overhead: 4,
            },
        };
        assert_eq!(
            err.to_string(),
            "database.pool_worker_max_connections (5) must be at least 12 \
             (worker.max_concurrent_tasks + 4)",
        );
    }

    #[test]
    fn test_display_malformed() {
        let err = ValidationError::Malformed {
            field: ConfigPath::from_static("server.bind_address"),
            value: "not-an-address".into(),
            expected: "socket address",
        };
        assert_eq!(
            err.to_string(),
            "server.bind_address is not a valid socket address: not-an-address",
        );
    }

    // -- ValidationError Display: semantic ----------------------------------

    #[test]
    fn test_display_missing_api_key_per_stage() {
        for (stage, expected) in [
            (
                ApiKeyStage::Embedding,
                "embedding.api_key is required when embedding.provider is openai",
            ),
            (
                ApiKeyStage::Extraction,
                "inference.extraction.api_key is required when \
                 inference.extraction.provider is openai",
            ),
            (
                ApiKeyStage::Triage,
                "inference.triage.api_key is required when \
                 inference.triage.provider is openai",
            ),
            (
                ApiKeyStage::Relation,
                "inference.relation.api_key is required when \
                 inference.relation.provider is openai",
            ),
        ] {
            let err = ValidationError::MissingApiKey {
                stage,
                provider: ProviderKind::OpenAi,
            };
            assert_eq!(err.to_string(), expected);
        }
    }

    #[test]
    fn test_display_bind_address_stdio_conflict() {
        let err = ValidationError::BindAddressStdioConflict;
        assert_eq!(
            err.to_string(),
            "server.bind_address cannot be set when server.transport is stdio",
        );
    }

    #[test]
    fn test_display_bind_address_malformed() {
        let err = ValidationError::BindAddressMalformed {
            value: "not-an-address".into(),
        };
        assert_eq!(
            err.to_string(),
            "server.bind_address is not a valid socket address: not-an-address",
        );
    }

    #[test]
    fn test_display_embedding_provider_unsupported() {
        let err = ValidationError::EmbeddingProviderUnsupported {
            provider: ProviderKind::Anthropic,
        };
        assert_eq!(
            err.to_string(),
            "embedding.provider cannot be anthropic: \
             anthropic does not provide an embedding API",
        );
    }

    #[test]
    fn test_display_telemetry_file_export_requires_enabled() {
        let err = ValidationError::TelemetryFileExportRequiresEnabled;
        assert_eq!(
            err.to_string(),
            "telemetry.file_export requires telemetry.enabled to be true",
        );
    }
}
