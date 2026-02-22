//! Provider registry for concurrency bounding and per-provider HTTP clients.
//!
//! [`ProviderRegistry`] maps provider targets (identified by
//! [`ProviderKey`]) to concurrency semaphores and dedicated HTTP clients.
//! Each key combines a provider kind, a normalised base URL, and a
//! [`RequestClass`] so that embedding and inference traffic to the same
//! provider are bounded independently.
//!
//! The registry is eagerly constructed and immutable after construction.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use url::Url;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// User-Agent header value sent on all registry-constructed HTTP clients.
const USER_AGENT: &str = concat!("tribal/", env!("CARGO_PKG_VERSION"));

// ---------------------------------------------------------------------------
// RequestClass
// ---------------------------------------------------------------------------

/// Distinguishes embedding traffic from inference traffic for
/// per-provider concurrency bounding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestClass {
    /// Embedding generation requests.
    Embedding,
    /// LLM completion (inference) requests.
    Inference,
}

// ---------------------------------------------------------------------------
// ProviderKey
// ---------------------------------------------------------------------------

/// Compound key identifying a unique provider + request class combination.
///
/// Two keys are equal when all three components match.  The
/// `normalised_base_url` is produced by [`normalise_registry_url`] at
/// construction time, ensuring trailing slashes, default ports, case
/// differences, and query strings do not create spurious distinct keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProviderKey {
    /// The provider kind (e.g. `"anthropic"`, `"openai"`, `"ollama"`).
    provider_kind: String,
    /// The normalised base URL for the provider endpoint.
    normalised_base_url: String,
    /// Whether this key represents embedding or inference traffic.
    request_class: RequestClass,
}

impl ProviderKey {
    /// Creates a new provider key, normalising the base URL.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderRegistryError::UnparseableUrl`] if `base_url`
    /// cannot be parsed as a valid URL.
    pub fn new(
        provider_kind: impl Into<String>,
        base_url: &str,
        request_class: RequestClass,
    ) -> Result<Self, ProviderRegistryError> {
        let normalised = normalise_registry_url(base_url)?;
        Ok(Self {
            provider_kind: provider_kind.into(),
            normalised_base_url: normalised,
            request_class,
        })
    }

    /// Returns the provider kind.
    #[must_use]
    pub fn provider_kind(&self) -> &str {
        &self.provider_kind
    }

    /// Returns the normalised base URL.
    #[must_use]
    pub fn normalised_base_url(&self) -> &str {
        &self.normalised_base_url
    }

    /// Returns the request class.
    #[must_use]
    pub fn request_class(&self) -> RequestClass {
        self.request_class
    }
}

// ---------------------------------------------------------------------------
// ProviderLimits
// ---------------------------------------------------------------------------

/// Concurrency and timeout configuration for a single provider key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderLimits {
    /// Maximum number of in-flight requests for this provider key.
    pub max_in_flight: u32,
    /// Per-request timeout applied to the HTTP client.
    pub request_timeout: Duration,
}

// ---------------------------------------------------------------------------
// ProviderRegistryError
// ---------------------------------------------------------------------------

/// Errors that can occur when constructing a [`ProviderRegistry`].
#[derive(Debug, thiserror::Error)]
pub enum ProviderRegistryError {
    /// The base URL could not be parsed.
    #[error("unparseable URL {url:?}: {reason}")]
    UnparseableUrl {
        /// The URL string that failed to parse.
        url: String,
        /// The parse error description.
        reason: String,
    },

    /// `max_in_flight` was zero.
    #[error("max_in_flight must be greater than zero for key {key:?}")]
    ZeroMaxInFlight {
        /// The key that had zero `max_in_flight`.
        key: ProviderKey,
    },

    /// `request_timeout` was zero.
    #[error("request_timeout must be greater than zero for key {key:?}")]
    ZeroTimeout {
        /// The key that had zero timeout.
        key: ProviderKey,
    },

    /// A duplicate key was registered.
    #[error("duplicate provider key: {key:?}")]
    DuplicateKey {
        /// The key that was already registered.
        key: ProviderKey,
    },
}

// ---------------------------------------------------------------------------
// ProviderRegistry
// ---------------------------------------------------------------------------

/// Maps provider targets to concurrency semaphores and dedicated HTTP
/// clients.
///
/// Eagerly constructed and immutable after construction.  Each
/// [`ProviderKey`] maps to:
///
/// - An [`Arc<Semaphore>`] with permits equal to `max_in_flight`
/// - A [`reqwest::Client`] configured with `pool_max_idle_per_host`
///   and `timeout` matching the provider's limits
///
/// # Construction
///
/// Use [`ProviderRegistry::new`] with an iterator of
/// `(ProviderKey, ProviderLimits)` pairs.  Construction validates all
/// entries and returns an error on the first invalid entry.
pub struct ProviderRegistry {
    semaphores: HashMap<ProviderKey, Arc<Semaphore>>,
    clients: HashMap<ProviderKey, reqwest::Client>,
}

impl ProviderRegistry {
    /// Creates a new registry from the given entries.
    ///
    /// Each entry produces an [`Arc<Semaphore>`] with
    /// `max_in_flight` permits and a [`reqwest::Client`] with
    /// `pool_max_idle_per_host` and `timeout` set to match.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderRegistryError::ZeroMaxInFlight`] if any entry
    /// has `max_in_flight == 0`.
    ///
    /// Returns [`ProviderRegistryError::ZeroTimeout`] if any entry has
    /// a zero `request_timeout`.
    ///
    /// Returns [`ProviderRegistryError::DuplicateKey`] if a key appears
    /// more than once.
    ///
    /// # Panics
    ///
    /// Panics if `reqwest::Client::builder().build()` fails.  This
    /// should not occur with the configuration used here (rustls
    /// backend, valid timeout and pool settings).
    pub fn new(
        entries: impl IntoIterator<Item = (ProviderKey, ProviderLimits)>,
    ) -> Result<Self, ProviderRegistryError> {
        let entries: Vec<_> = entries.into_iter().collect();
        let mut semaphores = HashMap::with_capacity(entries.len());
        let mut clients = HashMap::with_capacity(entries.len());

        for (key, limits) in entries {
            if limits.max_in_flight == 0 {
                return Err(ProviderRegistryError::ZeroMaxInFlight { key });
            }
            if limits.request_timeout.is_zero() {
                return Err(ProviderRegistryError::ZeroTimeout { key });
            }
            if semaphores.contains_key(&key) {
                return Err(ProviderRegistryError::DuplicateKey { key });
            }

            let pool_size = usize::try_from(limits.max_in_flight)
                .expect("u32 always fits in usize");

            let semaphore = Arc::new(Semaphore::new(pool_size));

            let client = reqwest::Client::builder()
                .pool_max_idle_per_host(pool_size)
                .timeout(limits.request_timeout)
                .user_agent(USER_AGENT)
                .build()
                .expect("reqwest client builder with valid configuration");

            semaphores.insert(key.clone(), semaphore);
            clients.insert(key, client);
        }

        Ok(Self {
            semaphores,
            clients,
        })
    }

    /// Returns the semaphore for the given key, if registered.
    #[must_use]
    pub fn semaphore(&self, key: &ProviderKey) -> Option<&Arc<Semaphore>> {
        self.semaphores.get(key)
    }

    /// Returns the HTTP client for the given key, if registered.
    #[must_use]
    pub fn client(&self, key: &ProviderKey) -> Option<&reqwest::Client> {
        self.clients.get(key)
    }
}

// ---------------------------------------------------------------------------
// URL normalisation
// ---------------------------------------------------------------------------

/// Normalises a base URL for use as a registry key component.
///
/// Steps:
/// 1. Parse with [`url::Url`]
/// 2. Lower-case scheme and host (handled by the `url` crate)
/// 3. Strip trailing slash from path
/// 4. Include explicit port (default 80 for HTTP, 443 for HTTPS)
/// 5. Retain path component
/// 6. Drop query string and fragment
fn normalise_registry_url(raw: &str) -> Result<String, ProviderRegistryError> {
    let parsed = Url::parse(raw).map_err(|e| ProviderRegistryError::UnparseableUrl {
        url: raw.to_owned(),
        reason: e.to_string(),
    })?;

    let scheme = parsed.scheme();

    let host = parsed
        .host_str()
        .ok_or_else(|| ProviderRegistryError::UnparseableUrl {
            url: raw.to_owned(),
            reason: "missing host".to_owned(),
        })?;

    let port = parsed.port_or_known_default().ok_or_else(|| {
        ProviderRegistryError::UnparseableUrl {
            url: raw.to_owned(),
            reason: "unknown port for scheme".to_owned(),
        }
    })?;

    let path = parsed.path().trim_end_matches('/');

    Ok(format!("{scheme}://{host}:{port}{path}"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- URL normalisation ---------------------------------------------------

    #[test]
    fn test_normalise_strips_trailing_slash() {
        let key = ProviderKey::new("ollama", "http://localhost:11434/", RequestClass::Embedding)
            .unwrap();
        assert_eq!(key.normalised_base_url(), "http://localhost:11434");
    }

    #[test]
    fn test_normalise_lowercases_scheme_and_host() {
        let key =
            ProviderKey::new("openai", "HTTPS://API.OPENAI.COM/v1", RequestClass::Inference)
                .unwrap();
        assert_eq!(
            key.normalised_base_url(),
            "https://api.openai.com:443/v1"
        );
    }

    #[test]
    fn test_normalise_includes_default_port_https() {
        let key = ProviderKey::new(
            "anthropic",
            "https://api.anthropic.com",
            RequestClass::Inference,
        )
        .unwrap();
        assert_eq!(
            key.normalised_base_url(),
            "https://api.anthropic.com:443"
        );
    }

    #[test]
    fn test_normalise_includes_default_port_http() {
        let key =
            ProviderKey::new("ollama", "http://localhost", RequestClass::Embedding).unwrap();
        assert_eq!(key.normalised_base_url(), "http://localhost:80");
    }

    #[test]
    fn test_normalise_retains_explicit_port() {
        let key = ProviderKey::new(
            "ollama",
            "http://localhost:11434",
            RequestClass::Embedding,
        )
        .unwrap();
        assert_eq!(key.normalised_base_url(), "http://localhost:11434");
    }

    #[test]
    fn test_normalise_retains_path() {
        let key = ProviderKey::new(
            "openai",
            "https://proxy.internal/openai",
            RequestClass::Inference,
        )
        .unwrap();
        assert_eq!(
            key.normalised_base_url(),
            "https://proxy.internal:443/openai"
        );
    }

    #[test]
    fn test_normalise_distinguishes_different_paths() {
        let openai = ProviderKey::new(
            "openai",
            "https://proxy.internal/openai",
            RequestClass::Inference,
        )
        .unwrap();
        let anthropic = ProviderKey::new(
            "openai",
            "https://proxy.internal/anthropic",
            RequestClass::Inference,
        )
        .unwrap();
        assert_ne!(openai, anthropic);
    }

    #[test]
    fn test_normalise_drops_query_string() {
        let key = ProviderKey::new(
            "custom",
            "http://localhost:11434/api?key=abc",
            RequestClass::Inference,
        )
        .unwrap();
        assert_eq!(
            key.normalised_base_url(),
            "http://localhost:11434/api"
        );
    }

    #[test]
    fn test_normalise_drops_fragment() {
        let key = ProviderKey::new(
            "custom",
            "http://localhost:11434/api#section",
            RequestClass::Inference,
        )
        .unwrap();
        assert_eq!(
            key.normalised_base_url(),
            "http://localhost:11434/api"
        );
    }

    #[test]
    fn test_normalise_drops_query_and_fragment() {
        let key = ProviderKey::new(
            "custom",
            "http://localhost:11434/api?key=x#frag",
            RequestClass::Inference,
        )
        .unwrap();
        assert_eq!(
            key.normalised_base_url(),
            "http://localhost:11434/api"
        );
    }

    #[test]
    fn test_unparseable_url_returns_error() {
        let result = ProviderKey::new("test", "not a url", RequestClass::Inference);
        assert!(
            matches!(
                result,
                Err(ProviderRegistryError::UnparseableUrl { ref url, .. })
                if url == "not a url"
            ),
            "expected UnparseableUrl, got {result:?}",
        );
    }

    // -- ProviderKey equality ------------------------------------------------

    #[test]
    fn test_key_same_normalised_url_equal() {
        let a = ProviderKey::new("ollama", "http://localhost:11434/", RequestClass::Embedding)
            .unwrap();
        let b = ProviderKey::new("ollama", "http://localhost:11434", RequestClass::Embedding)
            .unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn test_key_case_insensitive_url_equal() {
        let a = ProviderKey::new(
            "ollama",
            "HTTP://LOCALHOST:11434",
            RequestClass::Embedding,
        )
        .unwrap();
        let b = ProviderKey::new(
            "ollama",
            "http://localhost:11434",
            RequestClass::Embedding,
        )
        .unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn test_key_default_port_equal() {
        let a =
            ProviderKey::new("ollama", "http://localhost", RequestClass::Embedding).unwrap();
        let b =
            ProviderKey::new("ollama", "http://localhost:80", RequestClass::Embedding).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn test_key_different_request_class_not_equal() {
        let embedding = ProviderKey::new(
            "ollama",
            "http://localhost:11434",
            RequestClass::Embedding,
        )
        .unwrap();
        let inference = ProviderKey::new(
            "ollama",
            "http://localhost:11434",
            RequestClass::Inference,
        )
        .unwrap();
        assert_ne!(embedding, inference);
    }

    #[test]
    fn test_key_different_provider_kind_not_equal() {
        let a = ProviderKey::new(
            "openai",
            "https://api.example.com",
            RequestClass::Inference,
        )
        .unwrap();
        let b = ProviderKey::new(
            "anthropic",
            "https://api.example.com",
            RequestClass::Inference,
        )
        .unwrap();
        assert_ne!(a, b);
    }

    // -- ProviderRegistry construction ---------------------------------------

    #[test]
    fn test_registry_construction_and_lookup() {
        let key = ProviderKey::new("ollama", "http://localhost:11434", RequestClass::Embedding)
            .unwrap();
        let limits = ProviderLimits {
            max_in_flight: 4,
            request_timeout: Duration::from_secs(30),
        };
        let registry = ProviderRegistry::new(vec![(key.clone(), limits)]).unwrap();

        assert!(registry.semaphore(&key).is_some());
        assert!(registry.client(&key).is_some());
    }

    #[test]
    fn test_registry_semaphore_permit_count() {
        let key = ProviderKey::new("ollama", "http://localhost:11434", RequestClass::Embedding)
            .unwrap();
        let limits = ProviderLimits {
            max_in_flight: 3,
            request_timeout: Duration::from_secs(30),
        };
        let registry = ProviderRegistry::new(vec![(key.clone(), limits)]).unwrap();
        let semaphore = registry.semaphore(&key).unwrap();

        assert_eq!(semaphore.available_permits(), 3);
    }

    #[test]
    fn test_registry_multiple_entries() {
        let embedding_key = ProviderKey::new(
            "ollama",
            "http://localhost:11434",
            RequestClass::Embedding,
        )
        .unwrap();
        let inference_key = ProviderKey::new(
            "ollama",
            "http://localhost:11434",
            RequestClass::Inference,
        )
        .unwrap();

        let embedding_limits = ProviderLimits {
            max_in_flight: 2,
            request_timeout: Duration::from_secs(60),
        };
        let inference_limits = ProviderLimits {
            max_in_flight: 4,
            request_timeout: Duration::from_secs(30),
        };

        let registry = ProviderRegistry::new(vec![
            (embedding_key.clone(), embedding_limits),
            (inference_key.clone(), inference_limits),
        ])
        .unwrap();

        assert_eq!(
            registry.semaphore(&embedding_key).unwrap().available_permits(),
            2,
        );
        assert_eq!(
            registry.semaphore(&inference_key).unwrap().available_permits(),
            4,
        );
    }

    #[test]
    fn test_registry_lookup_unregistered_key_returns_none() {
        let registered = ProviderKey::new(
            "ollama",
            "http://localhost:11434",
            RequestClass::Embedding,
        )
        .unwrap();
        let unregistered = ProviderKey::new(
            "anthropic",
            "https://api.anthropic.com",
            RequestClass::Inference,
        )
        .unwrap();

        let limits = ProviderLimits {
            max_in_flight: 4,
            request_timeout: Duration::from_secs(30),
        };
        let registry = ProviderRegistry::new(vec![(registered, limits)]).unwrap();

        assert!(registry.semaphore(&unregistered).is_none());
        assert!(registry.client(&unregistered).is_none());
    }

    #[test]
    fn test_registry_empty_construction() {
        let registry = ProviderRegistry::new(Vec::new()).unwrap();
        let key = ProviderKey::new("ollama", "http://localhost:11434", RequestClass::Embedding)
            .unwrap();
        assert!(registry.semaphore(&key).is_none());
        assert!(registry.client(&key).is_none());
    }

    // -- Error cases ---------------------------------------------------------

    #[test]
    fn test_zero_max_in_flight_returns_error() {
        let key = ProviderKey::new("ollama", "http://localhost:11434", RequestClass::Embedding)
            .unwrap();
        let limits = ProviderLimits {
            max_in_flight: 0,
            request_timeout: Duration::from_secs(30),
        };
        let result = ProviderRegistry::new(vec![(key, limits)]);
        assert!(
            matches!(result, Err(ProviderRegistryError::ZeroMaxInFlight { .. })),
            "expected ZeroMaxInFlight, got {result:?}",
        );
    }

    #[test]
    fn test_zero_timeout_returns_error() {
        let key = ProviderKey::new("ollama", "http://localhost:11434", RequestClass::Embedding)
            .unwrap();
        let limits = ProviderLimits {
            max_in_flight: 4,
            request_timeout: Duration::ZERO,
        };
        let result = ProviderRegistry::new(vec![(key, limits)]);
        assert!(
            matches!(result, Err(ProviderRegistryError::ZeroTimeout { .. })),
            "expected ZeroTimeout, got {result:?}",
        );
    }

    #[test]
    fn test_duplicate_key_returns_error() {
        let key = ProviderKey::new("ollama", "http://localhost:11434", RequestClass::Embedding)
            .unwrap();
        let limits = ProviderLimits {
            max_in_flight: 4,
            request_timeout: Duration::from_secs(30),
        };
        let result =
            ProviderRegistry::new(vec![(key.clone(), limits.clone()), (key, limits)]);
        assert!(
            matches!(result, Err(ProviderRegistryError::DuplicateKey { .. })),
            "expected DuplicateKey, got {result:?}",
        );
    }

    #[test]
    fn test_duplicate_key_via_normalisation_returns_error() {
        let key_a = ProviderKey::new(
            "ollama",
            "http://localhost:11434/",
            RequestClass::Embedding,
        )
        .unwrap();
        let key_b = ProviderKey::new(
            "ollama",
            "http://localhost:11434",
            RequestClass::Embedding,
        )
        .unwrap();
        let limits = ProviderLimits {
            max_in_flight: 4,
            request_timeout: Duration::from_secs(30),
        };
        let result =
            ProviderRegistry::new(vec![(key_a, limits.clone()), (key_b, limits)]);
        assert!(
            matches!(result, Err(ProviderRegistryError::DuplicateKey { .. })),
            "expected DuplicateKey, got {result:?}",
        );
    }

    // -- Wiremock integration ------------------------------------------------

    #[tokio::test]
    async fn test_registry_client_sends_user_agent_header() {
        use wiremock::matchers::{header, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(header("user-agent", USER_AGENT))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let key =
            ProviderKey::new("test", &server.uri(), RequestClass::Inference).unwrap();
        let limits = ProviderLimits {
            max_in_flight: 4,
            request_timeout: Duration::from_secs(30),
        };
        let registry = ProviderRegistry::new(vec![(key.clone(), limits)]).unwrap();
        let client = registry.client(&key).unwrap();

        let response = client.get(server.uri()).send().await.unwrap();
        assert!(response.status().is_success());
    }

    #[tokio::test]
    async fn test_registry_client_respects_timeout() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200).set_delay(Duration::from_secs(30)),
            )
            .mount(&server)
            .await;

        let key =
            ProviderKey::new("test", &server.uri(), RequestClass::Inference).unwrap();
        let limits = ProviderLimits {
            max_in_flight: 4,
            request_timeout: Duration::from_millis(50),
        };
        let registry = ProviderRegistry::new(vec![(key.clone(), limits)]).unwrap();
        let client = registry.client(&key).unwrap();

        let result = client.get(server.uri()).send().await;
        assert!(result.is_err(), "expected timeout error");
    }
}
