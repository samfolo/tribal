//! Ollama embedding provider targeting the `/api/embed` endpoint.
//!
//! Implements [`EmbeddingProvider`] for local Ollama instances. The
//! provider owns the model name and expected vector dimensions, validating
//! every response against `expected_dimensions`.

use std::time::Instant;

use async_trait::async_trait;
use tracing::Instrument;
use tribal_domain::{EmbeddingPurpose, span_attrs};

use crate::{
    EmbeddingProvider, EmbeddingRequest, EmbeddingResponse, EmbeddingUsage, InferenceError,
    error::{map_body_read_error, map_http_error, map_json_parse_error, map_send_error},
    http::{latency_ms, normalise_base_url},
    validation::validate_embeddings,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PROVIDER_NAME: &str = "ollama";
const PROBE_INPUT: &str = "tribal probe";
const EMBED_PATH: &str = "/api/embed";

// ---------------------------------------------------------------------------
// Private serde types
// ---------------------------------------------------------------------------

// Targets Ollama /api/embed — tested against Ollama v0.6.x (Feb 2026).
// API reference: https://github.com/ollama/ollama/blob/main/docs/api.md

#[derive(serde::Serialize)]
struct OllamaEmbedRequest<'a> {
    model: &'a str,
    input: &'a str,
    truncate: bool,
}

#[derive(serde::Deserialize)]
struct OllamaEmbedResponse {
    embeddings: Vec<Vec<f32>>,
    #[serde(default)]
    prompt_eval_count: Option<u32>,
    #[serde(default)]
    total_duration: Option<u64>,
    #[serde(default)]
    load_duration: Option<u64>,
}

// ---------------------------------------------------------------------------
// OllamaEmbeddingProvider
// ---------------------------------------------------------------------------

/// Concrete embedding provider for Ollama's `/api/embed` endpoint.
///
/// Owns the model name and expected vector dimensions. Validates every
/// response vector against `expected_dimensions` and returns
/// [`InferenceError::ResponseParseFailed`] on mismatch.
pub struct OllamaEmbeddingProvider {
    client: reqwest::Client,
    base_url: String,
    model: String,
    expected_dimensions: u32,
}

impl OllamaEmbeddingProvider {
    /// Creates a new Ollama embedding provider.
    pub fn new(
        client: reqwest::Client,
        base_url: impl Into<String>,
        model: impl Into<String>,
        expected_dimensions: u32,
    ) -> Self {
        Self {
            client,
            base_url: normalise_base_url(base_url),
            model: model.into(),
            expected_dimensions,
        }
    }

    /// Validates model availability and dimension configuration.
    ///
    /// Sends a best-effort GET to `/api/tags` to check whether the
    /// configured model is locally available, then embeds the canonical
    /// string `"tribal probe"` and validates the returned vector length
    /// against `expected_dimensions`.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::ProviderUnavailable`] if Ollama cannot
    /// be reached.  Returns [`InferenceError::ResponseParseFailed`] if
    /// the returned vector length does not match expectations.
    pub async fn probe_model(&self) -> Result<(), InferenceError> {
        let span = tracing::info_span!(
            "tribal.embedding.probe",
            { span_attrs::EMBEDDING_PROVIDER } = PROVIDER_NAME,
            { span_attrs::EMBEDDING_MODEL } = %self.model,
        );

        async {
            super::tags::check_tags(&self.client, &self.base_url, &self.model).await;

            let request = EmbeddingRequest {
                input: PROBE_INPUT.to_owned(),
                purpose: EmbeddingPurpose::Candidate,
            };
            let _response = self.embed(request).await?;

            tracing::info!(
                dimensions = self.expected_dimensions,
                "model {} probe succeeded",
                self.model,
            );
            Ok(())
        }
        .instrument(span)
        .await
    }
}

// ---------------------------------------------------------------------------
// EmbeddingProvider impl
// ---------------------------------------------------------------------------

#[async_trait]
impl EmbeddingProvider for OllamaEmbeddingProvider {
    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse, InferenceError> {
        if request.input.is_empty() {
            return Err(InferenceError::EmbeddingFailed {
                model: self.model.clone(),
                context: "input text is empty".to_owned(),
                source: None,
            });
        }

        let span = tracing::info_span!(
            "tribal.embedding.generate",
            { span_attrs::EMBEDDING_PROVIDER } = PROVIDER_NAME,
            { span_attrs::EMBEDDING_MODEL } = %self.model,
            { span_attrs::EMBEDDING_PURPOSE } = %request.purpose,
            { span_attrs::EMBEDDING_TOKENS } = tracing::field::Empty,
            { span_attrs::EMBEDDING_DIMENSIONS } = tracing::field::Empty,
            { span_attrs::EMBEDDING_LATENCY_MS } = tracing::field::Empty,
        );

        async {
            let started = Instant::now();
            let url = format!("{}{EMBED_PATH}", self.base_url);
            let body = OllamaEmbedRequest {
                model: &self.model,
                input: &request.input,
                truncate: true,
            };

            let http_response = self
                .client
                .post(&url)
                .json(&body)
                .send()
                .await
                .map_err(|e| map_send_error(&e, PROVIDER_NAME))?;

            let status = http_response.status();
            let response_body = http_response
                .text()
                .await
                .map_err(|e| map_body_read_error(&e, PROVIDER_NAME))?;

            let latency = started.elapsed();

            if !status.is_success() {
                return Err(map_http_error(
                    status,
                    &response_body,
                    PROVIDER_NAME,
                    |ctx| InferenceError::EmbeddingFailed {
                        model: self.model.clone(),
                        context: ctx,
                        source: None,
                    },
                ));
            }

            let parsed: OllamaEmbedResponse =
                serde_json::from_str(&response_body).map_err(|e| {
                    map_json_parse_error(&e, "OllamaEmbedResponse JSON object", &response_body)
                })?;

            if let Some(total_ns) = parsed.total_duration {
                tracing::debug!(total_duration_ms = total_ns / 1_000_000, "provider timing");
            }
            if let Some(load_ns) = parsed.load_duration {
                tracing::debug!(load_duration_ms = load_ns / 1_000_000, "provider timing");
            }

            let vector =
                validate_embeddings(parsed.embeddings, self.expected_dimensions, &self.model)?;

            let total_tokens = parsed.prompt_eval_count.unwrap_or_else(|| {
                tracing::debug!("prompt_eval_count absent, defaulting to 0");
                0
            });

            let latency_ms = latency_ms(latency);

            let current = tracing::Span::current();
            current.record(span_attrs::EMBEDDING_TOKENS, total_tokens);
            current.record(span_attrs::EMBEDDING_DIMENSIONS, self.expected_dimensions);
            current.record(span_attrs::EMBEDDING_LATENCY_MS, latency_ms);

            tracing::debug!(
                tokens = total_tokens,
                dimensions = self.expected_dimensions,
                latency_ms,
                "embedding generated",
            );

            Ok(EmbeddingResponse {
                vector,
                usage: EmbeddingUsage {
                    provider: PROVIDER_NAME.to_owned(),
                    model: self.model.clone(),
                    total_tokens,
                    latency,
                },
            })
        }
        .instrument(span)
        .await
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tribal_domain::EmbeddingPurpose;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, method, path},
    };

    use super::*;
    use crate::ollama::tags::TAGS_PATH;

    fn a_request(input: &str) -> EmbeddingRequest {
        EmbeddingRequest {
            input: input.to_owned(),
            purpose: EmbeddingPurpose::Candidate,
        }
    }

    fn a_valid_response_json(dims: usize) -> serde_json::Value {
        serde_json::json!({
            "embeddings": [vec![0.1_f32; dims]],
            "prompt_eval_count": 5,
            "total_duration": 100_000_000_u64,
            "load_duration": 10_000_000_u64,
        })
    }

    fn setup(server: &MockServer, dims: u32) -> OllamaEmbeddingProvider {
        OllamaEmbeddingProvider::new(
            reqwest::Client::new(),
            server.uri(),
            "nomic-embed-text:v1.5",
            dims,
        )
    }

    // -- Constructor ---------------------------------------------------------

    #[tokio::test]
    async fn test_embed_trailing_slash_normalised() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json(3)))
            .expect(1)
            .mount(&server)
            .await;

        let url_with_slash = format!("{}/", server.uri());
        let provider = OllamaEmbeddingProvider::new(
            reqwest::Client::new(),
            url_with_slash,
            "nomic-embed-text:v1.5",
            3,
        );

        provider.embed(a_request("test")).await.unwrap();
    }

    // -- Happy path ---------------------------------------------------------

    #[tokio::test]
    async fn test_embed_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json(3)))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let response = provider.embed(a_request("test input")).await.unwrap();

        assert_eq!(response.vector, vec![0.1, 0.1, 0.1]);
        assert_eq!(response.usage.provider, PROVIDER_NAME);
        assert_eq!(response.usage.model, "nomic-embed-text:v1.5");
        assert_eq!(response.usage.total_tokens, 5);
        assert!(response.usage.latency > Duration::ZERO);
    }

    #[tokio::test]
    async fn test_embed_sends_correct_request_body() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .and(body_json(serde_json::json!({
                "model": "nomic-embed-text:v1.5",
                "input": "hello world",
                "truncate": true,
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json(3)))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let _ = provider.embed(a_request("hello world")).await.unwrap();
    }

    // -- Input validation ---------------------------------------------------

    #[tokio::test]
    async fn test_embed_empty_input_returns_embedding_failed() {
        let server = MockServer::start().await;
        let provider = setup(&server, 3);

        let err = provider.embed(a_request("")).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::EmbeddingFailed {
                ref model,
                ref context,
                ..
            }
            if model == "nomic-embed-text:v1.5"
                && context == "input text is empty"
        ));
    }

    // -- Network errors -----------------------------------------------------

    #[tokio::test]
    async fn test_embed_connection_refused_returns_provider_unavailable() {
        let provider =
            OllamaEmbeddingProvider::new(reqwest::Client::new(), "http://127.0.0.1:1", "model", 3);

        let err = provider.embed(a_request("test")).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref provider, .. }
            if provider == PROVIDER_NAME
        ));
    }

    #[tokio::test]
    async fn test_embed_timeout_returns_provider_unavailable() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(a_valid_response_json(3))
                    .set_delay(Duration::from_secs(30)),
            )
            .mount(&server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(50))
            .build()
            .unwrap();

        let provider = OllamaEmbeddingProvider::new(client, server.uri(), "model", 3);

        let err = provider.embed(a_request("test")).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref provider, .. }
            if provider == PROVIDER_NAME
        ));
    }

    // -- HTTP error mapping -------------------------------------------------

    #[tokio::test]
    async fn test_embed_http_500_returns_provider_unavailable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::ProviderUnavailable { ref reason, .. }
                if reason.contains("500") && reason.contains("internal error")
            ),
            "expected ProviderUnavailable with status and body, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_embed_http_429_returns_provider_unavailable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::ProviderUnavailable { ref reason, .. }
                if reason.contains("429")
            ),
            "expected ProviderUnavailable with 429 status, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_embed_http_400_returns_embedding_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::EmbeddingFailed { ref context, .. }
                if context.contains("400") && context.contains("bad request")
            ),
            "expected EmbeddingFailed with status and body, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_embed_http_404_returns_embedding_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(serde_json::json!({"error": "model not found"})),
            )
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::EmbeddingFailed { ref context, .. }
                if context.contains("404")
            ),
            "expected EmbeddingFailed with 404 status, got {err:?}"
        );
    }

    // -- Response parsing ---------------------------------------------------

    #[tokio::test]
    async fn test_embed_malformed_json_returns_response_parse_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json at all"))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::ResponseParseFailed {
                    ref expected_shape,
                    ref actual,
                }
                if expected_shape == "OllamaEmbedResponse JSON object"
                    && actual.starts_with("invalid JSON:")
            ),
            "expected ResponseParseFailed with shape and JSON error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_embed_empty_embeddings_returns_embedding_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embeddings": [],
                "prompt_eval_count": 0,
            })))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(matches!(
            err,
            InferenceError::EmbeddingFailed { ref context, .. }
            if context == "embeddings array is empty"
        ));
    }

    #[tokio::test]
    async fn test_embed_multiple_embeddings_returns_response_parse_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embeddings": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
                "prompt_eval_count": 5,
            })))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::ResponseParseFailed {
                    ref expected_shape,
                    ref actual,
                }
                if expected_shape == "embeddings array length == 1"
                    && actual == "embeddings array length == 2"
            ),
            "expected ResponseParseFailed with array length info, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_embed_dimension_mismatch_returns_response_parse_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json(5)))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::ResponseParseFailed {
                    ref expected_shape,
                    ref actual,
                }
                if expected_shape == "embedding vector length == 3"
                    && actual == "embedding vector length == 5"
            ),
            "expected ResponseParseFailed with dimension info, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_embed_missing_prompt_eval_count_defaults_to_zero() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embeddings": [[0.1, 0.2, 0.3]],
            })))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let response = provider.embed(a_request("test")).await.unwrap();

        assert_eq!(response.usage.total_tokens, 0);
    }

    // -- Response body truncation -------------------------------------------

    #[tokio::test]
    async fn test_embed_response_body_truncated_in_error_context() {
        let server = MockServer::start().await;
        let long_body = "x".repeat(500);
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(500).set_body_string(&long_body))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::ProviderUnavailable { ref reason, .. }
                if reason.contains("...") && reason.len() < 300
            ),
            "expected ProviderUnavailable with truncated body, got {err:?}"
        );
    }

    // -- Probe tests --------------------------------------------------------

    #[tokio::test]
    async fn test_probe_model_success() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path(TAGS_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [{"name": "nomic-embed-text:v1.5"}],
            })))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json(3)))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        provider.probe_model().await.unwrap();
    }

    #[tokio::test]
    async fn test_probe_model_tags_failure_continues() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path(TAGS_PATH))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json(3)))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        provider.probe_model().await.unwrap();
    }

    #[tokio::test]
    async fn test_probe_model_embed_failure_propagates() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path(TAGS_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [],
            })))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(500).set_body_string("server error"))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let err = provider.probe_model().await.unwrap_err();
        assert!(
            matches!(err, InferenceError::ProviderUnavailable { .. }),
            "expected ProviderUnavailable, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_probe_model_dimension_mismatch_propagates() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path(TAGS_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [{"name": "nomic-embed-text:v1.5"}],
            })))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json(5)))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let err = provider.probe_model().await.unwrap_err();
        assert!(
            matches!(err, InferenceError::ResponseParseFailed { .. }),
            "expected ResponseParseFailed, got {err:?}"
        );
    }
}
