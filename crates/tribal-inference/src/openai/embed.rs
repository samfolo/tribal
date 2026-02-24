//! `OpenAI` embedding provider targeting the `/v1/embeddings` endpoint.
//!
//! Implements [`EmbeddingProvider`] for `OpenAI` and `OpenAI`-compatible
//! runtimes (`vLLM`, `LM Studio`, `LocalAI`, `LiteLLM`). The provider owns
//! the model name and expected vector dimensions, validating every
//! response against `expected_dimensions`.

use std::time::Instant;

use async_trait::async_trait;
use tracing::Instrument;
use tribal_domain::{EmbeddingPurpose, span_attrs};

use crate::{
    EmbeddingProvider, EmbeddingRequest, EmbeddingResponse, EmbeddingUsage, InferenceError,
    ProviderIdentity,
    error::{map_body_read_error, map_http_error, map_json_parse_error, map_send_error},
    http::{latency_ms, normalise_base_url},
    validation::validate_embeddings,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PROVIDER_NAME: &str = "openai";
const PROBE_INPUT: &str = "tribal probe";
const EMBED_PATH: &str = "/v1/embeddings";

// ---------------------------------------------------------------------------
// Private serde types
// ---------------------------------------------------------------------------

// Targets OpenAI /v1/embeddings — tested against OpenAI API (Feb 2026).

#[derive(serde::Serialize)]
struct OpenAiEmbedRequest<'a> {
    model: &'a str,
    input: &'a str,
    encoding_format: &'a str,
}

#[derive(serde::Deserialize)]
struct OpenAiEmbedResponse {
    data: Vec<OpenAiEmbedData>,
    usage: OpenAiEmbedUsage,
}

#[derive(serde::Deserialize)]
struct OpenAiEmbedData {
    embedding: Vec<f32>,
}

#[derive(serde::Deserialize)]
struct OpenAiEmbedUsage {
    total_tokens: u32,
}

// ---------------------------------------------------------------------------
// OpenAiEmbeddingProvider
// ---------------------------------------------------------------------------

/// Concrete embedding provider for `OpenAI`'s `/v1/embeddings` endpoint.
///
/// Compatible with any `OpenAI`-compatible runtime. Owns the model name
/// and expected vector dimensions. Validates every response vector
/// against `expected_dimensions` and returns
/// [`InferenceError::ResponseParseFailed`] on mismatch.
///
/// Authentication is set per-request via the `Authorization: Bearer`
/// header — the shared [`reqwest::Client`] is not pre-configured with
/// credentials.
pub struct OpenAiEmbeddingProvider {
    client: reqwest::Client,
    base_url: String,
    identity: ProviderIdentity,
    api_key: String,
    expected_dimensions: u32,
}

impl OpenAiEmbeddingProvider {
    /// Creates a new `OpenAI` embedding provider.
    pub fn new(
        client: reqwest::Client,
        base_url: impl Into<String>,
        model: impl Into<String>,
        api_key: impl Into<String>,
        expected_dimensions: u32,
    ) -> Self {
        Self {
            client,
            base_url: normalise_base_url(base_url),
            identity: ProviderIdentity {
                name: PROVIDER_NAME.to_owned(),
                model: model.into(),
            },
            api_key: api_key.into(),
            expected_dimensions,
        }
    }

    /// Validates model availability and dimension configuration.
    ///
    /// Embeds the canonical string `"tribal probe"` and validates the
    /// returned vector length against `expected_dimensions`.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::ProviderUnavailable`] if the provider
    /// cannot be reached.  Returns [`InferenceError::EmbeddingFailed`]
    /// if the API key or model is rejected.  Returns
    /// [`InferenceError::ResponseParseFailed`] if the returned vector
    /// length does not match expectations.
    pub async fn probe_model(&self) -> Result<(), InferenceError> {
        let span = tracing::info_span!(
            "tribal.embedding.probe",
            { span_attrs::EMBEDDING_PROVIDER } = PROVIDER_NAME,
            { span_attrs::EMBEDDING_MODEL } = %self.identity.model,
        );

        async {
            let request = EmbeddingRequest {
                input: PROBE_INPUT.to_owned(),
                purpose: EmbeddingPurpose::Candidate,
            };
            let _response = self.embed(request).await?;

            tracing::info!(
                dimensions = self.expected_dimensions,
                "model {} probe succeeded",
                self.identity.model,
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
impl EmbeddingProvider for OpenAiEmbeddingProvider {
    fn identity(&self) -> &ProviderIdentity {
        &self.identity
    }

    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse, InferenceError> {
        if request.input.is_empty() {
            return Err(InferenceError::EmbeddingFailed {
                model: self.identity.model.clone(),
                context: "input text is empty".to_owned(),
                source: None,
            });
        }

        let span = tracing::info_span!(
            "tribal.embedding.generate",
            { span_attrs::EMBEDDING_PROVIDER } = PROVIDER_NAME,
            { span_attrs::EMBEDDING_MODEL } = %self.identity.model,
            { span_attrs::EMBEDDING_PURPOSE } = %request.purpose,
            { span_attrs::EMBEDDING_TOKENS } = tracing::field::Empty,
            { span_attrs::EMBEDDING_DIMENSIONS } = tracing::field::Empty,
            { span_attrs::EMBEDDING_LATENCY_MS } = tracing::field::Empty,
        );

        async {
            let started = Instant::now();
            let url = format!("{}{EMBED_PATH}", self.base_url);
            let body = OpenAiEmbedRequest {
                model: &self.identity.model,
                input: &request.input,
                encoding_format: "float",
            };

            let http_response = self
                .client
                .post(&url)
                .header("Authorization", format!("Bearer {}", self.api_key))
                .json(&body)
                .send()
                .await
                .map_err(|e| map_send_error(&e, PROVIDER_NAME))?;

            let status = http_response.status();
            let retry_after = http_response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let response_body = http_response
                .text()
                .await
                .map_err(|e| map_body_read_error(&e, PROVIDER_NAME))?;

            let latency = started.elapsed();

            if !status.is_success() {
                let extra: Vec<(&str, &str)> = retry_after
                    .as_deref()
                    .map(|v| vec![("Retry-After", v)])
                    .unwrap_or_default();
                return Err(map_http_error(
                    status,
                    &response_body,
                    PROVIDER_NAME,
                    &extra,
                    |ctx| InferenceError::EmbeddingFailed {
                        model: self.identity.model.clone(),
                        context: ctx,
                        source: None,
                    },
                ));
            }

            let parsed: OpenAiEmbedResponse =
                serde_json::from_str(&response_body).map_err(|e| {
                    map_json_parse_error(&e, "OpenAiEmbedResponse JSON object", &response_body)
                })?;

            let embeddings: Vec<Vec<f32>> = parsed.data.into_iter().map(|d| d.embedding).collect();
            let vector = validate_embeddings(embeddings, self.expected_dimensions, &self.identity.model)?;

            let total_tokens = parsed.usage.total_tokens;
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
                    model: self.identity.model.clone(),
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
        matchers::{body_json, header, method, path},
    };

    use super::*;

    fn a_request(input: &str) -> EmbeddingRequest {
        EmbeddingRequest {
            input: input.to_owned(),
            purpose: EmbeddingPurpose::Candidate,
        }
    }

    fn a_valid_response_json(dims: usize) -> serde_json::Value {
        serde_json::json!({
            "object": "list",
            "data": [{
                "object": "embedding",
                "embedding": vec![0.1_f32; dims],
                "index": 0,
            }],
            "model": "text-embedding-3-small",
            "usage": {
                "prompt_tokens": 5,
                "total_tokens": 5,
            },
        })
    }

    fn setup(server: &MockServer, dims: u32) -> OpenAiEmbeddingProvider {
        OpenAiEmbeddingProvider::new(
            reqwest::Client::new(),
            server.uri(),
            "text-embedding-3-small",
            "test-key",
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
        let provider = OpenAiEmbeddingProvider::new(
            reqwest::Client::new(),
            url_with_slash,
            "text-embedding-3-small",
            "test-key",
            3,
        );

        provider.embed(a_request("test")).await.unwrap();
    }

    // -- Happy path ----------------------------------------------------------

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
        assert_eq!(response.usage.model, "text-embedding-3-small");
        assert_eq!(response.usage.total_tokens, 5);
    }

    #[tokio::test]
    async fn test_embed_sends_correct_request_body() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .and(body_json(serde_json::json!({
                "model": "text-embedding-3-small",
                "input": "hello world",
                "encoding_format": "float",
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json(3)))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let _ = provider.embed(a_request("hello world")).await.unwrap();
    }

    // -- Auth headers --------------------------------------------------------

    #[tokio::test]
    async fn test_embed_sends_bearer_auth_header() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .and(header("Authorization", "Bearer test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json(3)))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let _ = provider.embed(a_request("test")).await.unwrap();
    }

    // -- Input validation ----------------------------------------------------

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
            if model == "text-embedding-3-small"
                && context == "input text is empty"
        ));
    }

    // -- Network errors ------------------------------------------------------

    #[tokio::test]
    async fn test_embed_connection_refused_returns_provider_unavailable() {
        let provider = OpenAiEmbeddingProvider::new(
            reqwest::Client::new(),
            "http://127.0.0.1:1",
            "model",
            "key",
            3,
        );

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

        let provider = OpenAiEmbeddingProvider::new(client, server.uri(), "model", "key", 3);

        let err = provider.embed(a_request("test")).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref provider, .. }
            if provider == PROVIDER_NAME
        ));
    }

    // -- HTTP error mapping --------------------------------------------------

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

    #[tokio::test]
    async fn test_embed_http_401_returns_embedding_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(401).set_body_string("unauthorised"))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::EmbeddingFailed { ref context, .. }
                if context.contains("401")
            ),
            "expected EmbeddingFailed with 401 status, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_embed_http_403_returns_embedding_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::EmbeddingFailed { ref context, .. }
                if context.contains("403")
            ),
            "expected EmbeddingFailed with 403 status, got {err:?}"
        );
    }

    // -- 429 Retry-After -----------------------------------------------------

    #[tokio::test]
    async fn test_embed_http_429_includes_retry_after_in_context() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(
                ResponseTemplate::new(429)
                    .set_body_string("rate limited")
                    .append_header("Retry-After", "30"),
            )
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::ProviderUnavailable { ref reason, .. }
                if reason.contains("429") && reason.contains("; Retry-After: 30")
            ),
            "expected ProviderUnavailable with Retry-After metadata, got {err:?}"
        );
    }

    // -- Response parsing ----------------------------------------------------

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
                if expected_shape == "OpenAiEmbedResponse JSON object"
                    && actual.starts_with("invalid JSON:")
            ),
            "expected ResponseParseFailed with shape and JSON error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_embed_empty_data_returns_embedding_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "object": "list",
                "data": [],
                "usage": { "prompt_tokens": 0, "total_tokens": 0 },
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
    async fn test_embed_multiple_data_returns_response_parse_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "object": "list",
                "data": [
                    { "object": "embedding", "embedding": [0.1, 0.2, 0.3], "index": 0 },
                    { "object": "embedding", "embedding": [0.4, 0.5, 0.6], "index": 1 },
                ],
                "usage": { "prompt_tokens": 5, "total_tokens": 5 },
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

    // -- Response body truncation --------------------------------------------

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

    // -- Probe tests ---------------------------------------------------------

    #[tokio::test]
    async fn test_probe_model_success() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(EMBED_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json(3)))
            .mount(&server)
            .await;

        let provider = setup(&server, 3);
        provider.probe_model().await.unwrap();
    }

    #[tokio::test]
    async fn test_probe_model_failure_propagates() {
        let server = MockServer::start().await;

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
