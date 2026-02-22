//! Ollama embedding provider targeting the `/api/embed` endpoint.
//!
//! Implements [`EmbeddingProvider`] for local Ollama instances. The
//! provider owns the model name and expected vector dimensions, validating
//! every response against `expected_dimensions`.

use std::time::Instant;

use async_trait::async_trait;
use tribal_domain::{EmbeddingPurpose, span_attrs};

use crate::{
    EmbeddingProvider, EmbeddingRequest, EmbeddingResponse, EmbeddingUsage, InferenceError,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PROVIDER_NAME: &str = "ollama";
const PROBE_INPUT: &str = "tribal probe";
const BODY_PREVIEW_LIMIT: usize = 200;

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

#[derive(serde::Deserialize)]
struct OllamaTagsResponse {
    models: Vec<OllamaTagModel>,
}

#[derive(serde::Deserialize)]
struct OllamaTagModel {
    name: String,
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
            base_url: base_url.into(),
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
        let _guard = span.enter();

        self.check_tags().await;

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

    /// Best-effort check of `/api/tags` for model availability.
    async fn check_tags(&self) {
        let url = format!("{}/api/tags", self.base_url);
        let result = self.client.get(&url).send().await;

        match result {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(tags) = resp.json::<OllamaTagsResponse>().await {
                    let found = tags
                        .models
                        .iter()
                        .any(|m| m.name == self.model || m.name.starts_with(&format!("{}:", self.model)));

                    if !found {
                        tracing::warn!(
                            model = %self.model,
                            "model not found in /api/tags — ensure it has been pulled",
                        );
                    }
                }
            }
            Ok(resp) => {
                tracing::warn!(
                    status = %resp.status(),
                    "/api/tags returned non-success status",
                );
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "/api/tags unreachable (best-effort check)",
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// EmbeddingProvider impl
// ---------------------------------------------------------------------------

#[async_trait]
impl EmbeddingProvider for OllamaEmbeddingProvider {
    async fn embed(
        &self,
        request: EmbeddingRequest,
    ) -> Result<EmbeddingResponse, InferenceError> {
        // Input validation — before span or HTTP call.
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
        let _guard = span.enter();

        let started = Instant::now();

        let url = format!("{}/api/embed", self.base_url);
        let body = OllamaEmbedRequest {
            model: &self.model,
            input: &request.input,
            truncate: true,
        };

        // Send request.
        let http_response = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "embed request failed");
                InferenceError::ProviderUnavailable {
                    provider: PROVIDER_NAME.to_owned(),
                    reason: e.to_string(),
                }
            })?;

        let status = http_response.status();
        let response_body = http_response.text().await.map_err(|e| {
            InferenceError::ProviderUnavailable {
                provider: PROVIDER_NAME.to_owned(),
                reason: format!("failed to read response body: {e}"),
            }
        })?;

        let latency = started.elapsed();

        // Map non-success HTTP status codes.
        if !status.is_success() {
            let preview = truncate_body(&response_body);
            if status.as_u16() == 429 || status.is_server_error() {
                tracing::warn!(%status, "provider returned retryable error");
                return Err(InferenceError::ProviderUnavailable {
                    provider: PROVIDER_NAME.to_owned(),
                    reason: format!("HTTP {status}: {preview}"),
                });
            }
            tracing::warn!(%status, "provider returned non-retryable error");
            return Err(InferenceError::EmbeddingFailed {
                model: self.model.clone(),
                context: format!("HTTP {status}: {preview}"),
                source: None,
            });
        }

        // Parse JSON response.
        let parsed: OllamaEmbedResponse =
            serde_json::from_str(&response_body).map_err(|e| {
                tracing::warn!(error = %e, "failed to parse embed response");
                tracing::debug!(body = %response_body, "raw response body");
                InferenceError::ResponseParseFailed {
                    expected_shape: "OllamaEmbedResponse JSON object".to_owned(),
                    actual: format!("invalid JSON: {e}"),
                }
            })?;

        // Log provider-reported durations at debug level.
        if let Some(total_ns) = parsed.total_duration {
            tracing::debug!(total_duration_ms = total_ns / 1_000_000, "provider timing");
        }
        if let Some(load_ns) = parsed.load_duration {
            tracing::debug!(load_duration_ms = load_ns / 1_000_000, "provider timing");
        }

        // Validate embeddings array length.
        if parsed.embeddings.is_empty() {
            return Err(InferenceError::EmbeddingFailed {
                model: self.model.clone(),
                context: "embeddings array is empty".to_owned(),
                source: None,
            });
        }
        if parsed.embeddings.len() > 1 {
            return Err(InferenceError::ResponseParseFailed {
                expected_shape: "embeddings array length == 1".to_owned(),
                actual: format!(
                    "embeddings array length == {}",
                    parsed.embeddings.len()
                ),
            });
        }

        let vector = parsed.embeddings.into_iter().next().expect("checked non-empty");

        // Validate dimensions.
        let actual_dims = vector.len();
        let expected = self.expected_dimensions as usize;
        if actual_dims != expected {
            tracing::error!(
                expected = expected,
                actual = actual_dims,
                "dimension mismatch — configuration error",
            );
            return Err(InferenceError::ResponseParseFailed {
                expected_shape: format!("embedding vector length == {expected}"),
                actual: format!("embedding vector length == {actual_dims}"),
            });
        }

        // Handle optional prompt_eval_count.
        let total_tokens = parsed.prompt_eval_count.unwrap_or_else(|| {
            tracing::debug!("prompt_eval_count absent, defaulting to 0");
            0
        });

        span.record(span_attrs::EMBEDDING_TOKENS, total_tokens);
        span.record(span_attrs::EMBEDDING_DIMENSIONS, actual_dims as u32);
        span.record(
            span_attrs::EMBEDDING_LATENCY_MS,
            latency.as_millis() as u64,
        );

        tracing::debug!(
            tokens = total_tokens,
            dimensions = actual_dims,
            latency_ms = latency.as_millis() as u64,
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
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Truncates a response body to at most [`BODY_PREVIEW_LIMIT`] characters,
/// collapsing whitespace and ensuring UTF-8 safety.
fn truncate_body(body: &str) -> String {
    let normalised: String = body.split_whitespace().collect::<Vec<_>>().join(" ");

    if normalised.len() <= BODY_PREVIEW_LIMIT {
        return normalised;
    }

    let boundary = normalised.floor_char_boundary(BODY_PREVIEW_LIMIT);
    format!("{}...", &normalised[..boundary])
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tribal_domain::EmbeddingPurpose;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

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

    async fn setup(server: &MockServer, dims: u32) -> OllamaEmbeddingProvider {
        OllamaEmbeddingProvider::new(
            reqwest::Client::new(),
            server.uri(),
            "nomic-embed-text:v1.5",
            dims,
        )
    }

    // -- Happy path ---------------------------------------------------------

    #[tokio::test]
    async fn test_embed_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(a_valid_response_json(3)),
            )
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        let response = provider.embed(a_request("test input")).await.unwrap();

        assert_eq!(response.vector, vec![0.1, 0.1, 0.1]);
        assert_eq!(response.usage.provider, "ollama");
        assert_eq!(response.usage.model, "nomic-embed-text:v1.5");
        assert_eq!(response.usage.total_tokens, 5);
        assert!(response.usage.latency > Duration::ZERO);
    }

    #[tokio::test]
    async fn test_embed_sends_correct_request_body() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .and(body_json(serde_json::json!({
                "model": "nomic-embed-text:v1.5",
                "input": "hello world",
                "truncate": true,
            })))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(a_valid_response_json(3)),
            )
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        let _ = provider.embed(a_request("hello world")).await.unwrap();
    }

    // -- Input validation ---------------------------------------------------

    #[tokio::test]
    async fn test_embed_empty_input_returns_embedding_failed() {
        let server = MockServer::start().await;
        // No mock mounted — request must not reach the server.
        let provider = setup(&server, 3).await;

        let err = provider.embed(a_request("")).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::EmbeddingFailed { ref context, .. }
            if context.contains("empty")
        ));
    }

    // -- Network errors -----------------------------------------------------

    #[tokio::test]
    async fn test_embed_connection_refused_returns_provider_unavailable() {
        let provider = OllamaEmbeddingProvider::new(
            reqwest::Client::new(),
            "http://127.0.0.1:1", // nothing listening
            "model",
            3,
        );

        let err = provider.embed(a_request("test")).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref provider, .. }
            if provider == "ollama"
        ));
    }

    #[tokio::test]
    async fn test_embed_timeout_returns_provider_unavailable() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/embed"))
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

        let provider = OllamaEmbeddingProvider::new(
            client,
            server.uri(),
            "model",
            3,
        );

        let err = provider.embed(a_request("test")).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref provider, .. }
            if provider == "ollama"
        ));
    }

    // -- HTTP error mapping -------------------------------------------------

    #[tokio::test]
    async fn test_embed_http_500_returns_provider_unavailable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref reason, .. }
            if reason.contains("500") && reason.contains("internal error")
        ));
    }

    #[tokio::test]
    async fn test_embed_http_429_returns_provider_unavailable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref reason, .. }
            if reason.contains("429")
        ));
    }

    #[tokio::test]
    async fn test_embed_http_400_returns_embedding_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(matches!(
            err,
            InferenceError::EmbeddingFailed { ref context, .. }
            if context.contains("400") && context.contains("bad request")
        ));
    }

    #[tokio::test]
    async fn test_embed_http_404_returns_embedding_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(serde_json::json!({"error": "model not found"})),
            )
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(matches!(
            err,
            InferenceError::EmbeddingFailed { ref context, .. }
            if context.contains("404")
        ));
    }

    // -- Response parsing ---------------------------------------------------

    #[tokio::test]
    async fn test_embed_malformed_json_returns_response_parse_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string("not json at all"),
            )
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(matches!(
            err,
            InferenceError::ResponseParseFailed {
                ref expected_shape, ..
            }
            if expected_shape == "OllamaEmbedResponse JSON object"
        ));
    }

    #[tokio::test]
    async fn test_embed_empty_embeddings_returns_embedding_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embeddings": [],
                "prompt_eval_count": 0,
            })))
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(matches!(
            err,
            InferenceError::EmbeddingFailed { ref context, .. }
            if context.contains("empty")
        ));
    }

    #[tokio::test]
    async fn test_embed_multiple_embeddings_returns_response_parse_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embeddings": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
                "prompt_eval_count": 5,
            })))
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(matches!(
            err,
            InferenceError::ResponseParseFailed {
                ref expected_shape,
                ref actual,
            }
            if expected_shape == "embeddings array length == 1"
                && actual == "embeddings array length == 2"
        ));
    }

    #[tokio::test]
    async fn test_embed_dimension_mismatch_returns_response_parse_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(a_valid_response_json(5)),
            )
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        let err = provider.embed(a_request("test")).await.unwrap_err();

        assert!(matches!(
            err,
            InferenceError::ResponseParseFailed {
                ref expected_shape,
                ref actual,
            }
            if expected_shape == "embedding vector length == 3"
                && actual == "embedding vector length == 5"
        ));
    }

    #[tokio::test]
    async fn test_embed_missing_prompt_eval_count_defaults_to_zero() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embeddings": [[0.1, 0.2, 0.3]],
            })))
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        let response = provider.embed(a_request("test")).await.unwrap();

        assert_eq!(response.usage.total_tokens, 0);
    }

    // -- Response body truncation -------------------------------------------

    #[tokio::test]
    async fn test_embed_response_body_truncated_in_error_context() {
        let server = MockServer::start().await;
        let long_body = "x".repeat(500);
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(
                ResponseTemplate::new(500).set_body_string(&long_body),
            )
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        let err = provider.embed(a_request("test")).await.unwrap_err();

        match err {
            InferenceError::ProviderUnavailable { reason, .. } => {
                assert!(reason.len() < 300, "reason should be truncated");
                assert!(reason.contains("..."), "should end with ellipsis");
            }
            other => panic!("expected ProviderUnavailable, got {other:?}"),
        }
    }

    // -- Probe tests --------------------------------------------------------

    #[tokio::test]
    async fn test_probe_model_success() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/tags"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [{"name": "nomic-embed-text:v1.5"}],
            })))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(a_valid_response_json(3)),
            )
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        provider.probe_model().await.unwrap();
    }

    #[tokio::test]
    async fn test_probe_model_tags_failure_continues() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/tags"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(a_valid_response_json(3)),
            )
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        provider.probe_model().await.unwrap();
    }

    #[tokio::test]
    async fn test_probe_model_embed_failure_propagates() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/tags"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [],
            })))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(ResponseTemplate::new(500).set_body_string("server error"))
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        let err = provider.probe_model().await.unwrap_err();
        assert!(matches!(err, InferenceError::ProviderUnavailable { .. }));
    }

    #[tokio::test]
    async fn test_probe_model_dimension_mismatch_propagates() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/tags"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [{"name": "nomic-embed-text:v1.5"}],
            })))
            .mount(&server)
            .await;

        // Response has 5 dimensions but provider expects 3.
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(a_valid_response_json(5)),
            )
            .mount(&server)
            .await;

        let provider = setup(&server, 3).await;
        let err = provider.probe_model().await.unwrap_err();
        assert!(matches!(err, InferenceError::ResponseParseFailed { .. }));
    }

    // -- Truncation helper --------------------------------------------------

    #[test]
    fn test_truncate_body_short_unchanged() {
        let input = "short response";
        assert_eq!(truncate_body(input), "short response");
    }

    #[test]
    fn test_truncate_body_whitespace_normalised() {
        let input = "line one\n\tline two\r\n  line  three";
        assert_eq!(truncate_body(input), "line one line two line three");
    }

    #[test]
    fn test_truncate_body_long_truncated() {
        let input = "a".repeat(300);
        let result = truncate_body(&input);
        assert!(result.ends_with("..."));
        // 200 chars + "..."
        assert!(result.len() <= 203);
    }

    #[test]
    fn test_truncate_body_multibyte_safe() {
        // £ is 2 bytes in UTF-8.
        let input = "£".repeat(300);
        let result = truncate_body(&input);
        assert!(result.ends_with("..."));
        // Must not panic on multi-byte boundary.
        assert!(result.len() <= 403); // 200 chars × 2 bytes + "..."
    }
}
