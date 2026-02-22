//! Ollama inference provider targeting the `/api/chat` endpoint.
//!
//! Implements [`InferenceProvider`] for local Ollama instances. The
//! provider sends non-streaming chat completion requests and maps
//! Ollama's response shape to [`CompletionResponse`].

use std::time::Instant;

use async_trait::async_trait;
use tracing::Instrument;
use tribal_domain::span_attrs;

use crate::{
    CompletionRequest, CompletionResponse, CompletionUsage, InferenceError, InferenceProvider,
    Message, ResponseFormat, Role,
    error::{map_body_read_error, map_http_error, map_json_parse_error, map_send_error},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PROVIDER_NAME: &str = "ollama";
const CHAT_PATH: &str = "/api/chat";

// ---------------------------------------------------------------------------
// Private serde types
// ---------------------------------------------------------------------------

// Targets Ollama /api/chat — tested against Ollama v0.6.x (Feb 2026).
// API reference: https://github.com/ollama/ollama/blob/main/docs/api.md

#[derive(serde::Serialize)]
struct OllamaChatRequest<'a> {
    model: &'a str,
    messages: Vec<OllamaChatMessage<'a>>,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    options: Option<OllamaChatOptions>,
}

#[derive(serde::Serialize)]
struct OllamaChatMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(serde::Serialize)]
struct OllamaChatOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    num_predict: Option<u32>,
}

#[derive(serde::Deserialize)]
struct OllamaChatResponse {
    message: OllamaChatResponseMessage,
    #[serde(default)]
    prompt_eval_count: Option<u32>,
    #[serde(default)]
    eval_count: Option<u32>,
    #[serde(default)]
    total_duration: Option<u64>,
    #[serde(default)]
    load_duration: Option<u64>,
}

#[derive(serde::Deserialize)]
struct OllamaChatResponseMessage {
    content: String,
}

// ---------------------------------------------------------------------------
// OllamaInferenceProvider
// ---------------------------------------------------------------------------

/// Concrete inference provider for Ollama's `/api/chat` endpoint.
///
/// Sends non-streaming chat completion requests and maps the response
/// to [`CompletionResponse`]. Ollama does not support prompt caching,
/// so cache token counts are always zero.
pub struct OllamaInferenceProvider {
    client: reqwest::Client,
    base_url: String,
    model: String,
}

impl OllamaInferenceProvider {
    /// Creates a new Ollama inference provider.
    pub fn new(
        client: reqwest::Client,
        base_url: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        let mut url = base_url.into();
        while url.ends_with('/') {
            url.pop();
        }

        Self {
            client,
            base_url: url,
            model: model.into(),
        }
    }

    /// Validates model availability by sending a trivial completion.
    ///
    /// Sends a best-effort GET to `/api/tags` to check whether the
    /// configured model is locally available, then sends a minimal
    /// completion request to verify the model can generate output.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::ProviderUnavailable`] if Ollama cannot
    /// be reached.  Returns [`InferenceError::LlmCallFailed`] if the
    /// model rejects the request.
    pub async fn probe_model(&self) -> Result<(), InferenceError> {
        let span = tracing::info_span!(
            "tribal.llm.probe",
            { span_attrs::LLM_PROVIDER } = PROVIDER_NAME,
            { span_attrs::LLM_MODEL } = %self.model,
        );

        async {
            super::tags::check_tags(&self.client, &self.base_url, &self.model).await;

            let request = CompletionRequest {
                system: None,
                messages: vec![Message {
                    role: Role::User,
                    content: "Respond with OK".to_owned(),
                }],
                temperature: Some(0.0),
                max_tokens: Some(8),
                response_format: None,
            };
            let _response = self.complete(request).await?;

            tracing::info!("model {} probe succeeded", self.model);
            Ok(())
        }
        .instrument(span)
        .await
    }
}

// ---------------------------------------------------------------------------
// InferenceProvider impl
// ---------------------------------------------------------------------------

#[async_trait]
impl InferenceProvider for OllamaInferenceProvider {
    async fn complete(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionResponse, InferenceError> {
        if request.messages.is_empty() {
            return Err(InferenceError::LlmCallFailed {
                model: self.model.clone(),
                context: "messages list is empty".to_owned(),
                source: None,
            });
        }

        let span = tracing::info_span!(
            "tribal.llm.call",
            { span_attrs::LLM_PROVIDER } = PROVIDER_NAME,
            { span_attrs::LLM_MODEL } = %self.model,
            { span_attrs::LLM_TOKENS_INPUT } = tracing::field::Empty,
            { span_attrs::LLM_TOKENS_OUTPUT } = tracing::field::Empty,
            { span_attrs::LLM_TOKENS_TOTAL } = tracing::field::Empty,
            { span_attrs::LLM_LATENCY_MS } = tracing::field::Empty,
            { span_attrs::LLM_TEMPERATURE } = tracing::field::Empty,
            { span_attrs::LLM_TOKENS_CACHE_READ } = tracing::field::Empty,
            { span_attrs::LLM_TOKENS_CACHE_WRITE } = tracing::field::Empty,
        );

        async {
            if let Some(temp) = request.temperature {
                tracing::Span::current().record(span_attrs::LLM_TEMPERATURE, f64::from(temp));
            }

            let started = Instant::now();

            let mut messages =
                Vec::with_capacity(request.messages.len() + usize::from(request.system.is_some()));

            if let Some(ref system) = request.system {
                messages.push(OllamaChatMessage {
                    role: "system",
                    content: system,
                });
            }

            for msg in &request.messages {
                messages.push(OllamaChatMessage {
                    role: role_str(msg.role),
                    content: &msg.content,
                });
            }

            let format = request.response_format.as_ref().map(map_response_format);

            let options = match (request.temperature, request.max_tokens) {
                (None, None) => None,
                (temp, max_tok) => Some(OllamaChatOptions {
                    temperature: temp,
                    num_predict: max_tok,
                }),
            };

            let body = OllamaChatRequest {
                model: &self.model,
                messages,
                stream: false,
                format,
                options,
            };

            let url = format!("{}{CHAT_PATH}", self.base_url);
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
                    |ctx| InferenceError::LlmCallFailed {
                        model: self.model.clone(),
                        context: ctx,
                        source: None,
                    },
                ));
            }

            let parsed: OllamaChatResponse = serde_json::from_str(&response_body).map_err(|e| {
                map_json_parse_error(&e, "OllamaChatResponse JSON object", &response_body)
            })?;

            let input_tokens = parsed.prompt_eval_count.unwrap_or_else(|| {
                tracing::debug!("prompt_eval_count absent, defaulting to 0");
                0
            });

            let output_tokens = parsed.eval_count.unwrap_or_else(|| {
                tracing::debug!("eval_count absent, defaulting to 0");
                0
            });

            let total_tokens = input_tokens + output_tokens;

            if let Some(total_ns) = parsed.total_duration {
                tracing::debug!(total_duration_ms = total_ns / 1_000_000, "provider timing");
            }
            if let Some(load_ns) = parsed.load_duration {
                tracing::debug!(load_duration_ms = load_ns / 1_000_000, "provider timing");
            }

            let latency_ms = u64::try_from(latency.as_millis()).unwrap_or(u64::MAX);

            let current = tracing::Span::current();
            current.record(span_attrs::LLM_TOKENS_INPUT, input_tokens);
            current.record(span_attrs::LLM_TOKENS_OUTPUT, output_tokens);
            current.record(span_attrs::LLM_TOKENS_TOTAL, total_tokens);
            current.record(span_attrs::LLM_LATENCY_MS, latency_ms);
            current.record(span_attrs::LLM_TOKENS_CACHE_READ, 0_u32);
            current.record(span_attrs::LLM_TOKENS_CACHE_WRITE, 0_u32);

            tracing::debug!(
                input_tokens,
                output_tokens,
                total_tokens,
                latency_ms,
                "completion generated",
            );

            Ok(CompletionResponse {
                text: parsed.message.content,
                usage: CompletionUsage {
                    provider: PROVIDER_NAME.to_owned(),
                    model: self.model.clone(),
                    input_tokens,
                    output_tokens,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
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
// Helpers
// ---------------------------------------------------------------------------

fn role_str(role: Role) -> &'static str {
    match role {
        Role::User => "user",
        Role::Assistant => "assistant",
    }
}

fn map_response_format(format: &ResponseFormat) -> serde_json::Value {
    match format {
        ResponseFormat::Json => serde_json::Value::String("json".to_owned()),
        ResponseFormat::JsonSchema { schema } => schema.clone(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, method, path},
    };

    use super::*;
    use crate::ollama::tags::TAGS_PATH;

    fn a_request(content: &str) -> CompletionRequest {
        CompletionRequest {
            system: None,
            messages: vec![Message {
                role: Role::User,
                content: content.to_owned(),
            }],
            temperature: None,
            max_tokens: None,
            response_format: None,
        }
    }

    fn a_valid_response_json() -> serde_json::Value {
        serde_json::json!({
            "message": { "role": "assistant", "content": "Hello!" },
            "prompt_eval_count": 10,
            "eval_count": 5,
            "total_duration": 200_000_000_u64,
            "load_duration": 20_000_000_u64,
        })
    }

    fn setup(server: &MockServer) -> OllamaInferenceProvider {
        OllamaInferenceProvider::new(reqwest::Client::new(), server.uri(), "llama3.2:3b")
    }

    // -- Constructor ---------------------------------------------------------

    #[tokio::test]
    async fn test_chat_trailing_slash_normalised() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let url_with_slash = format!("{}/", server.uri());
        let provider =
            OllamaInferenceProvider::new(reqwest::Client::new(), url_with_slash, "llama3.2:3b");

        provider.complete(a_request("test")).await.unwrap();
    }

    // -- Happy path ----------------------------------------------------------

    #[tokio::test]
    async fn test_chat_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let response = provider.complete(a_request("test input")).await.unwrap();

        assert_eq!(response.text, "Hello!");
        assert_eq!(response.usage.provider, PROVIDER_NAME);
        assert_eq!(response.usage.model, "llama3.2:3b");
        assert_eq!(response.usage.input_tokens, 10);
        assert_eq!(response.usage.output_tokens, 5);
        assert_eq!(response.usage.total_tokens, 15);
        assert_eq!(response.usage.cache_read_tokens, 0);
        assert_eq!(response.usage.cache_write_tokens, 0);
        assert!(response.usage.latency > Duration::ZERO);
    }

    #[tokio::test]
    async fn test_chat_sends_correct_request_body() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "llama3.2:3b",
                "messages": [{"role": "user", "content": "hello world"}],
                "stream": false,
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let _ = provider.complete(a_request("hello world")).await.unwrap();
    }

    #[tokio::test]
    async fn test_chat_sends_system_message() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "llama3.2:3b",
                "messages": [
                    {"role": "system", "content": "You are helpful."},
                    {"role": "user", "content": "hi"},
                ],
                "stream": false,
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let request = CompletionRequest {
            system: Some("You are helpful.".to_owned()),
            messages: vec![Message {
                role: Role::User,
                content: "hi".to_owned(),
            }],
            temperature: None,
            max_tokens: None,
            response_format: None,
        };
        let _ = provider.complete(request).await.unwrap();
    }

    #[tokio::test]
    async fn test_chat_sends_response_format_json() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "llama3.2:3b",
                "messages": [{"role": "user", "content": "test"}],
                "stream": false,
                "format": "json",
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let request = CompletionRequest {
            system: None,
            messages: vec![Message {
                role: Role::User,
                content: "test".to_owned(),
            }],
            temperature: None,
            max_tokens: None,
            response_format: Some(ResponseFormat::Json),
        };
        let _ = provider.complete(request).await.unwrap();
    }

    #[tokio::test]
    async fn test_chat_sends_response_format_json_schema() {
        let server = MockServer::start().await;
        let schema =
            serde_json::json!({"type": "object", "properties": {"name": {"type": "string"}}});

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "llama3.2:3b",
                "messages": [{"role": "user", "content": "test"}],
                "stream": false,
                "format": schema.clone(),
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let request = CompletionRequest {
            system: None,
            messages: vec![Message {
                role: Role::User,
                content: "test".to_owned(),
            }],
            temperature: None,
            max_tokens: None,
            response_format: Some(ResponseFormat::JsonSchema { schema }),
        };
        let _ = provider.complete(request).await.unwrap();
    }

    // -- Options serialisation -----------------------------------------------

    #[tokio::test]
    async fn test_chat_sends_options_both_set() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "llama3.2:3b",
                "messages": [{"role": "user", "content": "test"}],
                "stream": false,
                "options": {"temperature": 0.7, "num_predict": 100},
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let request = CompletionRequest {
            system: None,
            messages: vec![Message {
                role: Role::User,
                content: "test".to_owned(),
            }],
            temperature: Some(0.7),
            max_tokens: Some(100),
            response_format: None,
        };
        let _ = provider.complete(request).await.unwrap();
    }

    #[tokio::test]
    async fn test_chat_sends_options_temperature_only() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "llama3.2:3b",
                "messages": [{"role": "user", "content": "test"}],
                "stream": false,
                "options": {"temperature": 0.5},
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let request = CompletionRequest {
            system: None,
            messages: vec![Message {
                role: Role::User,
                content: "test".to_owned(),
            }],
            temperature: Some(0.5),
            max_tokens: None,
            response_format: None,
        };
        let _ = provider.complete(request).await.unwrap();
    }

    #[tokio::test]
    async fn test_chat_sends_options_max_tokens_only() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "llama3.2:3b",
                "messages": [{"role": "user", "content": "test"}],
                "stream": false,
                "options": {"num_predict": 200},
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let request = CompletionRequest {
            system: None,
            messages: vec![Message {
                role: Role::User,
                content: "test".to_owned(),
            }],
            temperature: None,
            max_tokens: Some(200),
            response_format: None,
        };
        let _ = provider.complete(request).await.unwrap();
    }

    #[tokio::test]
    async fn test_chat_omits_options_when_both_none() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "llama3.2:3b",
                "messages": [{"role": "user", "content": "test"}],
                "stream": false,
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let _ = provider.complete(a_request("test")).await.unwrap();
    }

    // -- Input validation ----------------------------------------------------

    #[tokio::test]
    async fn test_chat_empty_messages_returns_llm_call_failed() {
        let server = MockServer::start().await;
        let provider = setup(&server);

        let request = CompletionRequest {
            system: None,
            messages: vec![],
            temperature: None,
            max_tokens: None,
            response_format: None,
        };
        let err = provider.complete(request).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::LlmCallFailed {
                ref model,
                ref context,
                ..
            }
            if model == "llama3.2:3b"
                && context == "messages list is empty"
        ));
    }

    // -- Network errors ------------------------------------------------------

    #[tokio::test]
    async fn test_chat_connection_refused_returns_provider_unavailable() {
        let provider =
            OllamaInferenceProvider::new(reqwest::Client::new(), "http://127.0.0.1:1", "model");

        let err = provider.complete(a_request("test")).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref provider, .. }
            if provider == PROVIDER_NAME
        ));
    }

    #[tokio::test]
    async fn test_chat_timeout_returns_provider_unavailable() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(a_valid_response_json())
                    .set_delay(Duration::from_secs(30)),
            )
            .mount(&server)
            .await;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(50))
            .build()
            .unwrap();

        let provider = OllamaInferenceProvider::new(client, server.uri(), "model");

        let err = provider.complete(a_request("test")).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref provider, .. }
            if provider == PROVIDER_NAME
        ));
    }

    // -- HTTP error mapping --------------------------------------------------

    #[tokio::test]
    async fn test_chat_http_500_returns_provider_unavailable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let err = provider.complete(a_request("test")).await.unwrap_err();

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
    async fn test_chat_http_429_returns_provider_unavailable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let err = provider.complete(a_request("test")).await.unwrap_err();

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
    async fn test_chat_http_400_returns_llm_call_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let err = provider.complete(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::LlmCallFailed { ref context, .. }
                if context.contains("400") && context.contains("bad request")
            ),
            "expected LlmCallFailed with status and body, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_chat_http_404_returns_llm_call_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(serde_json::json!({"error": "model not found"})),
            )
            .mount(&server)
            .await;

        let provider = setup(&server);
        let err = provider.complete(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::LlmCallFailed { ref context, .. }
                if context.contains("404")
            ),
            "expected LlmCallFailed with 404 status, got {err:?}"
        );
    }

    // -- Response parsing ----------------------------------------------------

    #[tokio::test]
    async fn test_chat_malformed_json_returns_response_parse_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json at all"))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let err = provider.complete(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::ResponseParseFailed {
                    ref expected_shape,
                    ref actual,
                }
                if expected_shape == "OllamaChatResponse JSON object"
                    && actual.starts_with("invalid JSON:")
            ),
            "expected ResponseParseFailed with shape and JSON error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_chat_missing_prompt_eval_count_defaults_to_zero() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "message": {"role": "assistant", "content": "OK"},
                "eval_count": 3,
            })))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let response = provider.complete(a_request("test")).await.unwrap();

        assert_eq!(response.usage.input_tokens, 0);
        assert_eq!(response.usage.output_tokens, 3);
        assert_eq!(response.usage.total_tokens, 3);
    }

    #[tokio::test]
    async fn test_chat_missing_eval_count_defaults_to_zero() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "message": {"role": "assistant", "content": "OK"},
                "prompt_eval_count": 8,
            })))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let response = provider.complete(a_request("test")).await.unwrap();

        assert_eq!(response.usage.input_tokens, 8);
        assert_eq!(response.usage.output_tokens, 0);
        assert_eq!(response.usage.total_tokens, 8);
    }

    // -- Response body truncation --------------------------------------------

    #[tokio::test]
    async fn test_chat_response_body_truncated_in_error_context() {
        let server = MockServer::start().await;
        let long_body = "x".repeat(500);
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(500).set_body_string(&long_body))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let err = provider.complete(a_request("test")).await.unwrap_err();

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

        Mock::given(method("GET"))
            .and(path(TAGS_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [{"name": "llama3.2:3b"}],
            })))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .mount(&server)
            .await;

        let provider = setup(&server);
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
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .mount(&server)
            .await;

        let provider = setup(&server);
        provider.probe_model().await.unwrap();
    }

    #[tokio::test]
    async fn test_probe_model_completion_failure_propagates() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path(TAGS_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [],
            })))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(500).set_body_string("server error"))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let err = provider.probe_model().await.unwrap_err();
        assert!(
            matches!(err, InferenceError::ProviderUnavailable { .. }),
            "expected ProviderUnavailable, got {err:?}"
        );
    }
}
