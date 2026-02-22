//! Anthropic inference provider targeting the `/v1/messages` endpoint.
//!
//! Implements [`InferenceProvider`] for the Anthropic Messages API. The
//! provider sends non-streaming chat completion requests and maps the
//! response to [`CompletionResponse`]. Anthropic supports prompt
//! caching, so cache token counts are populated from usage fields.

use std::time::Instant;

use async_trait::async_trait;
use tracing::Instrument;
use tribal_domain::span_attrs;

use crate::{
    CompletionRequest, CompletionResponse, CompletionUsage, InferenceError, InferenceProvider,
    Message, ResponseFormat, Role,
    error::{map_body_read_error, map_http_error, map_json_parse_error, map_send_error},
    http::{PROBE_MAX_TOKENS, normalise_base_url, record_completion_usage},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PROVIDER_NAME: &str = "anthropic";
const PROBE_INPUT: &str = "Respond with OK";
const MESSAGES_PATH: &str = "/v1/messages";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const DEFAULT_MAX_TOKENS: u32 = 4096;

// ---------------------------------------------------------------------------
// Private serde types
// ---------------------------------------------------------------------------

// Targets Anthropic /v1/messages — tested against anthropic-version 2023-06-01 (Feb 2026).

#[derive(serde::Serialize)]
struct AnthropicChatRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<&'a str>,
    messages: Vec<AnthropicMessage<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_config: Option<AnthropicOutputConfig>,
}

#[derive(serde::Serialize)]
struct AnthropicMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(serde::Serialize)]
struct AnthropicOutputConfig {
    format: AnthropicOutputFormat,
}

#[derive(serde::Serialize)]
#[serde(tag = "type")]
enum AnthropicOutputFormat {
    #[serde(rename = "json_schema")]
    JsonSchema { schema: serde_json::Value },
}

#[derive(serde::Deserialize)]
struct AnthropicChatResponse {
    content: Vec<AnthropicContentBlock>,
    usage: AnthropicUsage,
}

#[derive(serde::Deserialize)]
#[serde(tag = "type")]
enum AnthropicContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(other)]
    Other,
}

#[derive(serde::Deserialize)]
#[allow(clippy::struct_field_names)] // Matches Anthropic API field names.
struct AnthropicUsage {
    input_tokens: u32,
    output_tokens: u32,
    #[serde(default)]
    cache_creation_input_tokens: Option<u32>,
    #[serde(default)]
    cache_read_input_tokens: Option<u32>,
}

// ---------------------------------------------------------------------------
// AnthropicInferenceProvider
// ---------------------------------------------------------------------------

/// Concrete inference provider for Anthropic's `/v1/messages` endpoint.
///
/// Sends non-streaming chat completion requests and maps the response
/// to [`CompletionResponse`]. Anthropic supports prompt caching;
/// `cache_read_tokens` and `cache_write_tokens` are populated from
/// the response usage fields.
///
/// Authentication is set per-request via the `x-api-key` header and
/// the `anthropic-version` header — the shared [`reqwest::Client`] is
/// not pre-configured with credentials.
pub struct AnthropicInferenceProvider {
    client: reqwest::Client,
    base_url: String,
    model: String,
    api_key: String,
}

impl AnthropicInferenceProvider {
    /// Creates a new Anthropic inference provider.
    pub fn new(
        client: reqwest::Client,
        base_url: impl Into<String>,
        model: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            client,
            base_url: normalise_base_url(base_url),
            model: model.into(),
            api_key: api_key.into(),
        }
    }

    /// Validates API key and model access by sending a trivial completion.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::ProviderUnavailable`] if the provider
    /// cannot be reached.  Returns [`InferenceError::LlmCallFailed`] if
    /// the API key or model is rejected.
    pub async fn probe_model(&self) -> Result<(), InferenceError> {
        let span = tracing::info_span!(
            "tribal.llm.probe",
            { span_attrs::LLM_PROVIDER } = PROVIDER_NAME,
            { span_attrs::LLM_MODEL } = %self.model,
        );

        async {
            let request = CompletionRequest {
                system: None,
                messages: vec![Message {
                    role: Role::User,
                    content: PROBE_INPUT.to_owned(),
                }],
                temperature: Some(0.0),
                max_tokens: Some(PROBE_MAX_TOKENS),
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
impl InferenceProvider for AnthropicInferenceProvider {
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
            let body = build_request(&self.model, &request);
            let url = format!("{}{MESSAGES_PATH}", self.base_url);
            let http_response = self
                .client
                .post(&url)
                .header("x-api-key", &self.api_key)
                .header("anthropic-version", ANTHROPIC_VERSION)
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
                    |ctx| InferenceError::LlmCallFailed {
                        model: self.model.clone(),
                        context: ctx,
                        source: None,
                    },
                ));
            }

            let parsed: AnthropicChatResponse =
                serde_json::from_str(&response_body).map_err(|e| {
                    map_json_parse_error(&e, "AnthropicChatResponse JSON object", &response_body)
                })?;

            let text = extract_text_content(&parsed.content)?;

            let input_tokens = parsed.usage.input_tokens;
            let output_tokens = parsed.usage.output_tokens;

            let usage = CompletionUsage {
                provider: PROVIDER_NAME.to_owned(),
                model: self.model.clone(),
                input_tokens,
                output_tokens,
                cache_read_tokens: parsed.usage.cache_read_input_tokens.unwrap_or(0),
                cache_write_tokens: parsed.usage.cache_creation_input_tokens.unwrap_or(0),
                total_tokens: input_tokens.saturating_add(output_tokens),
                latency,
            };
            record_completion_usage(&usage);

            Ok(CompletionResponse { text, usage })
        }
        .instrument(span)
        .await
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_request<'a>(model: &'a str, request: &'a CompletionRequest) -> AnthropicChatRequest<'a> {
    let messages: Vec<AnthropicMessage<'_>> = request
        .messages
        .iter()
        .map(|msg| AnthropicMessage {
            role: msg.role.as_str(),
            content: &msg.content,
        })
        .collect();

    let max_tokens = request.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS);

    let output_config = request
        .response_format
        .as_ref()
        .and_then(map_response_format);

    AnthropicChatRequest {
        model,
        max_tokens,
        system: request.system.as_deref(),
        messages,
        temperature: request.temperature,
        output_config,
    }
}

fn map_response_format(format: &ResponseFormat) -> Option<AnthropicOutputConfig> {
    match format {
        ResponseFormat::Json => {
            tracing::debug!(
                "ResponseFormat::Json has no native Anthropic equivalent, \
                 relying on prompt instructions"
            );
            None
        }
        ResponseFormat::JsonSchema { schema } => Some(AnthropicOutputConfig {
            format: AnthropicOutputFormat::JsonSchema {
                schema: schema.clone(),
            },
        }),
    }
}

/// Extracts and concatenates all text content blocks from an Anthropic
/// response.
fn extract_text_content(content: &[AnthropicContentBlock]) -> Result<String, InferenceError> {
    let mut text = String::new();
    let mut text_block_count: usize = 0;
    for block in content {
        if let AnthropicContentBlock::Text { text: t } = block {
            text.push_str(t);
            text_block_count += 1;
        }
    }

    if text_block_count == 0 {
        return Err(InferenceError::ResponseParseFailed {
            expected_shape: "at least one text content block".to_owned(),
            actual: "0 text blocks in response".to_owned(),
        });
    }

    Ok(text)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, header, method, path},
    };

    use super::*;

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
            "id": "msg_test",
            "type": "message",
            "role": "assistant",
            "content": [{
                "type": "text",
                "text": "Hello!",
            }],
            "model": "claude-haiku-4-5-20251001",
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0,
            },
        })
    }

    fn setup(server: &MockServer) -> AnthropicInferenceProvider {
        AnthropicInferenceProvider::new(
            reqwest::Client::new(),
            server.uri(),
            "claude-haiku-4-5-20251001",
            "test-key",
        )
    }

    // -- Constructor ---------------------------------------------------------

    #[tokio::test]
    async fn test_chat_trailing_slash_normalised() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let url_with_slash = format!("{}/", server.uri());
        let provider = AnthropicInferenceProvider::new(
            reqwest::Client::new(),
            url_with_slash,
            "claude-haiku-4-5-20251001",
            "test-key",
        );

        provider.complete(a_request("test")).await.unwrap();
    }

    // -- Happy path ----------------------------------------------------------

    #[tokio::test]
    async fn test_chat_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let response = provider.complete(a_request("test input")).await.unwrap();

        assert_eq!(response.text, "Hello!");
        assert_eq!(response.usage.provider, PROVIDER_NAME);
        assert_eq!(response.usage.model, "claude-haiku-4-5-20251001");
        assert_eq!(response.usage.input_tokens, 10);
        assert_eq!(response.usage.output_tokens, 5);
        assert_eq!(response.usage.total_tokens, 15);
        assert_eq!(response.usage.cache_read_tokens, 0);
        assert_eq!(response.usage.cache_write_tokens, 0);
    }

    #[tokio::test]
    async fn test_chat_sends_correct_request_body() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .and(body_json(serde_json::json!({
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": DEFAULT_MAX_TOKENS,
                "messages": [{"role": "user", "content": "hello world"}],
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let _ = provider.complete(a_request("hello world")).await.unwrap();
    }

    // -- Auth headers --------------------------------------------------------

    #[tokio::test]
    async fn test_chat_sends_api_key_and_version_headers() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .and(header("x-api-key", "test-key"))
            .and(header("anthropic-version", ANTHROPIC_VERSION))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let _ = provider.complete(a_request("test")).await.unwrap();
    }

    // -- System prompt -------------------------------------------------------

    #[tokio::test]
    async fn test_chat_sends_system_as_top_level_field() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .and(body_json(serde_json::json!({
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": DEFAULT_MAX_TOKENS,
                "system": "You are helpful.",
                "messages": [{"role": "user", "content": "hi"}],
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

    // -- max_tokens default --------------------------------------------------

    #[tokio::test]
    async fn test_chat_default_max_tokens_when_none() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .and(body_json(serde_json::json!({
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": DEFAULT_MAX_TOKENS,
                "messages": [{"role": "user", "content": "test"}],
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let _ = provider.complete(a_request("test")).await.unwrap();
    }

    #[tokio::test]
    async fn test_chat_explicit_max_tokens_used_when_set() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .and(body_json(serde_json::json!({
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 100,
                "messages": [{"role": "user", "content": "test"}],
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
            max_tokens: Some(100),
            response_format: None,
        };
        let _ = provider.complete(request).await.unwrap();
    }

    // -- Response format -----------------------------------------------------

    #[tokio::test]
    async fn test_chat_sends_output_config_for_json_schema() {
        let server = MockServer::start().await;
        let schema =
            serde_json::json!({"type": "object", "properties": {"name": {"type": "string"}}});

        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .and(body_json(serde_json::json!({
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": DEFAULT_MAX_TOKENS,
                "messages": [{"role": "user", "content": "test"}],
                "output_config": {
                    "format": {
                        "type": "json_schema",
                        "schema": schema.clone(),
                    },
                },
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

    #[tokio::test]
    async fn test_chat_omits_output_config_for_json() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .and(body_json(serde_json::json!({
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": DEFAULT_MAX_TOKENS,
                "messages": [{"role": "user", "content": "test"}],
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
    async fn test_chat_omits_output_config_for_none() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .and(body_json(serde_json::json!({
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": DEFAULT_MAX_TOKENS,
                "messages": [{"role": "user", "content": "test"}],
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let _ = provider.complete(a_request("test")).await.unwrap();
    }

    // -- Content block extraction --------------------------------------------

    #[tokio::test]
    async fn test_chat_extracts_text_from_single_block() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "text", "text": "single block"}],
                "model": "claude-haiku-4-5-20251001",
                "stop_reason": "end_turn",
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 5,
                },
            })))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let response = provider.complete(a_request("test")).await.unwrap();
        assert_eq!(response.text, "single block");
    }

    #[tokio::test]
    async fn test_chat_concatenates_multiple_text_blocks() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "first"},
                    {"type": "text", "text": "second"},
                ],
                "model": "claude-haiku-4-5-20251001",
                "stop_reason": "end_turn",
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 5,
                },
            })))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let response = provider.complete(a_request("test")).await.unwrap();
        assert_eq!(response.text, "firstsecond");
    }

    #[tokio::test]
    async fn test_chat_ignores_non_text_blocks() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "toolu_01abc123", "name": "search", "input": {}},
                    {"type": "text", "text": "result"},
                ],
                "model": "claude-haiku-4-5-20251001",
                "stop_reason": "end_turn",
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 5,
                },
            })))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let response = provider.complete(a_request("test")).await.unwrap();
        assert_eq!(response.text, "result");
    }

    #[tokio::test]
    async fn test_chat_no_text_blocks_returns_response_parse_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "toolu_01abc123", "name": "search", "input": {}},
                ],
                "model": "claude-haiku-4-5-20251001",
                "stop_reason": "tool_use",
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 5,
                },
            })))
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
                if expected_shape == "at least one text content block"
                    && actual == "0 text blocks in response"
            ),
            "expected ResponseParseFailed for no text blocks, got {err:?}"
        );
    }

    // -- Cache tokens --------------------------------------------------------

    #[tokio::test]
    async fn test_chat_maps_cache_tokens_from_usage() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "text", "text": "cached"}],
                "model": "claude-haiku-4-5-20251001",
                "stop_reason": "end_turn",
                "usage": {
                    "input_tokens": 200,
                    "output_tokens": 50,
                    "cache_creation_input_tokens": 150,
                    "cache_read_input_tokens": 100,
                },
            })))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let response = provider.complete(a_request("test")).await.unwrap();

        assert_eq!(response.usage.cache_write_tokens, 150);
        assert_eq!(response.usage.cache_read_tokens, 100);
        assert_eq!(response.usage.input_tokens, 200);
        assert_eq!(response.usage.output_tokens, 50);
        assert_eq!(response.usage.total_tokens, 250);
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
            if model == "claude-haiku-4-5-20251001"
                && context == "messages list is empty"
        ));
    }

    // -- Network errors ------------------------------------------------------

    #[tokio::test]
    async fn test_chat_connection_refused_returns_provider_unavailable() {
        let provider = AnthropicInferenceProvider::new(
            reqwest::Client::new(),
            "http://127.0.0.1:1",
            "model",
            "key",
        );

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
            .and(path(MESSAGES_PATH))
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

        let provider = AnthropicInferenceProvider::new(client, server.uri(), "model", "key");

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
            .and(path(MESSAGES_PATH))
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
            .and(path(MESSAGES_PATH))
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
    async fn test_chat_http_529_returns_provider_unavailable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(ResponseTemplate::new(529).set_body_string("overloaded"))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let err = provider.complete(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::ProviderUnavailable { ref reason, .. }
                if reason.contains("529")
            ),
            "expected ProviderUnavailable with 529 status, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_chat_http_400_returns_llm_call_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
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
            .and(path(MESSAGES_PATH))
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

    #[tokio::test]
    async fn test_chat_http_401_returns_llm_call_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(ResponseTemplate::new(401).set_body_string("unauthorised"))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let err = provider.complete(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::LlmCallFailed { ref context, .. }
                if context.contains("401")
            ),
            "expected LlmCallFailed with 401 status, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_chat_http_403_returns_llm_call_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let err = provider.complete(a_request("test")).await.unwrap_err();

        assert!(
            matches!(
                err,
                InferenceError::LlmCallFailed { ref context, .. }
                if context.contains("403")
            ),
            "expected LlmCallFailed with 403 status, got {err:?}"
        );
    }

    // -- 429 Retry-After -----------------------------------------------------

    #[tokio::test]
    async fn test_chat_http_429_includes_retry_after_in_context() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(
                ResponseTemplate::new(429)
                    .set_body_string("rate limited")
                    .append_header("Retry-After", "30"),
            )
            .mount(&server)
            .await;

        let provider = setup(&server);
        let err = provider.complete(a_request("test")).await.unwrap_err();

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
    async fn test_chat_malformed_json_returns_response_parse_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
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
                if expected_shape == "AnthropicChatResponse JSON object"
                    && actual.starts_with("invalid JSON:")
            ),
            "expected ResponseParseFailed with shape and JSON error, got {err:?}"
        );
    }

    // -- Response body truncation --------------------------------------------

    #[tokio::test]
    async fn test_chat_response_body_truncated_in_error_context() {
        let server = MockServer::start().await;
        let long_body = "x".repeat(500);
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
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

        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .mount(&server)
            .await;

        let provider = setup(&server);
        provider.probe_model().await.unwrap();
    }

    #[tokio::test]
    async fn test_probe_model_completion_failure_propagates() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
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

    #[tokio::test]
    async fn test_probe_model_auth_401_returns_llm_call_failed() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(ResponseTemplate::new(401).set_body_string("invalid api key"))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let err = provider.probe_model().await.unwrap_err();
        assert!(
            matches!(
                err,
                InferenceError::LlmCallFailed { ref context, .. }
                if context.contains("401")
            ),
            "expected LlmCallFailed with 401 status, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_probe_model_auth_403_returns_llm_call_failed() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
            .mount(&server)
            .await;

        let provider = setup(&server);
        let err = provider.probe_model().await.unwrap_err();
        assert!(
            matches!(
                err,
                InferenceError::LlmCallFailed { ref context, .. }
                if context.contains("403")
            ),
            "expected LlmCallFailed with 403 status, got {err:?}"
        );
    }
}
