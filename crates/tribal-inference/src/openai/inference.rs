//! `OpenAI` inference provider targeting the `/v1/chat/completions` endpoint.
//!
//! Implements [`InferenceProvider`] for `OpenAI` and `OpenAI`-compatible
//! runtimes (`vLLM`, `LM Studio`, `LocalAI`, `LiteLLM`). The provider sends
//! non-streaming chat completion requests and maps the response to
//! [`CompletionResponse`].

use std::time::Instant;

use async_trait::async_trait;
use tracing::Instrument;
use tribal_domain::{ProviderKind, span_attrs};

use crate::{
    CompletionRequest, CompletionResponse, CompletionUsage, InferenceError, InferenceProvider,
    Message, ProviderIdentity, ResponseFormat, Role, apply_dialect,
    capabilities::{MaxOutputTokensParam, StructuredOutputMode, reconcile_temperature, resolve},
    error::{map_body_read_error, map_http_error, map_json_parse_error, map_send_error},
    http::{INFERENCE_PROBE_INPUT, PROBE_MAX_TOKENS, normalise_base_url, record_completion_usage},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PROVIDER_NAME: &str = "openai";
pub const CHAT_PATH: &str = "/v1/chat/completions";

/// Logged at debug when the output-token cap is sent as `max_completion_tokens`
/// instead of `max_tokens`, which the resolved reasoning model rejects. A
/// lossless, expected translation, so it is a debug detail rather than a
/// warning.
const TOKEN_CAP_RENAMED: &str =
    "sending output-token cap as max_completion_tokens: target model rejects max_tokens";

// ---------------------------------------------------------------------------
// Private serde types
// ---------------------------------------------------------------------------

// Targets OpenAI /v1/chat/completions — tested against OpenAI API (Feb 2026).

#[derive(serde::Serialize)]
struct OpenAiChatRequest<'a> {
    model: &'a str,
    messages: Vec<OpenAiChatMessage<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_completion_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<serde_json::Value>,
}

#[derive(serde::Serialize)]
struct OpenAiChatMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(serde::Deserialize)]
struct OpenAiChatResponse {
    choices: Vec<OpenAiChoice>,
    usage: OpenAiChatUsage,
}

#[derive(serde::Deserialize)]
struct OpenAiChoice {
    message: OpenAiChoiceMessage,
}

#[derive(serde::Deserialize)]
struct OpenAiChoiceMessage {
    content: Option<String>,
}

#[derive(serde::Deserialize)]
#[allow(clippy::struct_field_names)] // Matches OpenAI API field names.
struct OpenAiChatUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
}

// ---------------------------------------------------------------------------
// OpenAiInferenceProvider
// ---------------------------------------------------------------------------

/// Concrete inference provider for `OpenAI`'s `/v1/chat/completions` endpoint.
///
/// Compatible with any `OpenAI`-compatible runtime. Sends non-streaming
/// chat completion requests and maps the response to
/// [`CompletionResponse`]. `OpenAI` does not expose prompt caching via
/// this endpoint, so cache token counts are always zero.
///
/// Authentication is set per-request via the `Authorization: Bearer`
/// header — the shared [`reqwest::Client`] is not pre-configured with
/// credentials.
pub struct OpenAiInferenceProvider {
    client: reqwest::Client,
    base_url: String,
    identity: ProviderIdentity,
    api_key: String,
}

impl OpenAiInferenceProvider {
    /// Creates a new `OpenAI` inference provider.
    pub fn new(
        client: reqwest::Client,
        base_url: impl Into<String>,
        model: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            client,
            base_url: normalise_base_url(base_url),
            identity: ProviderIdentity {
                name: PROVIDER_NAME.to_owned(),
                model: model.into(),
            },
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
            { span_attrs::LLM_MODEL } = %self.identity.model,
        );

        async {
            let request = CompletionRequest {
                system: None,
                messages: vec![Message {
                    role: Role::User,
                    content: INFERENCE_PROBE_INPUT.to_owned(),
                }],
                temperature: Some(0.0),
                max_tokens: Some(PROBE_MAX_TOKENS),
                response_format: None,
            };
            let _response = self.complete(request).await?;

            tracing::info!("model {} probe succeeded", self.identity.model);
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
impl InferenceProvider for OpenAiInferenceProvider {
    fn identity(&self) -> &ProviderIdentity {
        &self.identity
    }

    async fn complete(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionResponse, InferenceError> {
        if request.messages.is_empty() {
            return Err(InferenceError::LlmCallFailed {
                model: self.identity.model.clone(),
                context: "messages list is empty".to_owned(),
                source: None,
            });
        }

        let span = tracing::info_span!(
            "tribal.llm.call",
            { span_attrs::LLM_PROVIDER } = PROVIDER_NAME,
            { span_attrs::LLM_MODEL } = %self.identity.model,
            { span_attrs::LLM_TOKENS_INPUT } = tracing::field::Empty,
            { span_attrs::LLM_TOKENS_OUTPUT } = tracing::field::Empty,
            { span_attrs::LLM_TOKENS_TOTAL } = tracing::field::Empty,
            { span_attrs::LLM_LATENCY_MS } = tracing::field::Empty,
            { span_attrs::LLM_TEMPERATURE } = tracing::field::Empty,
            { span_attrs::LLM_TOKENS_CACHE_READ } = tracing::field::Empty,
            { span_attrs::LLM_TOKENS_CACHE_WRITE } = tracing::field::Empty,
        );

        async {
            let started = Instant::now();
            let body = build_request(&self.identity.model, &request);
            // Record the effective (post-reconcile) temperature, which the
            // capability layer may have dropped, so the span matches the wire.
            if let Some(temperature) = body.temperature {
                tracing::Span::current()
                    .record(span_attrs::LLM_TEMPERATURE, f64::from(temperature));
            }
            let url = format!("{}{CHAT_PATH}", self.base_url);
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
                    |ctx| InferenceError::LlmCallFailed {
                        model: self.identity.model.clone(),
                        context: ctx,
                        source: None,
                    },
                ));
            }

            let parsed: OpenAiChatResponse = serde_json::from_str(&response_body).map_err(|e| {
                map_json_parse_error(&e, "OpenAiChatResponse JSON object", &response_body)
            })?;

            let choice =
                parsed
                    .choices
                    .first()
                    .ok_or_else(|| InferenceError::ResponseParseFailed {
                        expected_shape: "choices[0] present".to_owned(),
                        actual: "choices array is empty".to_owned(),
                    })?;

            let text = choice.message.content.clone().ok_or_else(|| {
                InferenceError::ResponseParseFailed {
                    expected_shape: "choices[0].message.content present".to_owned(),
                    actual: "choices[0].message.content is null".to_owned(),
                }
            })?;

            let usage = CompletionUsage {
                provider: PROVIDER_NAME.to_owned(),
                model: self.identity.model.clone(),
                input_tokens: parsed.usage.prompt_tokens,
                output_tokens: parsed.usage.completion_tokens,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
                total_tokens: parsed.usage.total_tokens,
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

fn build_request<'a>(model: &'a str, request: &'a CompletionRequest) -> OpenAiChatRequest<'a> {
    let mut messages =
        Vec::with_capacity(request.messages.len() + usize::from(request.system.is_some()));

    if let Some(ref system) = request.system {
        messages.push(OpenAiChatMessage {
            role: "system",
            content: system,
        });
    }

    for msg in &request.messages {
        messages.push(OpenAiChatMessage {
            role: msg.role.as_str(),
            content: &msg.content,
        });
    }

    // -- Capability reconciliation --
    let caps = resolve(ProviderKind::OpenAi, model);
    let response_format = request
        .response_format
        .as_ref()
        .map(|format| map_response_format(format, caps.structured_output_mode));
    let temperature = reconcile_temperature(caps.sampling, request.temperature, model);
    let (max_tokens, max_completion_tokens) = match caps.max_output_tokens_param {
        MaxOutputTokensParam::MaxTokens => (request.max_tokens, None),
        MaxOutputTokensParam::MaxCompletionTokens => {
            if let Some(max_completion_tokens) = request.max_tokens {
                tracing::debug!(model, max_completion_tokens, "{TOKEN_CAP_RENAMED}");
            }
            (None, request.max_tokens)
        }
    };

    OpenAiChatRequest {
        model,
        messages,
        temperature,
        max_tokens,
        max_completion_tokens,
        response_format,
    }
}

/// Maps a [`ResponseFormat`] to the `OpenAI` `response_format` envelope. A
/// schema is normalised into the strict dialect and marked `strict` per the
/// resolved structured-output mode.
fn map_response_format(
    format: &ResponseFormat,
    structured_output_mode: StructuredOutputMode,
) -> serde_json::Value {
    match format {
        ResponseFormat::Json => serde_json::json!({"type": "json_object"}),
        ResponseFormat::JsonSchema { schema } => serde_json::json!({
            "type": "json_schema",
            "json_schema": {
                "name": "response",
                "schema": apply_dialect(ProviderKind::OpenAi, schema.clone()),
                "strict": structured_output_mode.requires_strict(),
            },
        }),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

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
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "model": "gpt-4o-mini",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello!",
                },
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15,
            },
        })
    }

    fn setup(server: &MockServer) -> OpenAiInferenceProvider {
        OpenAiInferenceProvider::new(
            reqwest::Client::new(),
            server.uri(),
            "gpt-4o-mini",
            "test-key",
        )
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
        let provider = OpenAiInferenceProvider::new(
            reqwest::Client::new(),
            url_with_slash,
            "gpt-4o-mini",
            "test-key",
        );

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
        assert_eq!(response.usage.model, "gpt-4o-mini");
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
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": "hello world"}],
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
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": "You are helpful."},
                    {"role": "user", "content": "hi"},
                ],
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

    // -- Auth headers --------------------------------------------------------

    #[tokio::test]
    async fn test_chat_sends_bearer_auth_header() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(header("Authorization", "Bearer test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let _ = provider.complete(a_request("test")).await.unwrap();
    }

    // -- Response format -----------------------------------------------------

    #[tokio::test]
    async fn test_chat_sends_response_format_json() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": "test"}],
                "response_format": {"type": "json_object"},
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
        let schema = serde_json::json!({
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
        });
        // The wire schema is the strict-dialect normalisation: closed object,
        // and the envelope carries `strict: true` (gpt-4o-mini resolves strict).
        let expected_schema = serde_json::json!({
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
            "additionalProperties": false,
        });

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": "test"}],
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "response",
                        "schema": expected_schema,
                        "strict": true,
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

    #[test]
    fn test_map_response_format_strict_marks_schema_strict() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
        });
        let result = map_response_format(
            &ResponseFormat::JsonSchema { schema },
            StructuredOutputMode::Strict,
        );
        assert_eq!(result["json_schema"]["strict"], serde_json::json!(true));
    }

    #[test]
    fn test_map_response_format_advisory_does_not_mark_schema_strict() {
        // The advisory branch is unreachable through `resolve` today; a
        // constructed mode is the only coverage of the non-strict envelope.
        let schema = serde_json::json!({
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
        });
        let result = map_response_format(
            &ResponseFormat::JsonSchema { schema },
            StructuredOutputMode::Advisory,
        );
        assert_eq!(result["json_schema"]["strict"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn test_chat_omits_response_format_when_none() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": "test"}],
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = setup(&server);
        let _ = provider.complete(a_request("test")).await.unwrap();
    }

    // -- Options serialisation -----------------------------------------------

    #[tokio::test]
    async fn test_chat_sends_temperature_and_max_tokens() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": "test"}],
                "temperature": 0.7,
                "max_tokens": 100,
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

    // -- Capability reconciliation -------------------------------------------

    #[tokio::test]
    async fn test_chat_reasoning_model_drops_temperature_and_renames_cap() {
        let server = MockServer::start().await;

        // body_json matches the full body, so the absence of `temperature`
        // and `max_tokens` is asserted by their omission from the expected
        // object.
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "o3",
                "messages": [{"role": "user", "content": "test"}],
                "max_completion_tokens": 100,
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider =
            OpenAiInferenceProvider::new(reqwest::Client::new(), server.uri(), "o3", "test-key");
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
    async fn test_probe_reasoning_model_drops_temperature_and_renames_cap() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .and(body_json(serde_json::json!({
                "model": "o3",
                "messages": [{"role": "user", "content": INFERENCE_PROBE_INPUT}],
                "max_completion_tokens": PROBE_MAX_TOKENS,
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_valid_response_json()))
            .expect(1)
            .mount(&server)
            .await;

        let provider =
            OpenAiInferenceProvider::new(reqwest::Client::new(), server.uri(), "o3", "test-key");
        provider.probe_model().await.unwrap();
    }

    #[test]
    fn test_probe_and_ingest_agree_on_admissible_fields_for_reasoning_model() {
        // Probe and ingest share `build_request`, so a reasoning identity
        // yields the same admissible field set regardless of caller.
        let probe = CompletionRequest {
            system: None,
            messages: vec![Message {
                role: Role::User,
                content: INFERENCE_PROBE_INPUT.to_owned(),
            }],
            temperature: Some(0.0),
            max_tokens: Some(PROBE_MAX_TOKENS),
            response_format: None,
        };
        let ingest = CompletionRequest {
            system: None,
            messages: vec![Message {
                role: Role::User,
                content: "real input".to_owned(),
            }],
            temperature: Some(0.5),
            max_tokens: Some(512),
            response_format: None,
        };

        let probe_body = serde_json::to_value(build_request("o3", &probe)).unwrap();
        let ingest_body = serde_json::to_value(build_request("o3", &ingest)).unwrap();
        let probe_keys: BTreeSet<&String> = probe_body.as_object().unwrap().keys().collect();
        let ingest_keys: BTreeSet<&String> = ingest_body.as_object().unwrap().keys().collect();

        assert_eq!(probe_keys, ingest_keys);
        assert!(!probe_keys.contains(&"temperature".to_owned()));
        assert!(!probe_keys.contains(&"max_tokens".to_owned()));
        assert!(probe_keys.contains(&"max_completion_tokens".to_owned()));
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
            if model == "gpt-4o-mini"
                && context == "messages list is empty"
        ));
    }

    // -- Network errors ------------------------------------------------------

    #[tokio::test]
    async fn test_chat_connection_refused_returns_provider_unavailable() {
        let provider = OpenAiInferenceProvider::new(
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

        let provider = OpenAiInferenceProvider::new(client, server.uri(), "model", "key");

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

    #[tokio::test]
    async fn test_chat_http_401_returns_llm_call_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
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
            .and(path(CHAT_PATH))
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
            .and(path(CHAT_PATH))
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
                if expected_shape == "OpenAiChatResponse JSON object"
                    && actual.starts_with("invalid JSON:")
            ),
            "expected ResponseParseFailed with shape and JSON error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_chat_null_content_returns_response_parse_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "chatcmpl-test",
                "object": "chat.completion",
                "model": "gpt-4o-mini",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": null,
                    },
                    "finish_reason": "stop",
                }],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 0,
                    "total_tokens": 10,
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
                if expected_shape == "choices[0].message.content present"
                    && actual == "choices[0].message.content is null"
            ),
            "expected ResponseParseFailed for null content, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_chat_empty_choices_returns_response_parse_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "chatcmpl-test",
                "object": "chat.completion",
                "model": "gpt-4o-mini",
                "choices": [],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 0,
                    "total_tokens": 10,
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
                if expected_shape == "choices[0] present"
                    && actual == "choices array is empty"
            ),
            "expected ResponseParseFailed for empty choices, got {err:?}"
        );
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
