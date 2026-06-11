//! Streaming-wire translation for the `OpenAI` chat completions API.
//!
//! Translates the `/v1/chat/completions` server-sent event grammar into
//! [`InferenceEvent`]s. Content arrives as `choices[].delta` fragments,
//! token usage arrives in a dedicated final chunk (requested via
//! `stream_options.include_usage`), and the `[DONE]` sentinel closes the
//! exchange with the terminal event. `OpenAI`-compatible runtimes that
//! surface reasoning as `delta.reasoning_content` map onto reasoning
//! deltas.

use std::time::Instant;

use tribal_domain::{CompletionResponse, CompletionUsage, InferenceEvent};

use super::inference::OpenAiChatUsage;
use crate::{
    InferenceError, ProviderIdentity,
    http::{body_preview, record_completion_usage},
    stream::{EventTranslator, SseAssembler, parse_frame},
};

/// The sentinel data payload that closes an `OpenAI` event stream.
const DONE_SENTINEL: &str = "[DONE]";

// ---------------------------------------------------------------------------
// Private serde types
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct OpenAiStreamChunk {
    #[serde(default)]
    choices: Vec<StreamChoice>,
    #[serde(default)]
    usage: Option<OpenAiChatUsage>,
    /// Some compatible runtimes stream failures as an in-band error object.
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(serde::Deserialize)]
struct StreamChoice {
    #[serde(default)]
    delta: StreamDelta,
}

#[derive(serde::Deserialize, Default)]
struct StreamDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    reasoning_content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<StreamToolCall>,
}

#[derive(serde::Deserialize)]
struct StreamToolCall {
    index: u32,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    function: Option<StreamToolFunction>,
}

#[derive(serde::Deserialize, Default)]
struct StreamToolFunction {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

// ---------------------------------------------------------------------------
// Translator
// ---------------------------------------------------------------------------

/// Accumulates one streamed chat completions exchange into the terminal
/// response.
pub(super) struct OpenAiStreamTranslator {
    identity: ProviderIdentity,
    started: Instant,
    sse: SseAssembler,
    text: String,
    content_seen: bool,
    usage: Option<OpenAiChatUsage>,
}

impl OpenAiStreamTranslator {
    pub(super) fn new(identity: ProviderIdentity) -> Self {
        Self {
            identity,
            started: Instant::now(),
            sse: SseAssembler::default(),
            text: String::new(),
            content_seen: false,
            usage: None,
        }
    }

    /// Translates one assembled SSE data payload, however it dispatched.
    fn on_data(&mut self, data: &str) -> Result<Vec<InferenceEvent>, InferenceError> {
        if data == DONE_SENTINEL {
            return self.terminal().map(|event| vec![event]);
        }
        let chunk = parse_frame(data, "OpenAI stream chunk JSON object")?;
        self.on_chunk(chunk)
    }

    fn on_chunk(
        &mut self,
        chunk: OpenAiStreamChunk,
    ) -> Result<Vec<InferenceEvent>, InferenceError> {
        if let Some(error) = chunk.error {
            return Err(InferenceError::LlmCallFailed {
                model: self.identity.model.clone(),
                context: format!(
                    "provider streamed an error object: {}",
                    body_preview(&error.to_string())
                ),
                source: None,
            });
        }
        if let Some(usage) = chunk.usage {
            self.usage = Some(usage);
        }

        let mut events = Vec::new();
        for choice in chunk.choices {
            if let Some(content) = choice.delta.content {
                self.content_seen = true;
                if !content.is_empty() {
                    self.text.push_str(&content);
                    events.push(InferenceEvent::TextDelta { text: content });
                }
            }
            if let Some(reasoning) = choice.delta.reasoning_content
                && !reasoning.is_empty()
            {
                events.push(InferenceEvent::ReasoningDelta { text: reasoning });
            }
            for call in choice.delta.tool_calls {
                let function = call.function.unwrap_or_default();
                events.push(InferenceEvent::ToolCallDelta {
                    index: call.index,
                    call_id: call.id,
                    name: function.name,
                    arguments_fragment: function.arguments.unwrap_or_default(),
                });
            }
        }
        Ok(events)
    }

    /// Builds the terminal event, mirroring the buffered path's
    /// null-content failure when no content delta ever arrived.
    fn terminal(&mut self) -> Result<InferenceEvent, InferenceError> {
        if !self.content_seen {
            return Err(InferenceError::ResponseParseFailed {
                expected_shape: "choices[0].message.content present".to_owned(),
                actual: "no content deltas in stream".to_owned(),
            });
        }

        if self.usage.is_none() {
            tracing::debug!("stream ended without a usage chunk, defaulting counts to 0");
        }
        let counts = self.usage.take().unwrap_or(OpenAiChatUsage {
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
        });

        let usage = CompletionUsage {
            provider: self.identity.name.clone(),
            model: self.identity.model.clone(),
            input_tokens: counts.prompt_tokens,
            output_tokens: counts.completion_tokens,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            total_tokens: counts.total_tokens,
            latency: self.started.elapsed(),
        };
        record_completion_usage(&usage);

        Ok(InferenceEvent::Completed {
            response: CompletionResponse {
                text: std::mem::take(&mut self.text),
                usage,
            },
        })
    }
}

impl EventTranslator for OpenAiStreamTranslator {
    fn on_line(&mut self, line: &str) -> Result<Vec<InferenceEvent>, InferenceError> {
        let Some(data) = self.sse.on_line(line) else {
            return Ok(vec![]);
        };
        self.on_data(&data)
    }

    fn on_end(&mut self) -> Result<Vec<InferenceEvent>, InferenceError> {
        // A final event may be dispatched by the wire closing rather than a
        // blank line: a sentinel buffered at EOF still terminates cleanly.
        // Only a terminal flush satisfies the contract — a non-terminal
        // payload still leaves the exchange without its closing signal.
        if let Some(data) = self.sse.take_pending() {
            let events = self.on_data(&data)?;
            if events
                .iter()
                .any(|event| matches!(event, InferenceEvent::Completed { .. }))
            {
                return Ok(events);
            }
        }
        Err(InferenceError::ResponseParseFailed {
            expected_shape: "a [DONE] sentinel".to_owned(),
            actual: "stream ended without one".to_owned(),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use tribal_domain::InferenceEvent;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::super::inference::CHAT_PATH;
    use crate::{
        CompletionRequest, InferenceProvider, Message, Role, collect_completion,
        openai::OpenAiInferenceProvider,
    };

    const MODEL: &str = "gpt-4o-mini";

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

    fn setup(server: &MockServer) -> OpenAiInferenceProvider {
        OpenAiInferenceProvider::new(reqwest::Client::new(), server.uri(), MODEL, "test-key")
    }

    /// An SSE body equivalent to the buffered fixture: text "Hello!",
    /// 10 input tokens, 5 output tokens.
    fn a_stream_body() -> String {
        [
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}"#,
            "",
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"Hel"}}]}"#,
            "",
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"lo!"}}]}"#,
            "",
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
            "",
            r#"data: {"id":"c1","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}"#,
            "",
            "data: [DONE]",
            "",
            "",
        ]
        .join("\n")
    }

    fn a_buffered_body() -> serde_json::Value {
        serde_json::json!({
            "id": "c1",
            "object": "chat.completion",
            "model": MODEL,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "Hello!"},
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15,
            },
        })
    }

    async fn mount_stream(server: &MockServer, body: String) {
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn test_stream_folds_to_the_buffered_response() {
        let stream_server = MockServer::start().await;
        mount_stream(&stream_server, a_stream_body()).await;
        let buffered_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_buffered_body()))
            .mount(&buffered_server)
            .await;

        let streamed = setup(&stream_server)
            .complete_stream(a_request("test"))
            .await
            .unwrap();
        let streamed = collect_completion(streamed).await.unwrap();
        let buffered = setup(&buffered_server)
            .complete(a_request("test"))
            .await
            .unwrap();

        assert_eq!(streamed.text, buffered.text);
        assert_eq!(streamed.usage.provider, buffered.usage.provider);
        assert_eq!(streamed.usage.model, buffered.usage.model);
        assert_eq!(streamed.usage.input_tokens, buffered.usage.input_tokens);
        assert_eq!(streamed.usage.output_tokens, buffered.usage.output_tokens);
        assert_eq!(streamed.usage.total_tokens, buffered.usage.total_tokens);
    }

    #[tokio::test]
    async fn test_stream_deltas_fold_to_the_terminal_text() {
        let server = MockServer::start().await;
        mount_stream(&server, a_stream_body()).await;

        let mut stream = setup(&server)
            .complete_stream(a_request("test"))
            .await
            .unwrap();

        let mut folded = String::new();
        let mut terminal = None;
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                InferenceEvent::TextDelta { text } => folded.push_str(&text),
                InferenceEvent::Completed { response } => terminal = Some(response),
                InferenceEvent::ReasoningDelta { .. } | InferenceEvent::ToolCallDelta { .. } => {}
            }
        }

        let terminal = terminal.expect("stream must end with the terminal event");
        assert_eq!(folded, "Hello!");
        assert_eq!(terminal.text, folded);
    }

    #[tokio::test]
    async fn test_stream_request_body_differs_only_by_stream_fields() {
        let stream_server = MockServer::start().await;
        mount_stream(&stream_server, a_stream_body()).await;
        let buffered_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(a_buffered_body()))
            .mount(&buffered_server)
            .await;

        let _ = setup(&stream_server)
            .complete_stream(a_request("identical input"))
            .await
            .unwrap();
        let _ = setup(&buffered_server)
            .complete(a_request("identical input"))
            .await
            .unwrap();

        let streamed_request = &stream_server.received_requests().await.unwrap()[0];
        let buffered_request = &buffered_server.received_requests().await.unwrap()[0];
        let mut streamed_body: serde_json::Value =
            serde_json::from_slice(&streamed_request.body).unwrap();
        let buffered_body: serde_json::Value =
            serde_json::from_slice(&buffered_request.body).unwrap();

        let body = streamed_body.as_object_mut().unwrap();
        assert_eq!(body.remove("stream"), Some(serde_json::json!(true)));
        assert_eq!(
            body.remove("stream_options"),
            Some(serde_json::json!({"include_usage": true}))
        );
        assert_eq!(streamed_body, buffered_body);
    }

    #[tokio::test]
    async fn test_stream_reasoning_content_surfaces_as_reasoning() {
        let body = [
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"reasoning_content":"hmm"}}]}"#,
            "",
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"answer"}}]}"#,
            "",
            "data: [DONE]",
            "",
            "",
        ]
        .join("\n");
        let server = MockServer::start().await;
        mount_stream(&server, body).await;

        let stream = setup(&server)
            .complete_stream(a_request("test"))
            .await
            .unwrap();
        let events: Vec<_> = stream.collect().await;

        assert!(matches!(
            events[0].as_ref().unwrap(),
            InferenceEvent::ReasoningDelta { text } if text == "hmm"
        ));
        assert!(matches!(
            events.last().unwrap().as_ref().unwrap(),
            InferenceEvent::Completed { response } if response.text == "answer"
        ));
    }

    #[tokio::test]
    async fn test_stream_tool_call_deltas_carry_fragment_identity() {
        let body = [
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"calling"}}]}"#,
            "",
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"search","arguments":"{\"q\":"}}]}}]}"#,
            "",
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"x\"}"}}]}}]}"#,
            "",
            "data: [DONE]",
            "",
            "",
        ]
        .join("\n");
        let server = MockServer::start().await;
        mount_stream(&server, body).await;

        let stream = setup(&server)
            .complete_stream(a_request("test"))
            .await
            .unwrap();
        let events: Vec<_> = stream.collect().await;

        assert!(matches!(
            events[1].as_ref().unwrap(),
            InferenceEvent::ToolCallDelta { index: 0, call_id: Some(id), name: Some(name), arguments_fragment }
                if id == "call_1" && name == "search" && arguments_fragment == "{\"q\":"
        ));
        assert!(matches!(
            events[2].as_ref().unwrap(),
            InferenceEvent::ToolCallDelta { index: 0, call_id: None, name: None, arguments_fragment }
                if arguments_fragment == "\"x\"}"
        ));
    }

    #[tokio::test]
    async fn test_stream_error_object_fails_the_stream() {
        let body = [
            r#"data: {"error":{"message":"insufficient quota","type":"insufficient_quota"}}"#,
            "",
            "",
        ]
        .join("\n");
        let server = MockServer::start().await;
        mount_stream(&server, body).await;

        let stream = setup(&server)
            .complete_stream(a_request("test"))
            .await
            .unwrap();
        let err = collect_completion(stream).await.unwrap_err();

        assert!(matches!(
            err,
            crate::InferenceError::LlmCallFailed { ref context, .. }
                if context.contains("insufficient quota")
        ));
    }

    #[tokio::test]
    async fn test_stream_eof_without_done_sentinel_errors() {
        let body = [
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"partial"}}]}"#,
            "",
            "",
        ]
        .join("\n");
        let server = MockServer::start().await;
        mount_stream(&server, body).await;

        let stream = setup(&server)
            .complete_stream(a_request("test"))
            .await
            .unwrap();
        let err = collect_completion(stream).await.unwrap_err();

        assert!(matches!(
            err,
            crate::InferenceError::ResponseParseFailed { ref expected_shape, .. }
                if expected_shape == "a [DONE] sentinel"
        ));
    }

    #[tokio::test]
    async fn test_stream_non_terminal_flush_at_eof_still_errors() {
        // The wire closes after a complete non-terminal data line with no
        // blank line: the flush must not satisfy the terminal contract.
        let body = [
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"Hello!"}}]}"#,
            "",
            r#"data: {"id":"c1","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}"#,
        ]
        .join("\n");
        let server = MockServer::start().await;
        mount_stream(&server, body).await;

        let stream = setup(&server)
            .complete_stream(a_request("test"))
            .await
            .unwrap();
        let err = collect_completion(stream).await.unwrap_err();

        assert!(matches!(
            err,
            crate::InferenceError::ResponseParseFailed { ref expected_shape, .. }
                if expected_shape == "a [DONE] sentinel"
        ));
    }

    #[tokio::test]
    async fn test_stream_sentinel_dispatched_by_eof_still_terminates() {
        // No blank line after the sentinel: the wire closing dispatches it.
        let body = [
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"Hello!"}}]}"#,
            "",
            "data: [DONE]",
        ]
        .join("\n");
        let server = MockServer::start().await;
        mount_stream(&server, body).await;

        let stream = setup(&server)
            .complete_stream(a_request("test"))
            .await
            .unwrap();
        let response = collect_completion(stream).await.unwrap();

        assert_eq!(response.text, "Hello!");
    }

    #[tokio::test]
    async fn test_stream_without_content_mirrors_the_buffered_failure() {
        let body = [
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"role":"assistant"}}]}"#,
            "",
            "data: [DONE]",
            "",
            "",
        ]
        .join("\n");
        let server = MockServer::start().await;
        mount_stream(&server, body).await;

        let stream = setup(&server)
            .complete_stream(a_request("test"))
            .await
            .unwrap();
        let err = collect_completion(stream).await.unwrap_err();

        assert!(matches!(
            err,
            crate::InferenceError::ResponseParseFailed { ref expected_shape, ref actual }
                if expected_shape == "choices[0].message.content present"
                    && actual == "no content deltas in stream"
        ));
    }

    #[tokio::test]
    async fn test_stream_without_usage_chunk_defaults_counts_to_zero() {
        let body = [
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"text"}}]}"#,
            "",
            "data: [DONE]",
            "",
            "",
        ]
        .join("\n");
        let server = MockServer::start().await;
        mount_stream(&server, body).await;

        let stream = setup(&server)
            .complete_stream(a_request("test"))
            .await
            .unwrap();
        let response = collect_completion(stream).await.unwrap();

        assert_eq!(response.text, "text");
        assert_eq!(response.usage.input_tokens, 0);
        assert_eq!(response.usage.output_tokens, 0);
        assert_eq!(response.usage.total_tokens, 0);
    }

    #[tokio::test]
    async fn test_stream_http_error_maps_before_the_stream_opens() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&server)
            .await;

        let err = setup(&server)
            .complete_stream(a_request("test"))
            .await
            .err()
            .expect("an error status must fail the call before the stream opens");

        assert!(matches!(
            err,
            crate::InferenceError::ProviderUnavailable { ref reason, .. }
                if reason.contains("500")
        ));
    }
}
