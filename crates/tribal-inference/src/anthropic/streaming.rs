//! Streaming-wire translation for the Anthropic Messages API.
//!
//! Translates the `/v1/messages` server-sent event grammar into
//! [`InferenceEvent`]s. Each SSE data payload carries its own `type`
//! discriminator, so `event:` lines are ignored. Usage accumulates across
//! the exchange: input and cache counts arrive on `message_start`, the
//! cumulative output count on `message_delta`, and `message_stop` closes
//! the exchange with the terminal event.

use std::{collections::HashMap, time::Instant};

use tribal_domain::{CompletionResponse, CompletionUsage, InferenceEvent};

use super::inference::AnthropicUsage;
use crate::{
    InferenceError, ProviderIdentity,
    http::record_completion_usage,
    stream::{EventTranslator, SseAssembler, parse_frame},
};

// ---------------------------------------------------------------------------
// Private serde types
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
#[serde(tag = "type")]
enum AnthropicStreamEvent {
    #[serde(rename = "message_start")]
    MessageStart { message: StreamMessageStart },
    #[serde(rename = "content_block_start")]
    ContentBlockStart {
        index: u32,
        content_block: StreamContentBlock,
    },
    #[serde(rename = "content_block_delta")]
    ContentBlockDelta { index: u32, delta: StreamDelta },
    #[serde(rename = "message_delta")]
    MessageDelta {
        #[serde(default)]
        usage: Option<StreamMessageDeltaUsage>,
    },
    #[serde(rename = "message_stop")]
    MessageStop,
    #[serde(rename = "error")]
    Error { error: StreamError },
    /// `content_block_stop`, `ping`, and any event the API grows later;
    /// the streaming grammar documents unknown events as ignorable.
    #[serde(other)]
    Other,
}

#[derive(serde::Deserialize)]
struct StreamMessageStart {
    usage: AnthropicUsage,
}

#[derive(serde::Deserialize)]
#[serde(tag = "type")]
enum StreamContentBlock {
    #[serde(rename = "text")]
    Text {},
    #[serde(rename = "tool_use")]
    ToolUse { id: String, name: String },
    #[serde(other)]
    Other,
}

#[derive(serde::Deserialize)]
#[serde(tag = "type")]
enum StreamDelta {
    #[serde(rename = "text_delta")]
    TextDelta { text: String },
    #[serde(rename = "thinking_delta")]
    ThinkingDelta { thinking: String },
    #[serde(rename = "input_json_delta")]
    InputJsonDelta { partial_json: String },
    #[serde(other)]
    Other,
}

#[derive(serde::Deserialize)]
struct StreamMessageDeltaUsage {
    /// Cumulative output token count for the message so far.
    output_tokens: u32,
}

#[derive(serde::Deserialize)]
struct StreamError {
    #[serde(default, rename = "type")]
    kind: String,
    #[serde(default)]
    message: String,
}

// ---------------------------------------------------------------------------
// Translator
// ---------------------------------------------------------------------------

/// Accumulates one streamed Messages exchange into the terminal response.
pub(super) struct AnthropicStreamTranslator {
    identity: ProviderIdentity,
    started: Instant,
    sse: SseAssembler,
    text: String,
    text_blocks: usize,
    tool_blocks: HashMap<u32, ToolBlock>,
    input_tokens: u32,
    output_tokens: u32,
    cache_read_tokens: u32,
    cache_write_tokens: u32,
}

struct ToolBlock {
    id: String,
    name: String,
}

impl AnthropicStreamTranslator {
    pub(super) fn new(identity: ProviderIdentity) -> Self {
        Self {
            identity,
            started: Instant::now(),
            sse: SseAssembler::default(),
            text: String::new(),
            text_blocks: 0,
            tool_blocks: HashMap::new(),
            input_tokens: 0,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        }
    }

    fn on_event(
        &mut self,
        event: AnthropicStreamEvent,
    ) -> Result<Vec<InferenceEvent>, InferenceError> {
        match event {
            AnthropicStreamEvent::MessageStart { message } => {
                self.input_tokens = message.usage.input_tokens;
                self.output_tokens = message.usage.output_tokens;
                self.cache_read_tokens = message.usage.cache_read_input_tokens.unwrap_or(0);
                self.cache_write_tokens = message.usage.cache_creation_input_tokens.unwrap_or(0);
                Ok(vec![])
            }
            AnthropicStreamEvent::ContentBlockStart {
                index,
                content_block,
            } => {
                match content_block {
                    StreamContentBlock::Text {} => self.text_blocks += 1,
                    StreamContentBlock::ToolUse { id, name } => {
                        self.tool_blocks.insert(index, ToolBlock { id, name });
                    }
                    StreamContentBlock::Other => {}
                }
                Ok(vec![])
            }
            AnthropicStreamEvent::ContentBlockDelta { index, delta } => Ok(match delta {
                StreamDelta::TextDelta { text } => {
                    self.text.push_str(&text);
                    vec![InferenceEvent::TextDelta { text }]
                }
                StreamDelta::ThinkingDelta { thinking } => {
                    vec![InferenceEvent::ReasoningDelta { text: thinking }]
                }
                StreamDelta::InputJsonDelta { partial_json } => {
                    let block = self.tool_blocks.get(&index);
                    vec![InferenceEvent::ToolCallDelta {
                        index,
                        call_id: block.map(|b| b.id.clone()),
                        name: block.map(|b| b.name.clone()),
                        arguments_fragment: partial_json,
                    }]
                }
                StreamDelta::Other => vec![],
            }),
            AnthropicStreamEvent::MessageDelta { usage } => {
                if let Some(usage) = usage {
                    self.output_tokens = usage.output_tokens;
                }
                Ok(vec![])
            }
            AnthropicStreamEvent::MessageStop => self.terminal().map(|event| vec![event]),
            AnthropicStreamEvent::Error { error } => Err(InferenceError::LlmCallFailed {
                model: self.identity.model.clone(),
                context: format!(
                    "provider streamed an error event: {}: {}",
                    error.kind, error.message
                ),
                source: None,
            }),
            AnthropicStreamEvent::Other => Ok(vec![]),
        }
    }

    /// Builds the terminal event, mirroring the buffered path's
    /// no-text-content failure.
    fn terminal(&mut self) -> Result<InferenceEvent, InferenceError> {
        if self.text_blocks == 0 {
            return Err(InferenceError::ResponseParseFailed {
                expected_shape: "at least one text content block".to_owned(),
                actual: "0 text blocks in response".to_owned(),
            });
        }

        let usage = CompletionUsage {
            provider: self.identity.name.clone(),
            model: self.identity.model.clone(),
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_read_tokens: self.cache_read_tokens,
            cache_write_tokens: self.cache_write_tokens,
            total_tokens: self.input_tokens.saturating_add(self.output_tokens),
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

impl EventTranslator for AnthropicStreamTranslator {
    fn on_line(&mut self, line: &str) -> Result<Vec<InferenceEvent>, InferenceError> {
        let Some(data) = self.sse.on_line(line) else {
            return Ok(vec![]);
        };
        let event = parse_frame(&data, "Anthropic stream event JSON object")?;
        self.on_event(event)
    }

    fn on_end(&mut self) -> Result<Vec<InferenceEvent>, InferenceError> {
        Err(InferenceError::ResponseParseFailed {
            expected_shape: "a message_stop event".to_owned(),
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
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::super::inference::MESSAGES_PATH;
    use crate::{
        AnthropicInferenceProvider, CompletionRequest, InferenceProvider, Message, Role,
        collect_completion,
    };
    use tribal_domain::InferenceEvent;

    const MODEL: &str = "claude-haiku-4-5-20251001";

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

    fn setup(server: &MockServer) -> AnthropicInferenceProvider {
        AnthropicInferenceProvider::new(reqwest::Client::new(), server.uri(), MODEL, "test-key")
    }

    /// An SSE body equivalent to the buffered fixture: text "Hello!",
    /// 10 input tokens, 5 output tokens, 100 cache-read, 150 cache-write.
    fn a_stream_body() -> String {
        [
            "event: message_start",
            r#"data: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":1,"cache_creation_input_tokens":150,"cache_read_input_tokens":100}}}"#,
            "",
            "event: content_block_start",
            r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
            "",
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hel"}}"#,
            "",
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"lo!"}}"#,
            "",
            r#"data: {"type":"content_block_stop","index":0}"#,
            "",
            r#"data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}"#,
            "",
            r#"data: {"type":"message_stop"}"#,
            "",
            "",
        ]
        .join("\n")
    }

    fn a_buffered_body() -> serde_json::Value {
        serde_json::json!({
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": "Hello!"}],
            "model": MODEL,
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "cache_creation_input_tokens": 150,
                "cache_read_input_tokens": 100,
            },
        })
    }

    async fn mount_stream(server: &MockServer, body: String) {
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
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
            .and(path(MESSAGES_PATH))
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
        assert_eq!(
            streamed.usage.cache_read_tokens,
            buffered.usage.cache_read_tokens
        );
        assert_eq!(
            streamed.usage.cache_write_tokens,
            buffered.usage.cache_write_tokens
        );
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
    async fn test_stream_request_body_differs_only_by_stream_field() {
        let stream_server = MockServer::start().await;
        mount_stream(&stream_server, a_stream_body()).await;
        let buffered_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
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

        let stream_field = streamed_body.as_object_mut().unwrap().remove("stream");
        assert_eq!(stream_field, Some(serde_json::json!(true)));
        assert_eq!(streamed_body, buffered_body);
    }

    #[tokio::test]
    async fn test_stream_thinking_deltas_surface_as_reasoning() {
        let body = [
            r#"data: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":1}}}"#,
            "",
            r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking"}}"#,
            "",
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"hmm"}}"#,
            "",
            r#"data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}"#,
            "",
            r#"data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"answer"}}"#,
            "",
            r#"data: {"type":"message_stop"}"#,
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
    async fn test_stream_tool_call_deltas_carry_block_identity() {
        let body = [
            r#"data: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":1}}}"#,
            "",
            r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
            "",
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"calling"}}"#,
            "",
            r#"data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_01","name":"search"}}"#,
            "",
            r#"data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"q\":"}}"#,
            "",
            r#"data: {"type":"message_stop"}"#,
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
            InferenceEvent::ToolCallDelta { index: 1, call_id: Some(id), name: Some(name), arguments_fragment }
                if id == "toolu_01" && name == "search" && arguments_fragment == "{\"q\":"
        ));
    }

    #[tokio::test]
    async fn test_stream_error_event_fails_the_stream() {
        let body = [
            r#"data: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":1}}}"#,
            "",
            r#"data: {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#,
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
                if context.contains("overloaded_error") && context.contains("Overloaded")
        ));
    }

    #[tokio::test]
    async fn test_stream_eof_without_message_stop_errors() {
        let body = [
            r#"data: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":1}}}"#,
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
                if expected_shape == "a message_stop event"
        ));
    }

    #[tokio::test]
    async fn test_stream_no_text_blocks_mirrors_the_buffered_failure() {
        let body = [
            r#"data: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":1}}}"#,
            "",
            r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_01","name":"search"}}"#,
            "",
            r#"data: {"type":"message_stop"}"#,
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
                if expected_shape == "at least one text content block"
                    && actual == "0 text blocks in response"
        ));
    }

    #[tokio::test]
    async fn test_stream_http_error_maps_before_the_stream_opens() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MESSAGES_PATH))
            .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
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
                if reason.contains("429")
        ));
    }

    #[tokio::test]
    async fn test_stream_empty_messages_rejected_before_the_wire() {
        let server = MockServer::start().await;
        let request = CompletionRequest {
            system: None,
            messages: vec![],
            temperature: None,
            max_tokens: None,
            response_format: None,
        };

        let err = setup(&server)
            .complete_stream(request)
            .await
            .err()
            .expect("an empty messages list must fail the call before the wire");

        assert!(matches!(
            err,
            crate::InferenceError::LlmCallFailed { ref context, .. }
                if context == "messages list is empty"
        ));
    }
}
