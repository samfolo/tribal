//! Streaming-wire translation for the Ollama chat API.
//!
//! Translates the `/api/chat` newline-delimited JSON grammar into
//! [`InferenceEvent`]s: one JSON chunk per line, content fragments while
//! `done` is false, and a final `done: true` chunk carrying the evaluation
//! counts that closes the exchange with the terminal event. Thinking
//! models surface `message.thinking` fragments, which map onto reasoning
//! deltas. A mid-stream failure arrives as an `error` line.

use std::time::Instant;

use tribal_domain::{CompletionResponse, CompletionUsage, InferenceEvent};

use crate::{
    InferenceError, ProviderIdentity,
    http::record_completion_usage,
    stream::{EventTranslator, parse_frame},
};

// ---------------------------------------------------------------------------
// Private serde types
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct OllamaStreamChunk {
    #[serde(default)]
    message: Option<StreamMessage>,
    #[serde(default)]
    done: bool,
    #[serde(default)]
    prompt_eval_count: Option<u32>,
    #[serde(default)]
    eval_count: Option<u32>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(serde::Deserialize)]
struct StreamMessage {
    #[serde(default)]
    content: String,
    #[serde(default)]
    thinking: Option<String>,
}

// ---------------------------------------------------------------------------
// Translator
// ---------------------------------------------------------------------------

/// Accumulates one streamed chat exchange into the terminal response.
pub(super) struct OllamaStreamTranslator {
    identity: ProviderIdentity,
    started: Instant,
    text: String,
}

impl OllamaStreamTranslator {
    pub(super) fn new(identity: ProviderIdentity) -> Self {
        Self {
            identity,
            started: Instant::now(),
            text: String::new(),
        }
    }

    fn terminal(&mut self, chunk: &OllamaStreamChunk) -> InferenceEvent {
        let input_tokens = chunk.prompt_eval_count.unwrap_or_else(|| {
            tracing::debug!("prompt_eval_count absent, defaulting to 0");
            0
        });
        let output_tokens = chunk.eval_count.unwrap_or_else(|| {
            tracing::debug!("eval_count absent, defaulting to 0");
            0
        });

        let usage = CompletionUsage {
            provider: self.identity.name.clone(),
            model: self.identity.model.clone(),
            input_tokens,
            output_tokens,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            total_tokens: input_tokens.saturating_add(output_tokens),
            latency: self.started.elapsed(),
        };
        record_completion_usage(&usage);

        InferenceEvent::Completed {
            response: CompletionResponse {
                text: std::mem::take(&mut self.text),
                usage,
            },
        }
    }
}

impl EventTranslator for OllamaStreamTranslator {
    fn on_line(&mut self, line: &str) -> Result<Vec<InferenceEvent>, InferenceError> {
        if line.is_empty() {
            return Ok(vec![]);
        }
        let chunk: OllamaStreamChunk = parse_frame(line, "Ollama stream chunk JSON object")?;

        if let Some(error) = chunk.error {
            return Err(InferenceError::LlmCallFailed {
                model: self.identity.model.clone(),
                context: format!("provider streamed an error line: {error}"),
                source: None,
            });
        }

        let mut events = Vec::new();
        if let Some(message) = &chunk.message {
            if let Some(thinking) = &message.thinking
                && !thinking.is_empty()
            {
                events.push(InferenceEvent::ReasoningDelta {
                    text: thinking.clone(),
                });
            }
            if !message.content.is_empty() {
                self.text.push_str(&message.content);
                events.push(InferenceEvent::TextDelta {
                    text: message.content.clone(),
                });
            }
        }
        if chunk.done {
            events.push(self.terminal(&chunk));
        }
        Ok(events)
    }

    fn on_end(&mut self) -> Result<Vec<InferenceEvent>, InferenceError> {
        Err(InferenceError::ResponseParseFailed {
            expected_shape: "a done=true chunk".to_owned(),
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

    use super::super::inference::CHAT_PATH;
    use crate::{
        CompletionRequest, InferenceProvider, Message, Role, collect_completion,
        ollama::OllamaInferenceProvider,
    };
    use tribal_domain::InferenceEvent;

    const MODEL: &str = "llama3";

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

    fn setup(server: &MockServer) -> OllamaInferenceProvider {
        OllamaInferenceProvider::new(reqwest::Client::new(), server.uri(), MODEL)
    }

    /// An NDJSON body equivalent to the buffered fixture: text "Hello!",
    /// 10 input tokens, 5 output tokens.
    fn a_stream_body() -> String {
        [
            r#"{"model":"llama3","message":{"role":"assistant","content":"Hel"},"done":false}"#,
            r#"{"model":"llama3","message":{"role":"assistant","content":"lo!"},"done":false}"#,
            r#"{"model":"llama3","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop","prompt_eval_count":10,"eval_count":5}"#,
            "",
        ]
        .join("\n")
    }

    fn a_buffered_body() -> serde_json::Value {
        serde_json::json!({
            "model": MODEL,
            "message": {"role": "assistant", "content": "Hello!"},
            "done": true,
            "prompt_eval_count": 10,
            "eval_count": 5,
        })
    }

    async fn mount_stream(server: &MockServer, body: String) {
        Mock::given(method("POST"))
            .and(path(CHAT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/x-ndjson"))
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
    async fn test_stream_request_body_differs_only_by_stream_value() {
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
        let mut buffered_body: serde_json::Value =
            serde_json::from_slice(&buffered_request.body).unwrap();

        assert_eq!(
            streamed_body.as_object_mut().unwrap().remove("stream"),
            Some(serde_json::json!(true))
        );
        assert_eq!(
            buffered_body.as_object_mut().unwrap().remove("stream"),
            Some(serde_json::json!(false))
        );
        assert_eq!(streamed_body, buffered_body);
    }

    #[tokio::test]
    async fn test_stream_thinking_fragments_surface_as_reasoning() {
        let body = [
            r#"{"model":"llama3","message":{"role":"assistant","content":"","thinking":"hmm"},"done":false}"#,
            r#"{"model":"llama3","message":{"role":"assistant","content":"answer"},"done":false}"#,
            r#"{"model":"llama3","message":{"role":"assistant","content":""},"done":true,"prompt_eval_count":10,"eval_count":5}"#,
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
    async fn test_stream_error_line_fails_the_stream() {
        let body = [
            r#"{"model":"llama3","message":{"role":"assistant","content":"par"},"done":false}"#,
            r#"{"error":"model runner has unexpectedly stopped"}"#,
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
                if context.contains("model runner has unexpectedly stopped")
        ));
    }

    #[tokio::test]
    async fn test_stream_eof_without_done_chunk_errors() {
        let body = [
            r#"{"model":"llama3","message":{"role":"assistant","content":"par"},"done":false}"#,
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
                if expected_shape == "a done=true chunk"
        ));
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
