//! Implements [`InferenceProvider`] for Tribal's managed platform.
//!
//! The transport is the control-plane gateway, not a raw model endpoint: the
//! request is wrapped as the wire inference bracket, the fleet bearer token
//! authenticates the deployment, and the run-scoped grant set rides an HTTP
//! header the gateway derives tenancy from. The metered path is streamed, and
//! the gateway meters server-side — so the folded response carries no token
//! counts, and the local ledger records nothing for it.

use std::time::Instant;

use async_trait::async_trait;
use reqwest::StatusCode;
use tribal_domain::{CompletionResponse, CompletionUsage, InferenceEvent, ToolCall};
use tribal_wire::gateway::{
    ChatMessage, CompletionChunk, CompletionEnvelope, CompletionTerminal, GATEWAY_CONTRACT_VERSION,
    GRANT_SET_HEADER, GatewayError, InferenceCall, InferenceRequest, ModelId,
    ResponseFormat as WireResponseFormat, ToolCall as WireToolCall, ToolDefinition,
};

use crate::{
    CallContext, CompletionRequest, InferenceError, InferenceProvider, Message, ProviderIdentity,
    ResponseFormat, ToolWireDefinition,
    error::map_send_error,
    http::normalise_base_url,
    stream::{EventTranslator, InferenceEventStream, drive_event_stream, parse_frame},
};

/// The provider name recorded on the folded response and error contexts.
const PROVIDER_NAME: &str = "platform";

/// The gateway's metered-completion endpoint.
pub const INFER_PATH: &str = "/v1/infer";

// ---------------------------------------------------------------------------
// PlatformInferenceProvider
// ---------------------------------------------------------------------------

/// Routes a completion through the managed platform's metered bracket.
pub struct PlatformInferenceProvider {
    client: reqwest::Client,
    base_url: String,
    bearer: String,
    identity: ProviderIdentity,
}

impl PlatformInferenceProvider {
    /// Builds a provider over the gateway at `base_url`, authenticating with the
    /// deployment's `bearer` credential.
    pub fn new(
        client: reqwest::Client,
        base_url: impl Into<String>,
        model: impl Into<String>,
        bearer: impl Into<String>,
    ) -> Self {
        Self {
            client,
            base_url: normalise_base_url(base_url),
            bearer: bearer.into(),
            identity: ProviderIdentity {
                name: PROVIDER_NAME.to_owned(),
                model: model.into(),
            },
        }
    }

    /// Maps a non-success response to its error. A typed refusal carries a
    /// [`GatewayError`] body; a boundary fault (auth, contract) carries only a
    /// status.
    async fn map_refusal(&self, status: StatusCode, response: reqwest::Response) -> InferenceError {
        match status {
            StatusCode::PAYMENT_REQUIRED
            | StatusCode::FORBIDDEN
            | StatusCode::UNPROCESSABLE_ENTITY
            | StatusCode::CONFLICT
            | StatusCode::BAD_GATEWAY => match response.json::<GatewayError>().await {
                Ok(error) => InferenceError::GatewayRefused { error },
                Err(source) => InferenceError::provider_unavailable(
                    PROVIDER_NAME,
                    format!("unparseable typed refusal at {status}: {source}"),
                ),
            },
            _ => InferenceError::provider_unavailable(
                PROVIDER_NAME,
                format!("the gateway refused the call at the boundary with status {status}"),
            ),
        }
    }
}

#[async_trait]
impl InferenceProvider for PlatformInferenceProvider {
    fn identity(&self) -> &ProviderIdentity {
        &self.identity
    }

    async fn complete(
        &self,
        _request: CompletionRequest,
    ) -> Result<CompletionResponse, InferenceError> {
        Err(InferenceError::LlmCallFailed {
            model: self.identity.model.clone(),
            context: "the metered platform path is streamed; complete_stream is required"
                .to_owned(),
            source: None,
        })
    }

    async fn complete_stream(
        &self,
        request: CompletionRequest,
        context: &CallContext,
    ) -> Result<InferenceEventStream, InferenceError> {
        // Fail closed before any envelope or money: the gateway sizes the
        // worst-case hold from `max_tokens`, so a call without it is refused
        // here rather than reaching the wire.
        let max_tokens = request
            .max_tokens
            .ok_or_else(|| InferenceError::MaxTokensRequired {
                model: self.identity.model.clone(),
            })?;
        let position_key =
            context
                .position_key
                .clone()
                .ok_or_else(|| InferenceError::LlmCallFailed {
                    model: self.identity.model.clone(),
                    context: "a metered call requires a position key".to_owned(),
                    source: None,
                })?;

        let body = InferenceRequest {
            contract_version: GATEWAY_CONTRACT_VERSION,
            position_key,
            call: InferenceCall::Completion(build_envelope(
                &self.identity.model,
                request,
                max_tokens,
            )),
        };

        let url = format!("{}{INFER_PATH}", self.base_url);
        let mut call = self.client.post(&url).bearer_auth(&self.bearer).json(&body);
        // A call with no grant reaches the gateway grantless and is refused at
        // its transport boundary; the provider never fabricates one.
        if let Some(grant) = &context.grant {
            let value =
                serde_json::to_string(grant).map_err(|source| InferenceError::LlmCallFailed {
                    model: self.identity.model.clone(),
                    context: format!("serialising the grant set for its header failed: {source}"),
                    source: None,
                })?;
            call = call.header(GRANT_SET_HEADER, value);
        }

        let response = call
            .send()
            .await
            .map_err(|source| map_send_error(&source, PROVIDER_NAME))?;
        let status = response.status();
        if status.is_success() {
            let translator = PlatformStreamTranslator::new(self.identity.clone());
            Ok(drive_event_stream(response, translator, PROVIDER_NAME))
        } else {
            Err(self.map_refusal(status, response).await)
        }
    }
}

// ---------------------------------------------------------------------------
// Request translation
// ---------------------------------------------------------------------------

fn build_envelope(model: &str, request: CompletionRequest, max_tokens: u32) -> CompletionEnvelope {
    CompletionEnvelope {
        model: ModelId::new(model),
        system: request.system,
        messages: request.messages.into_iter().map(to_wire_message).collect(),
        tools: request.tools.into_iter().map(to_wire_tool).collect(),
        response_format: request.response_format.map(to_wire_response_format),
        max_tokens,
        temperature: request.temperature,
        top_p: None,
        stop_sequences: vec![],
    }
}

fn to_wire_message(message: Message) -> ChatMessage {
    match message {
        Message::User { content } => ChatMessage::User { content },
        Message::Assistant {
            content,
            tool_calls,
        } => ChatMessage::Assistant {
            content,
            tool_calls: tool_calls.into_iter().map(to_wire_tool_call).collect(),
        },
        Message::Tool {
            tool_call_id,
            content,
        } => ChatMessage::Tool {
            tool_call_id,
            content,
        },
    }
}

fn to_wire_tool_call(call: ToolCall) -> WireToolCall {
    WireToolCall {
        id: call.id,
        name: call.name,
        arguments: call.arguments,
    }
}

fn to_wire_tool(tool: ToolWireDefinition) -> ToolDefinition {
    ToolDefinition {
        name: tool.name,
        description: tool.description,
        input_schema: tool.input_schema,
    }
}

fn to_wire_response_format(format: ResponseFormat) -> WireResponseFormat {
    match format {
        ResponseFormat::Json => WireResponseFormat::Json,
        ResponseFormat::JsonSchema { schema } => WireResponseFormat::JsonSchema { schema },
    }
}

fn to_domain_tool_call(call: WireToolCall) -> ToolCall {
    ToolCall {
        id: call.id,
        name: call.name,
        arguments: call.arguments,
    }
}

// ---------------------------------------------------------------------------
// Response translation
// ---------------------------------------------------------------------------

/// One line of the gateway's NDJSON completion stream: a text chunk, or the
/// terminal that closes the exchange. The terminal carries `finish_reason`,
/// which a chunk never does, so an untagged decode disambiguates them.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum PlatformFrame {
    Terminal(CompletionTerminal),
    Chunk(CompletionChunk),
}

/// Accumulates one streamed completion into its terminal response.
struct PlatformStreamTranslator {
    identity: ProviderIdentity,
    started: Instant,
    text: String,
}

impl PlatformStreamTranslator {
    fn new(identity: ProviderIdentity) -> Self {
        Self {
            identity,
            started: Instant::now(),
            text: String::new(),
        }
    }
}

impl EventTranslator for PlatformStreamTranslator {
    fn on_line(&mut self, line: &str) -> Result<Vec<InferenceEvent>, InferenceError> {
        if line.is_empty() {
            return Ok(vec![]);
        }
        match parse_frame(line, "a platform completion chunk or terminal")? {
            PlatformFrame::Chunk(chunk) => {
                if chunk.text.is_empty() {
                    return Ok(vec![]);
                }
                self.text.push_str(&chunk.text);
                Ok(vec![InferenceEvent::TextDelta { text: chunk.text }])
            }
            PlatformFrame::Terminal(terminal) => {
                // The gateway meters server-side, so the terminal carries no
                // counts; the folded usage is zeroed and never ledgered locally.
                let usage = CompletionUsage {
                    provider: PROVIDER_NAME.to_owned(),
                    model: self.identity.model.clone(),
                    input_tokens: 0,
                    output_tokens: 0,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                    total_tokens: 0,
                    latency: self.started.elapsed(),
                };
                let response = CompletionResponse {
                    text: std::mem::take(&mut self.text),
                    tool_calls: terminal
                        .tool_calls
                        .into_iter()
                        .map(to_domain_tool_call)
                        .collect(),
                    usage,
                };
                Ok(vec![InferenceEvent::Completed { response }])
            }
        }
    }

    fn on_end(&mut self) -> Result<Vec<InferenceEvent>, InferenceError> {
        Err(InferenceError::ResponseParseFailed {
            expected_shape: "a terminal completion frame".to_owned(),
            actual: "stream ended without one".to_owned(),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_wire::gateway::{
        AccountReference, GATEWAY_CONTRACT_VERSION, GRANT_SET_HEADER, GatewayError, GrantSet,
        InferenceCall, InferenceRequest, PositionKey, PrincipalReference,
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::{INFER_PATH, PlatformInferenceProvider};
    use crate::{
        CallContext, CompletionRequest, InferenceError, InferenceProvider, Message,
        collect_completion,
    };

    const MODEL: &str = "mock-model";
    const BEARER: &str = "fleet-token";

    fn a_request(max_tokens: Option<u32>) -> CompletionRequest {
        CompletionRequest {
            system: Some("be terse".to_owned()),
            messages: vec![Message::User {
                content: "hi".to_owned(),
            }],
            tools: vec![],
            temperature: None,
            max_tokens,
            response_format: None,
        }
    }

    fn a_context() -> CallContext {
        CallContext {
            position_key: Some(PositionKey::new("thread_01:10")),
            grant: Some(GrantSet {
                account: AccountReference::new("acct_01"),
                principal: Some(PrincipalReference::new("prin_01")),
                tools: vec!["lookup".to_owned()],
            }),
        }
    }

    fn setup(server: &MockServer) -> PlatformInferenceProvider {
        PlatformInferenceProvider::new(reqwest::Client::new(), server.uri(), MODEL, BEARER)
    }

    async fn mount(server: &MockServer, status: u16, body: impl Into<String>) {
        Mock::given(method("POST"))
            .and(path(INFER_PATH))
            .respond_with(
                ResponseTemplate::new(status).set_body_raw(body.into(), "application/x-ndjson"),
            )
            .mount(server)
            .await;
    }

    fn a_completion_body() -> String {
        [
            r#"{"text":"Hel"}"#,
            r#"{"text":"lo!"}"#,
            r#"{"finish_reason":"stop","tool_calls":[]}"#,
            "",
        ]
        .join("\n")
    }

    #[tokio::test]
    async fn test_a_completion_folds_chunks_and_terminal() {
        let server = MockServer::start().await;
        mount(&server, 200, a_completion_body()).await;

        let stream = setup(&server)
            .complete_stream(a_request(Some(256)), &a_context())
            .await
            .expect("the call opens");
        let response = collect_completion(stream).await.expect("it folds");

        assert_eq!(response.text, "Hello!");
        assert!(response.tool_calls.is_empty());
        assert_eq!(response.usage.provider, "platform");
        assert_eq!(
            response.usage.total_tokens, 0,
            "a platform terminal carries no counts",
        );
    }

    #[tokio::test]
    async fn test_the_request_presents_the_envelope_grant_and_position() {
        let server = MockServer::start().await;
        mount(&server, 200, a_completion_body()).await;

        let _opened = setup(&server)
            .complete_stream(a_request(Some(256)), &a_context())
            .await
            .expect("the call opens");

        let requests = server.received_requests().await.expect("recorded");
        let request = requests.first().expect("one call reached the gateway");
        let sent: InferenceRequest =
            serde_json::from_slice(&request.body).expect("the body is an InferenceRequest");
        assert_eq!(sent.contract_version, GATEWAY_CONTRACT_VERSION);
        assert_eq!(sent.position_key.as_str(), "thread_01:10");
        let InferenceCall::Completion(envelope) = sent.call else {
            panic!("a completion call");
        };
        assert_eq!(envelope.max_tokens, 256);
        assert_eq!(envelope.model.as_str(), MODEL);

        let grant = request
            .headers
            .get(GRANT_SET_HEADER)
            .expect("the grant rides its header");
        let grant: GrantSet = serde_json::from_str(grant.to_str().expect("ascii header"))
            .expect("the header is a GrantSet");
        assert_eq!(grant.account.as_str(), "acct_01");
    }

    #[tokio::test]
    async fn test_a_missing_max_tokens_is_refused_before_dispatch() {
        let server = MockServer::start().await;
        mount(&server, 200, a_completion_body()).await;

        let Err(refusal) = setup(&server)
            .complete_stream(a_request(None), &a_context())
            .await
        else {
            panic!("a metered call without max_tokens must be refused");
        };

        assert!(matches!(refusal, InferenceError::MaxTokensRequired { .. }));
        assert!(
            server
                .received_requests()
                .await
                .expect("recorded")
                .is_empty(),
            "the refusal fires before any HTTP dispatch",
        );
    }

    #[tokio::test]
    async fn test_a_typed_refusal_maps_to_gateway_refused() {
        for (status, body, expected) in [
            (402_u16, r#"{"kind":"over_cap"}"#, GatewayError::OverCap),
            (403, r#"{"kind":"not_entitled"}"#, GatewayError::NotEntitled),
            (422, r#"{"kind":"unpriceable"}"#, GatewayError::Unpriceable),
            (502, r#"{"kind":"failed"}"#, GatewayError::Failed),
            (
                409,
                r#"{"kind":"in_flight","retry_after_ms":1000}"#,
                GatewayError::InFlight {
                    retry_after_ms: 1000,
                },
            ),
        ] {
            let server = MockServer::start().await;
            mount(&server, status, body).await;

            let Err(refusal) = setup(&server)
                .complete_stream(a_request(Some(256)), &a_context())
                .await
            else {
                panic!("a typed refusal must be an error");
            };

            assert!(
                matches!(refusal, InferenceError::GatewayRefused { error } if error == expected),
                "status {status} should map to {expected:?}",
            );
        }
    }

    #[tokio::test]
    async fn test_a_boundary_rejection_maps_to_provider_unavailable() {
        let server = MockServer::start().await;
        mount(&server, 401, "missing access token").await;

        let Err(refusal) = setup(&server)
            .complete_stream(a_request(Some(256)), &a_context())
            .await
        else {
            panic!("a boundary rejection must be an error");
        };

        assert!(matches!(
            refusal,
            InferenceError::ProviderUnavailable { .. }
        ));
    }
}
