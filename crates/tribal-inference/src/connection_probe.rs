//! Non-billable reachability probes for reusable provider connections.

use std::time::Duration;

use reqwest::{StatusCode, header::HeaderMap, redirect};
use serde::Deserialize;
use tribal_domain::ProviderKind;

use crate::{
    anthropic::ANTHROPIC_VERSION, http::normalise_base_url, ollama::tags::TAGS_PATH,
    registry::USER_AGENT,
};

const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const MESSAGE_LIMIT: usize = 240;

/// A provider connection probe that does not select or execute a model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderConnectionProbeOutcome {
    /// The endpoint accepted the provider's discovery request.
    Reachable,
    /// The provider connection has no direct endpoint to probe.
    Skipped(ProviderConnectionProbeSkipReason),
    /// The endpoint or credential rejected the discovery request.
    Failed(ProviderConnectionFailure),
}

/// Why a provider connection cannot be probed directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderConnectionProbeSkipReason {
    /// Tribal Platform availability is established by its authenticated gateway.
    ManagedConnection,
}

/// Stable failure classes shared by supported provider error envelopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderConnectionFailureKind {
    Authentication,
    Permission,
    QuotaExhausted,
    RateLimited,
    Overloaded,
    UpstreamUnavailable,
    EndpointUnreachable,
    TimedOut,
    InvalidRequest,
    Unknown,
}

/// Safe diagnostic details from a failed provider connection probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderConnectionFailure {
    pub kind: ProviderConnectionFailureKind,
    pub provider_code: Option<String>,
    pub http_status: Option<u16>,
    pub message: Option<String>,
    pub request_id: Option<String>,
    pub retry_after_seconds: Option<u64>,
}

/// The provider's own error fields, before classification. `error_type` is the
/// provider's `type` discriminator, distinct from the
/// [`ProviderConnectionFailureKind`] this repo classifies it into.
struct ParsedProviderError {
    code: Option<String>,
    error_type: Option<String>,
    message: String,
}

#[derive(Deserialize)]
struct OpenAiErrorEnvelope {
    error: OpenAiError,
}

#[derive(Deserialize)]
struct OpenAiError {
    message: String,
    code: Option<String>,
    #[serde(rename = "type")]
    error_type: Option<String>,
}

#[derive(Deserialize)]
struct AnthropicErrorEnvelope {
    error: AnthropicError,
}

#[derive(Deserialize)]
struct AnthropicError {
    #[serde(rename = "type")]
    error_type: String,
    message: String,
}

#[derive(Deserialize)]
struct OllamaErrorEnvelope {
    error: String,
}

/// Probes a configured provider endpoint through its model-discovery API.
///
/// The probe validates endpoint reachability and credentials without selecting,
/// loading, or billing a model. Configured-model execution remains the
/// responsibility of the inference and embedding readiness checks.
pub async fn probe_provider_connection(
    provider: ProviderKind,
    base_url: Option<&str>,
    api_key: Option<&str>,
) -> ProviderConnectionProbeOutcome {
    if provider == ProviderKind::Platform {
        return ProviderConnectionProbeOutcome::Skipped(
            ProviderConnectionProbeSkipReason::ManagedConnection,
        );
    }
    if provider.requires_api_key() && api_key.is_none_or(str::is_empty) {
        return ProviderConnectionProbeOutcome::Failed(ProviderConnectionFailure {
            kind: ProviderConnectionFailureKind::Authentication,
            provider_code: Some("credential_missing".to_owned()),
            http_status: None,
            message: None,
            request_id: None,
            retry_after_seconds: None,
        });
    }
    let Some(base_url) = base_url else {
        return ProviderConnectionProbeOutcome::Failed(ProviderConnectionFailure {
            kind: ProviderConnectionFailureKind::InvalidRequest,
            provider_code: Some("endpoint_missing".to_owned()),
            http_status: None,
            message: None,
            request_id: None,
            retry_after_seconds: None,
        });
    };

    // Credentials ride custom headers (`x-api-key`), which reqwest carries across a
    // cross-origin redirect — it strips only `Authorization` and friends. The base URL
    // is operator-supplied, so a redirect is a credential-exfiltration path: refuse to
    // follow one and report the 3xx as the endpoint answering wrongly.
    let Ok(client) = reqwest::Client::builder()
        .timeout(PROBE_TIMEOUT)
        .user_agent(USER_AGENT)
        .redirect(redirect::Policy::none())
        .build()
    else {
        return ProviderConnectionProbeOutcome::Failed(ProviderConnectionFailure {
            kind: ProviderConnectionFailureKind::Unknown,
            provider_code: Some("client_build_failed".to_owned()),
            http_status: None,
            message: None,
            request_id: None,
            retry_after_seconds: None,
        });
    };
    probe_with_client(&client, provider, base_url, api_key).await
}

async fn probe_with_client(
    client: &reqwest::Client,
    provider: ProviderKind,
    base_url: &str,
    api_key: Option<&str>,
) -> ProviderConnectionProbeOutcome {
    let endpoint = discovery_endpoint(provider, base_url);
    let mut request = client.get(endpoint);
    match provider {
        ProviderKind::Anthropic => {
            request = request
                .header("x-api-key", api_key.unwrap_or_default())
                .header("anthropic-version", ANTHROPIC_VERSION);
        }
        ProviderKind::OpenAi => {
            request = request.bearer_auth(api_key.unwrap_or_default());
        }
        ProviderKind::Ollama | ProviderKind::Platform => {}
    }

    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            let kind = if error.is_timeout() {
                ProviderConnectionFailureKind::TimedOut
            } else {
                ProviderConnectionFailureKind::EndpointUnreachable
            };
            return ProviderConnectionProbeOutcome::Failed(ProviderConnectionFailure {
                kind,
                provider_code: None,
                http_status: error.status().map(|status| status.as_u16()),
                message: None,
                request_id: None,
                retry_after_seconds: None,
            });
        }
    };
    let status = response.status();
    let headers = response.headers().clone();
    let body = match response.text().await {
        Ok(body) => body,
        Err(error) => {
            return ProviderConnectionProbeOutcome::Failed(ProviderConnectionFailure {
                kind: if status.is_success() {
                    ProviderConnectionFailureKind::EndpointUnreachable
                } else {
                    kind_from_status(status)
                },
                provider_code: None,
                http_status: Some(status.as_u16()),
                message: Some(sanitise_message(&error.to_string())),
                request_id: request_id(provider, &headers),
                retry_after_seconds: retry_after_seconds(&headers),
            });
        }
    };
    if status.is_success() {
        return discovery_outcome(provider, status, &headers, &body);
    }
    ProviderConnectionProbeOutcome::Failed(failure_from_response(provider, status, &headers, &body))
}

fn discovery_endpoint(provider: ProviderKind, base_url: &str) -> String {
    let base_url = normalise_base_url(base_url.to_owned());
    let path = match provider {
        ProviderKind::Ollama => TAGS_PATH,
        ProviderKind::Anthropic | ProviderKind::OpenAi => "/v1/models",
        ProviderKind::Platform => "",
    };
    format!("{base_url}{path}")
}

/// Judges a 2xx as the provider's model catalogue, or as a stranger answering.
///
/// A success status alone proves only that something answered: a proxy, a
/// captive portal, or an unrelated service returns 200 without ever seeing the
/// credential. Reachability is claimed only when the body carries the
/// provider's own catalogue envelope.
fn discovery_outcome(
    provider: ProviderKind,
    status: StatusCode,
    headers: &HeaderMap,
    body: &str,
) -> ProviderConnectionProbeOutcome {
    if carries_discovery_catalogue(provider, body) {
        return ProviderConnectionProbeOutcome::Reachable;
    }
    ProviderConnectionProbeOutcome::Failed(ProviderConnectionFailure {
        kind: ProviderConnectionFailureKind::InvalidRequest,
        provider_code: Some("discovery_catalogue_absent".to_owned()),
        http_status: Some(status.as_u16()),
        message: None,
        request_id: request_id(provider, headers),
        retry_after_seconds: None,
    })
}

fn carries_discovery_catalogue(provider: ProviderKind, body: &str) -> bool {
    let Ok(envelope) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    let catalogue = match provider {
        ProviderKind::Ollama => envelope.get("models"),
        ProviderKind::Anthropic | ProviderKind::OpenAi => envelope.get("data"),
        ProviderKind::Platform => return true,
    };
    catalogue.is_some_and(serde_json::Value::is_array)
}

fn failure_from_response(
    provider: ProviderKind,
    status: StatusCode,
    headers: &HeaderMap,
    body: &str,
) -> ProviderConnectionFailure {
    let parsed = parse_error(provider, body);
    let provider_code = parsed
        .as_ref()
        .and_then(|error| error.code.clone().or_else(|| error.error_type.clone()));
    let kind = parsed
        .as_ref()
        .and_then(|error| {
            error
                .code
                .as_deref()
                .and_then(kind_from_code)
                .or_else(|| error.error_type.as_deref().and_then(kind_from_code))
        })
        .unwrap_or_else(|| kind_from_status(status));
    ProviderConnectionFailure {
        kind,
        provider_code,
        http_status: Some(status.as_u16()),
        message: parsed.map(|error| sanitise_message(&error.message)),
        request_id: request_id(provider, headers),
        retry_after_seconds: retry_after_seconds(headers),
    }
}

fn parse_error(provider: ProviderKind, body: &str) -> Option<ParsedProviderError> {
    match provider {
        ProviderKind::OpenAi => {
            serde_json::from_str::<OpenAiErrorEnvelope>(body)
                .ok()
                .map(|envelope| ParsedProviderError {
                    code: envelope.error.code,
                    error_type: envelope.error.error_type,
                    message: envelope.error.message,
                })
        }
        ProviderKind::Anthropic => serde_json::from_str::<AnthropicErrorEnvelope>(body)
            .ok()
            .map(|envelope| ParsedProviderError {
                code: None,
                error_type: Some(envelope.error.error_type),
                message: envelope.error.message,
            }),
        ProviderKind::Ollama => {
            serde_json::from_str::<OllamaErrorEnvelope>(body)
                .ok()
                .map(|envelope| ParsedProviderError {
                    code: None,
                    error_type: None,
                    message: envelope.error,
                })
        }
        ProviderKind::Platform => None,
    }
}

fn kind_from_code(code: &str) -> Option<ProviderConnectionFailureKind> {
    Some(match code {
        "authentication_error" | "invalid_api_key" | "invalid_authentication" => {
            ProviderConnectionFailureKind::Authentication
        }
        "permission_error" | "permission_denied" => ProviderConnectionFailureKind::Permission,
        "billing_error" | "insufficient_quota" => ProviderConnectionFailureKind::QuotaExhausted,
        "rate_limit_error" | "rate_limit_exceeded" => ProviderConnectionFailureKind::RateLimited,
        "overloaded_error" => ProviderConnectionFailureKind::Overloaded,
        "timeout_error" | "request_timeout" => ProviderConnectionFailureKind::TimedOut,
        "invalid_request_error" | "not_found_error" | "request_too_large" => {
            ProviderConnectionFailureKind::InvalidRequest
        }
        "api_error" | "server_error" => ProviderConnectionFailureKind::UpstreamUnavailable,
        _ => return None,
    })
}

fn kind_from_status(status: StatusCode) -> ProviderConnectionFailureKind {
    match status.as_u16() {
        // A redirect reaches here only because the probe refuses to follow one:
        // the configured base URL does not host the discovery API itself.
        300..=399 | 400 | 404 | 409 | 413 | 422 => ProviderConnectionFailureKind::InvalidRequest,
        401 => ProviderConnectionFailureKind::Authentication,
        402 => ProviderConnectionFailureKind::QuotaExhausted,
        403 => ProviderConnectionFailureKind::Permission,
        408 | 504 => ProviderConnectionFailureKind::TimedOut,
        429 => ProviderConnectionFailureKind::RateLimited,
        529 => ProviderConnectionFailureKind::Overloaded,
        500..=599 => ProviderConnectionFailureKind::UpstreamUnavailable,
        _ => ProviderConnectionFailureKind::Unknown,
    }
}

fn request_id(provider: ProviderKind, headers: &HeaderMap) -> Option<String> {
    let names: &[&str] = match provider {
        ProviderKind::OpenAi => &["x-request-id"],
        ProviderKind::Anthropic => &["request-id"],
        ProviderKind::Ollama | ProviderKind::Platform => &[],
    };
    names.iter().find_map(|name| {
        headers
            .get(*name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    })
}

fn retry_after_seconds(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse().ok())
}

fn sanitise_message(message: &str) -> String {
    let normalised = message.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalised.len() <= MESSAGE_LIMIT {
        return normalised;
    }
    let boundary = normalised.floor_char_boundary(MESSAGE_LIMIT);
    format!("{}…", &normalised[..boundary])
}

#[cfg(test)]
mod tests {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path},
    };

    use super::*;

    #[tokio::test]
    async fn test_openai_probe_uses_model_discovery_and_bearer_auth() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .and(header("authorization", "Bearer sk-test"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "object": "list",
                "data": []
            })))
            .mount(&server)
            .await;

        let result = probe_with_client(
            &reqwest::Client::new(),
            ProviderKind::OpenAi,
            &server.uri(),
            Some("sk-test"),
        )
        .await;

        assert_eq!(result, ProviderConnectionProbeOutcome::Reachable);
    }

    #[tokio::test]
    async fn test_anthropic_probe_uses_model_discovery_headers() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .and(header("x-api-key", "sk-ant-test"))
            .and(header("anthropic-version", ANTHROPIC_VERSION))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [],
                "has_more": false
            })))
            .mount(&server)
            .await;

        let result = probe_with_client(
            &reqwest::Client::new(),
            ProviderKind::Anthropic,
            &server.uri(),
            Some("sk-ant-test"),
        )
        .await;

        assert_eq!(result, ProviderConnectionProbeOutcome::Reachable);
    }

    #[tokio::test]
    async fn test_ollama_probe_uses_tags_without_a_model() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/tags"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "models": [] })),
            )
            .mount(&server)
            .await;

        let result = probe_with_client(
            &reqwest::Client::new(),
            ProviderKind::Ollama,
            &server.uri(),
            None,
        )
        .await;

        assert_eq!(result, ProviderConnectionProbeOutcome::Reachable);
    }

    #[tokio::test]
    async fn test_openai_insufficient_quota_is_not_rate_limiting() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("x-request-id", "req_test")
                    .set_body_json(serde_json::json!({
                        "error": {
                            "message": "You exceeded your current quota.",
                            "type": "insufficient_quota",
                            "code": "insufficient_quota"
                        }
                    })),
            )
            .mount(&server)
            .await;

        let result = probe_with_client(
            &reqwest::Client::new(),
            ProviderKind::OpenAi,
            &server.uri(),
            Some("sk-test"),
        )
        .await;

        assert_eq!(
            result,
            ProviderConnectionProbeOutcome::Failed(ProviderConnectionFailure {
                kind: ProviderConnectionFailureKind::QuotaExhausted,
                provider_code: Some("insufficient_quota".to_owned()),
                http_status: Some(429),
                message: Some("You exceeded your current quota.".to_owned()),
                request_id: Some("req_test".to_owned()),
                retry_after_seconds: None,
            })
        );
    }

    #[tokio::test]
    async fn test_anthropic_billing_error_is_typed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(
                ResponseTemplate::new(402)
                    .insert_header("request-id", "req_anthropic")
                    .set_body_json(serde_json::json!({
                        "type": "error",
                        "error": {
                            "type": "billing_error",
                            "message": "Your account requires attention."
                        },
                        "request_id": "req_anthropic"
                    })),
            )
            .mount(&server)
            .await;

        let result = probe_with_client(
            &reqwest::Client::new(),
            ProviderKind::Anthropic,
            &server.uri(),
            Some("sk-ant-test"),
        )
        .await;

        assert!(matches!(
            result,
            ProviderConnectionProbeOutcome::Failed(ProviderConnectionFailure {
                kind: ProviderConnectionFailureKind::QuotaExhausted,
                provider_code: Some(code),
                request_id: Some(request_id),
                ..
            }) if code == "billing_error" && request_id == "req_anthropic"
        ));
    }

    #[tokio::test]
    async fn test_credentials_do_not_follow_a_cross_origin_redirect() {
        let elsewhere = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": []
            })))
            .expect(0)
            .mount(&elsewhere)
            .await;
        let configured = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(302).insert_header(
                "location",
                format!("{}/v1/models", elsewhere.uri()).as_str(),
            ))
            .mount(&configured)
            .await;

        let result = probe_provider_connection(
            ProviderKind::Anthropic,
            Some(&configured.uri()),
            Some("sk-ant-secret"),
        )
        .await;

        // `expect(0)` on the redirect target is the assertion that matters: the
        // credential never left the configured origin. Verified on drop.
        assert!(matches!(
            result,
            ProviderConnectionProbeOutcome::Failed(ProviderConnectionFailure {
                kind: ProviderConnectionFailureKind::InvalidRequest,
                http_status: Some(302),
                ..
            })
        ));
    }

    #[tokio::test]
    async fn test_a_stranger_answering_200_is_not_reachable() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string("<html>Sign in to the WiFi</html>"),
            )
            .mount(&server)
            .await;

        let result = probe_with_client(
            &reqwest::Client::new(),
            ProviderKind::OpenAi,
            &server.uri(),
            Some("sk-test"),
        )
        .await;

        assert!(matches!(
            result,
            ProviderConnectionProbeOutcome::Failed(ProviderConnectionFailure {
                kind: ProviderConnectionFailureKind::InvalidRequest,
                provider_code: Some(code),
                http_status: Some(200),
                ..
            }) if code == "discovery_catalogue_absent"
        ));
    }

    #[tokio::test]
    async fn test_ollama_catalogue_shape_is_not_accepted_for_openai() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "models": [] })),
            )
            .mount(&server)
            .await;

        let result = probe_with_client(
            &reqwest::Client::new(),
            ProviderKind::OpenAi,
            &server.uri(),
            Some("sk-test"),
        )
        .await;

        assert!(matches!(
            result,
            ProviderConnectionProbeOutcome::Failed(ProviderConnectionFailure {
                kind: ProviderConnectionFailureKind::InvalidRequest,
                ..
            })
        ));
    }

    #[test]
    fn test_unknown_body_is_not_exposed_as_raw_json() {
        let failure = failure_from_response(
            ProviderKind::OpenAi,
            StatusCode::BAD_GATEWAY,
            &HeaderMap::new(),
            r#"{"secret":"must-not-cross-the-boundary"}"#,
        );

        assert_eq!(
            failure.kind,
            ProviderConnectionFailureKind::UpstreamUnavailable
        );
        assert_eq!(failure.message, None);
    }

    #[test]
    fn test_unknown_provider_code_falls_back_to_error_type() {
        let failure = failure_from_response(
            ProviderKind::OpenAi,
            StatusCode::NOT_FOUND,
            &HeaderMap::new(),
            r#"{"error":{"message":"No such model.","type":"invalid_request_error","code":"model_not_found"}}"#,
        );

        assert_eq!(failure.kind, ProviderConnectionFailureKind::InvalidRequest);
        assert_eq!(failure.provider_code.as_deref(), Some("model_not_found"));
    }

    #[test]
    fn test_provider_message_is_bounded_without_splitting_utf8() {
        let message = "£".repeat(MESSAGE_LIMIT);
        let result = sanitise_message(&message);
        assert!(result.ends_with('…'));
        assert!(result.len() <= MESSAGE_LIMIT + '…'.len_utf8());
    }
}
