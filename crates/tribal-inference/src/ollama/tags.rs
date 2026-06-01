//! Best-effort model availability check via Ollama's `/api/tags` endpoint.

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const TAGS_PATH: &str = "/api/tags";

// ---------------------------------------------------------------------------
// Private serde types
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct OllamaTagsResponse {
    models: Vec<OllamaTagModel>,
}

#[derive(serde::Deserialize)]
struct OllamaTagModel {
    name: String,
    /// Content-addressed sha256 digest; changes on a retag or weight update.
    #[serde(default)]
    digest: String,
}

// ---------------------------------------------------------------------------
// check_tags
// ---------------------------------------------------------------------------

/// Whether an `/api/tags` entry names `model`, accepting an exact match or a
/// `model:tag` variant.
fn names_model(entry_name: &str, model: &str) -> bool {
    entry_name == model || entry_name.starts_with(&format!("{model}:"))
}

/// Resolves the content-addressed revision token for `model` from `/api/tags`.
///
/// The digest changes on a retag or weight update, so it is the cheapest
/// reliable geometry-drift signal a self-hosted Ollama exposes. Best-effort:
/// returns an empty string when the endpoint is unreachable, the model is
/// absent, or it reports no digest, leaving the probe digest as the backstop.
pub async fn resolve_revision_token(
    client: &reqwest::Client,
    base_url: &str,
    model: &str,
) -> String {
    let url = format!("{base_url}{TAGS_PATH}");

    let response = match client.get(&url).send().await {
        Ok(response) if response.status().is_success() => response,
        Ok(response) => {
            tracing::warn!(
                model,
                status = %response.status(),
                "{TAGS_PATH} returned non-success resolving revision token",
            );
            return String::new();
        }
        Err(e) => {
            tracing::warn!(model, error = %e, "{TAGS_PATH} unreachable resolving revision token");
            return String::new();
        }
    };

    match response.json::<OllamaTagsResponse>().await {
        Ok(tags) => tags
            .models
            .into_iter()
            .find(|m| names_model(&m.name, model))
            .map(|m| m.digest)
            .unwrap_or_default(),
        Err(e) => {
            tracing::warn!(
                model,
                error = %e,
                "failed to deserialise {TAGS_PATH} resolving revision token",
            );
            String::new()
        }
    }
}

/// Best-effort GET to `/api/tags` to check whether a model is locally
/// available.
///
/// Never returns an error — warnings are logged and the caller proceeds
/// regardless.
pub(super) async fn check_tags(client: &reqwest::Client, base_url: &str, model: &str) {
    let url = format!("{base_url}{TAGS_PATH}");
    let result = client.get(&url).send().await;

    match result {
        Ok(resp) if resp.status().is_success() => match resp.json::<OllamaTagsResponse>().await {
            Ok(tags) => {
                let found = tags.models.iter().any(|m| names_model(&m.name, model));

                if !found {
                    tracing::warn!(
                        model = %model,
                        "model not found in {TAGS_PATH} — ensure it has been pulled",
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to deserialise {TAGS_PATH} response (best-effort check)",
                );
            }
        },
        Ok(resp) => {
            tracing::warn!(
                status = %resp.status(),
                "{TAGS_PATH} returned non-success status",
            );
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "{TAGS_PATH} unreachable (best-effort check)",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;

    async fn mount_tags(models: serde_json::Value) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(TAGS_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "models": models })),
            )
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn test_resolve_revision_token_returns_the_models_digest() {
        let server = mount_tags(serde_json::json!([
            { "name": "nomic-embed-text:v1.5", "digest": "sha256:abc123" }
        ]))
        .await;

        let token = resolve_revision_token(
            &reqwest::Client::new(),
            &server.uri(),
            "nomic-embed-text:v1.5",
        )
        .await;
        assert_eq!(token, "sha256:abc123");
    }

    #[tokio::test]
    async fn test_resolve_revision_token_matches_a_tag_variant() {
        let server = mount_tags(serde_json::json!([
            { "name": "nomic-embed-text:latest", "digest": "sha256:deadbeef" }
        ]))
        .await;

        let token =
            resolve_revision_token(&reqwest::Client::new(), &server.uri(), "nomic-embed-text")
                .await;
        assert_eq!(token, "sha256:deadbeef");
    }

    #[tokio::test]
    async fn test_resolve_revision_token_empty_when_model_absent() {
        let server = mount_tags(serde_json::json!([
            { "name": "some-other-model", "digest": "sha256:x" }
        ]))
        .await;

        let token =
            resolve_revision_token(&reqwest::Client::new(), &server.uri(), "nomic-embed-text")
                .await;
        assert!(token.is_empty());
    }

    #[tokio::test]
    async fn test_resolve_revision_token_empty_on_non_success() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(TAGS_PATH))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let token =
            resolve_revision_token(&reqwest::Client::new(), &server.uri(), "nomic-embed-text")
                .await;
        assert!(token.is_empty());
    }
}
