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
}

// ---------------------------------------------------------------------------
// check_tags
// ---------------------------------------------------------------------------

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
                let found = tags
                    .models
                    .iter()
                    .any(|m| m.name == model || m.name.starts_with(&format!("{model}:")));

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
