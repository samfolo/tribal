//! Wire DTOs for the reindex operator tools.

use serde::{Deserialize, Serialize};

/// The MCP request for `tribal_reindex`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpReindexRequest {
    /// The target embedding provider (for example `"ollama"` or `"openai"`).
    pub provider: String,
    /// The target embedding model.
    pub model: String,
    /// The target vector dimension; resolved from the model when omitted.
    #[serde(default)]
    pub dimensions: Option<u32>,
    /// The target endpoint; the provider's default when omitted.
    #[serde(default)]
    pub base_url: Option<String>,
    /// When true, resolve and estimate the target without creating a run.
    #[serde(default)]
    pub dry_run: bool,
}

/// The MCP response for `tribal_reindex`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpReindexResponse {
    /// The outcome: `plan` for a dry run, otherwise `created`, `unchanged`,
    /// `already_live`, or `lock_contended`.
    pub outcome: String,
    /// The run id, present for `created` and `already_live`.
    pub run_id: Option<String>,
    /// The resolved target provider.
    pub provider: String,
    /// The resolved target model.
    pub model: String,
    /// The resolved target dimension.
    pub dimensions: u32,
    /// The resolved, normalised target endpoint.
    pub base_url: String,
    /// The number of items the new geometry must embed.
    pub estimated_items: u64,
    /// The number of tags the new geometry must embed.
    pub estimated_tags: u64,
}

/// The MCP response for `tribal_reindex_cancel`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpReindexCancelResponse {
    /// Whether a live run was transitioned to aborted.
    pub cancelled: bool,
    /// The aborted run's id, present only when a run was cancelled.
    pub run_id: Option<String>,
}

/// The MCP response for `tribal_reindex_prune`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpReindexPruneResponse {
    /// The number of profiles transitioned to superseded.
    pub profiles_superseded: u64,
    /// The number of item embeddings deleted.
    pub embeddings_deleted: u64,
    /// The number of tag embeddings deleted.
    pub tag_embeddings_deleted: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reindex_request_deserialises_with_defaults() {
        let json = r#"{"provider":"ollama","model":"nomic-embed-text"}"#;
        let request: McpReindexRequest = serde_json::from_str(json).unwrap();
        assert_eq!(
            request,
            McpReindexRequest {
                provider: "ollama".to_owned(),
                model: "nomic-embed-text".to_owned(),
                dimensions: None,
                base_url: None,
                dry_run: false,
            }
        );
    }

    #[test]
    fn reindex_request_deserialises_full() {
        let json = r#"{
            "provider": "openai",
            "model": "text-embedding-3-small",
            "dimensions": 1536,
            "base_url": "https://api.openai.com/v1",
            "dry_run": true
        }"#;
        let request: McpReindexRequest = serde_json::from_str(json).unwrap();
        assert_eq!(
            request,
            McpReindexRequest {
                provider: "openai".to_owned(),
                model: "text-embedding-3-small".to_owned(),
                dimensions: Some(1536),
                base_url: Some("https://api.openai.com/v1".to_owned()),
                dry_run: true,
            }
        );
    }

    #[test]
    fn reindex_request_round_trips() {
        let request = McpReindexRequest {
            provider: "ollama".to_owned(),
            model: "nomic-embed-text".to_owned(),
            dimensions: Some(768),
            base_url: Some("http://localhost:11434".to_owned()),
            dry_run: false,
        };
        let json = serde_json::to_string(&request).unwrap();
        let decoded: McpReindexRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(request, decoded);
    }

    #[test]
    fn reindex_response_serialises() {
        let response = McpReindexResponse {
            outcome: "created".to_owned(),
            run_id: Some("run-123".to_owned()),
            provider: "ollama".to_owned(),
            model: "nomic-embed-text".to_owned(),
            dimensions: 768,
            base_url: "http://localhost:11434".to_owned(),
            estimated_items: 42,
            estimated_tags: 7,
        };
        let value = serde_json::to_value(&response).unwrap();
        assert_eq!(value["outcome"], "created");
        assert_eq!(value["run_id"], "run-123");
        assert_eq!(value["provider"], "ollama");
        assert_eq!(value["model"], "nomic-embed-text");
        assert_eq!(value["dimensions"], 768);
        assert_eq!(value["base_url"], "http://localhost:11434");
        assert_eq!(value["estimated_items"], 42);
        assert_eq!(value["estimated_tags"], 7);
    }

    #[test]
    fn reindex_response_round_trips() {
        let response = McpReindexResponse {
            outcome: "plan".to_owned(),
            run_id: None,
            provider: "openai".to_owned(),
            model: "text-embedding-3-small".to_owned(),
            dimensions: 1536,
            base_url: "https://api.openai.com/v1".to_owned(),
            estimated_items: 1000,
            estimated_tags: 250,
        };
        let json = serde_json::to_string(&response).unwrap();
        let decoded: McpReindexResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(response, decoded);
    }

    #[test]
    fn reindex_cancel_response_round_trips() {
        let response = McpReindexCancelResponse {
            cancelled: true,
            run_id: Some("run-456".to_owned()),
        };
        let json = serde_json::to_string(&response).unwrap();
        let decoded: McpReindexCancelResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(response, decoded);

        let none = McpReindexCancelResponse {
            cancelled: false,
            run_id: None,
        };
        let value = serde_json::to_value(&none).unwrap();
        assert_eq!(value["cancelled"], false);
        assert!(value["run_id"].is_null());
    }

    #[test]
    fn reindex_prune_response_round_trips() {
        let response = McpReindexPruneResponse {
            profiles_superseded: 3,
            embeddings_deleted: 120,
            tag_embeddings_deleted: 45,
        };
        let json = serde_json::to_string(&response).unwrap();
        let decoded: McpReindexPruneResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(response, decoded);

        let value = serde_json::to_value(&response).unwrap();
        assert_eq!(value["profiles_superseded"], 3);
        assert_eq!(value["embeddings_deleted"], 120);
        assert_eq!(value["tag_embeddings_deleted"], 45);
    }
}
