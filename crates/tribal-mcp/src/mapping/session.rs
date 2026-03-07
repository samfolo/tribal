//! MCP request and response types for `tribal_set_context`, plus the
//! existing raw JSON conversion for the session resource.

use rmcp::model::{CallToolResult, Content, RawContent};
use serde::{Deserialize, Serialize};

use crate::{error::IntoCallToolResult, session::SessionContext};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERIALISE_SET_CONTEXT_RESPONSE: &str =
    "McpSetContextResponse should always serialise successfully";

// ---------------------------------------------------------------------------
// Outbound mapping: SessionContext → JSON (session resource)
// ---------------------------------------------------------------------------

impl From<&SessionContext> for serde_json::Value {
    fn from(ctx: &SessionContext) -> Self {
        let project = ctx.project.as_ref().map_or(serde_json::Value::Null, |p| {
            serde_json::json!({
                "id": p.id.to_string(),
                "name": p.name,
                "git_remote": p.git_remote,
            })
        });

        serde_json::json!({
            "project": project,
            "principal_key": ctx.principal_key,
            "actor": {
                "client_name": ctx.actor.client_name,
                "client_version": ctx.actor.client_version,
                "model": ctx.actor.model,
                "provider": ctx.actor.provider,
            },
        })
    }
}

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_set_context` input.
#[derive(Debug, Deserialize)]
pub(crate) struct McpSetContextRequest {
    pub(crate) project_id: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) provider: Option<String>,
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Project on the MCP set_context response surface.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct McpSessionProject {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) git_remote: String,
}

/// Actor metadata on the MCP set_context response surface.
///
/// All fields are always present in serialised JSON (null when absent),
/// matching the existing session resource representation.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct McpSessionActor {
    pub(crate) client_name: Option<String>,
    pub(crate) client_version: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) provider: Option<String>,
}

/// Typed response for `tribal_set_context`.
///
/// The `project` field is non-required in the output schema but always
/// included (null when absent) for consistency with the session resource
/// representation and Tool Surface §3.2.
#[derive(Debug, Serialize)]
pub(crate) struct McpSetContextResponse {
    pub(crate) project: Option<McpSessionProject>,
    pub(crate) principal_key: String,
    pub(crate) actor: McpSessionActor,
}

impl From<&SessionContext> for McpSetContextResponse {
    fn from(ctx: &SessionContext) -> Self {
        Self {
            project: ctx.project.as_ref().map(|p| McpSessionProject {
                id: p.id.to_string(),
                name: p.name.clone(),
                git_remote: p.git_remote.clone(),
            }),
            principal_key: ctx.principal_key.clone(),
            actor: McpSessionActor {
                client_name: ctx.actor.client_name.clone(),
                client_version: ctx.actor.client_version.clone(),
                model: ctx.actor.model.clone(),
                provider: ctx.actor.provider.clone(),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// IntoCallToolResult
// ---------------------------------------------------------------------------

impl IntoCallToolResult for McpSetContextResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let mut parts = Vec::new();

        if let Some(ref project) = self.project {
            parts.push(format!("project: {}", project.name));
        }
        if let Some(ref model) = self.actor.model {
            parts.push(format!("model: {model}"));
        }
        if let Some(ref provider) = self.actor.provider {
            parts.push(format!("provider: {provider}"));
        }

        let text = if parts.is_empty() {
            "Context updated".to_owned()
        } else {
            format!("Context updated ({})", parts.join(", "))
        };

        let structured = serde_json::to_value(&self).expect(SERIALISE_SET_CONTEXT_RESPONSE);
        let mut result = CallToolResult::success(vec![Content::text(text)]);
        result.structured_content = Some(structured);
        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::ProjectId;

    use super::*;
    use crate::session::{SessionActor, SessionContext, SessionProject};

    // -- Existing resource JSON tests -------------------------------------

    #[test]
    fn test_session_json_with_project() {
        let id = ProjectId::new();
        let ctx = SessionContext::new(
            Some(SessionProject {
                id,
                name: "tribal".into(),
                git_remote: "git@github.com:user/tribal.git".into(),
            }),
            "user:sam".into(),
        );

        let json: serde_json::Value = (&ctx).into();

        let project = &json["project"];
        assert_eq!(project["id"], id.to_string());
        assert_eq!(project["name"], "tribal");
        assert_eq!(project["git_remote"], "git@github.com:user/tribal.git");
        assert_eq!(json["principal_key"], "user:sam");
    }

    #[test]
    fn test_session_json_without_project() {
        let ctx = SessionContext::new(None, "user:sam".into());
        let json: serde_json::Value = (&ctx).into();

        assert!(json["project"].is_null());
        assert_eq!(json["principal_key"], "user:sam");
    }

    #[test]
    fn test_session_json_actor_fields() {
        let mut ctx = SessionContext::new(None, "user:sam".into());
        ctx.actor = SessionActor {
            client_name: Some("claude-code".into()),
            client_version: None,
            model: Some("claude-opus-4-6".into()),
            provider: None,
        };

        let json: serde_json::Value = (&ctx).into();
        let actor = &json["actor"];

        assert_eq!(actor["client_name"], "claude-code");
        assert!(actor["client_version"].is_null());
        assert_eq!(actor["model"], "claude-opus-4-6");
        assert!(actor["provider"].is_null());
    }

    // -- McpSetContextRequest ---------------------------------------------

    #[test]
    fn test_set_context_request_deserialises_empty() {
        let json = serde_json::json!({});
        let req: McpSetContextRequest = serde_json::from_value(json).expect("deserialises");
        assert!(req.project_id.is_none());
        assert!(req.model.is_none());
        assert!(req.provider.is_none());
    }

    // -- McpSetContextResponse --------------------------------------------

    #[test]
    fn test_set_context_response_from_session_with_project() {
        let id = ProjectId::new();
        let ctx = SessionContext::new(
            Some(SessionProject {
                id,
                name: "tribal".into(),
                git_remote: "git@github.com:user/tribal.git".into(),
            }),
            "user:sam".into(),
        );
        let resp = McpSetContextResponse::from(&ctx);
        let json = serde_json::to_value(&resp).expect("serialises");

        assert_eq!(json["project"]["id"], id.to_string());
        assert_eq!(json["project"]["name"], "tribal");
        assert_eq!(json["principal_key"], "user:sam");
    }

    #[test]
    fn test_set_context_response_from_session_without_project() {
        let ctx = SessionContext::new(None, "user:sam".into());
        let resp = McpSetContextResponse::from(&ctx);
        let json = serde_json::to_value(&resp).expect("serialises");

        // project is always present — null when absent
        assert!(json.get("project").is_some());
        assert!(json["project"].is_null());
        assert_eq!(json["principal_key"], "user:sam");
    }

    #[test]
    fn test_set_context_response_into_call_tool_result() {
        let mut ctx = SessionContext::new(
            Some(SessionProject {
                id: ProjectId::new(),
                name: "tribal".into(),
                git_remote: "git@github.com:user/tribal.git".into(),
            }),
            "user:sam".into(),
        );
        ctx.actor = SessionActor {
            client_name: None,
            client_version: None,
            model: Some("claude-opus-4-6".into()),
            provider: Some("anthropic".into()),
        };

        let resp = McpSetContextResponse::from(&ctx);
        let result = resp.into_call_tool_result();
        assert_eq!(result.is_error, Some(false));

        let RawContent::Text(t) = &result.content[0].raw else {
            panic!("expected text content");
        };
        let text = &t.text;
        assert!(text.contains("project: tribal"));
        assert!(text.contains("model: claude-opus-4-6"));
        assert!(text.contains("provider: anthropic"));
    }
}
