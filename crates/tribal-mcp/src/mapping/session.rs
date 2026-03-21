//! MCP request and response types for `tribal_set_context`, plus the
//! existing raw JSON conversion for the session resource.

use rmcp::model::{CallToolResult, Content};
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

/// Renders a [`SessionContext`] as JSON for the session resource.
///
/// The `principal_key` is sourced from the authenticated principal on
/// the handler, not from the session itself.
pub(crate) fn session_to_json(ctx: &SessionContext, principal_key: &str) -> serde_json::Value {
    let project = ctx.project.as_ref().map_or(serde_json::Value::Null, |p| {
        serde_json::json!({
            "id": p.id.to_string(),
            "name": p.name,
            "git_remote": p.git_remote.to_string(),
        })
    });

    serde_json::json!({
        "project": project,
        "principal_key": principal_key,
        "actor": {
            "client_name": ctx.actor.client_name,
            "client_version": ctx.actor.client_version,
            "model": ctx.actor.model,
            "provider": ctx.actor.provider,
        },
    })
}

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_set_context` input.
#[derive(Debug, Default, Deserialize, PartialEq)]
pub(crate) struct McpSetContextRequest {
    pub(crate) project_id: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) provider: Option<String>,
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Project on the MCP `set_context` response surface.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct McpSessionProject {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) git_remote: String,
}

/// Actor metadata on the MCP `set_context` response surface.
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
    #[serde(skip)]
    pub(crate) mutated: bool,
}

/// Builds an [`McpSetContextResponse`] from session state and the
/// authenticated principal key.
pub(crate) fn set_context_response(
    ctx: &SessionContext,
    principal_key: &str,
) -> McpSetContextResponse {
    McpSetContextResponse {
        project: ctx.project.as_ref().map(|p| McpSessionProject {
            id: p.id.to_string(),
            name: p.name.clone(),
            git_remote: p.git_remote.to_string(),
        }),
        principal_key: principal_key.to_owned(),
        actor: McpSessionActor {
            client_name: ctx.actor.client_name.clone(),
            client_version: ctx.actor.client_version.clone(),
            model: ctx.actor.model.clone(),
            provider: ctx.actor.provider.clone(),
        },
        mutated: false,
    }
}

// ---------------------------------------------------------------------------
// IntoCallToolResult
// ---------------------------------------------------------------------------

impl IntoCallToolResult for McpSetContextResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let text = if self.mutated {
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

            if parts.is_empty() {
                "Context updated".to_owned()
            } else {
                format!("Context updated ({})", parts.join(", "))
            }
        } else {
            "Context unchanged".to_owned()
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
    use rmcp::model::RawContent;
    use tribal_domain::ProjectId;

    use super::*;
    use crate::session::{SessionActor, SessionContext, SessionProject};

    // -- Existing resource JSON tests -------------------------------------

    #[test]
    fn test_session_json_with_project() {
        let id = ProjectId::new();
        let ctx = SessionContext::new(Some(SessionProject {
            id,
            name: "tribal".into(),
            git_remote: "git@github.com:user/tribal.git"
                .parse()
                .expect("valid test git remote"),
        }));

        let json = session_to_json(&ctx, "user:sam");

        let project = &json["project"];
        assert_eq!(project["id"], id.to_string());
        assert_eq!(project["name"], "tribal");
        assert_eq!(project["git_remote"], "github.com/user/tribal");
        assert_eq!(json["principal_key"], "user:sam");
    }

    #[test]
    fn test_session_json_without_project() {
        let ctx = SessionContext::new(None);
        let json = session_to_json(&ctx, "user:sam");

        assert!(json["project"].is_null());
        assert_eq!(json["principal_key"], "user:sam");
    }

    #[test]
    fn test_session_json_actor_fields() {
        let mut ctx = SessionContext::new(None);
        ctx.actor = SessionActor {
            client_name: Some("claude-code".into()),
            client_version: None,
            model: Some("claude-opus-4-6".into()),
            provider: None,
        };

        let json = session_to_json(&ctx, "user:sam");
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
    fn test_set_context_response_with_project() {
        let id = ProjectId::new();
        let ctx = SessionContext::new(Some(SessionProject {
            id,
            name: "tribal".into(),
            git_remote: "git@github.com:user/tribal.git"
                .parse()
                .expect("valid test git remote"),
        }));
        let resp = set_context_response(&ctx, "user:sam");
        let json = serde_json::to_value(&resp).expect("serialises");

        assert_eq!(json["project"]["id"], id.to_string());
        assert_eq!(json["project"]["name"], "tribal");
        assert_eq!(json["principal_key"], "user:sam");
    }

    #[test]
    fn test_set_context_response_without_project() {
        let ctx = SessionContext::new(None);
        let resp = set_context_response(&ctx, "user:sam");
        let json = serde_json::to_value(&resp).expect("serialises");

        // project is always present — null when absent
        assert!(json.get("project").is_some());
        assert!(json["project"].is_null());
        assert_eq!(json["principal_key"], "user:sam");
    }

    #[test]
    fn test_set_context_response_into_call_tool_result() {
        let mut ctx = SessionContext::new(Some(SessionProject {
            id: ProjectId::new(),
            name: "tribal".into(),
            git_remote: "git@github.com:user/tribal.git"
                .parse()
                .expect("valid test git remote"),
        }));
        ctx.actor = SessionActor {
            client_name: None,
            client_version: None,
            model: Some("claude-opus-4-6".into()),
            provider: Some("anthropic".into()),
        };

        let mut resp = set_context_response(&ctx, "user:sam");
        resp.mutated = true;
        let result = resp.into_call_tool_result();
        assert_eq!(result.is_error, Some(false));

        assert!(
            matches!(&result.content[0].raw, RawContent::Text(t) if t.text.contains("project: tribal")),
        );
        assert!(
            matches!(&result.content[0].raw, RawContent::Text(t) if t.text.contains("model: claude-opus-4-6")),
        );
        assert!(
            matches!(&result.content[0].raw, RawContent::Text(t) if t.text.contains("provider: anthropic")),
        );
    }

    #[test]
    fn test_set_context_response_unchanged_text() {
        let ctx = SessionContext::new(None);
        let resp = set_context_response(&ctx, "user:sam");
        let result = resp.into_call_tool_result();

        assert!(
            matches!(&result.content[0].raw, RawContent::Text(t) if t.text == "Context unchanged"),
        );
    }
}
