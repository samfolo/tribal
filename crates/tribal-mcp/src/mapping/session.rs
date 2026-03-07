use crate::session::SessionContext;

// ---------------------------------------------------------------------------
// Outbound mapping: SessionContext → JSON
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::ProjectId;

    use crate::session::{SessionActor, SessionContext, SessionProject};

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
}
