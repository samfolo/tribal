use rmcp::model::{
    AnnotateAble, RawResource, Resource, ResourceUpdatedNotificationParam,
};
use rmcp::service::{Peer, RoleServer};
use tokio::sync::RwLock;
use tribal_domain::ProjectId;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const SESSION_RESOURCE_URI: &str = "tribal://session/context";

// ---------------------------------------------------------------------------
// SessionProject
// ---------------------------------------------------------------------------

/// Project identity stored on the session.
///
/// Holds a domain-level `ProjectId` (UUID) — the `proj_` prefix is applied
/// only at the MCP mapping layer when serialising outbound JSON.
pub struct SessionProject {
    pub id: ProjectId,
    pub name: String,
    pub git_remote: String,
}

// ---------------------------------------------------------------------------
// SessionActor
// ---------------------------------------------------------------------------

/// Agent identity fields declared by the connected client.
pub struct SessionActor {
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
}

// ---------------------------------------------------------------------------
// SessionContext
// ---------------------------------------------------------------------------

/// Per-connection session state held on `TribalServerHandler`.
///
/// Wrapped in `Arc<RwLock<SessionContext>>` — read-locked by resource reads,
/// write-locked by `tribal_set_context`.
pub struct SessionContext {
    pub(crate) project: Option<SessionProject>,
    pub(crate) principal_key: String,
    pub(crate) actor: SessionActor,
    pub(crate) subscribed: bool,
}

impl SessionContext {
    /// Creates a new session with the given project and principal.
    ///
    /// Actor fields default to `None`; subscription starts inactive.
    #[must_use]
    pub fn new(project: Option<SessionProject>, principal_key: String) -> Self {
        Self {
            project,
            principal_key,
            actor: SessionActor {
                client_name: None,
                client_version: None,
                model: None,
                provider: None,
            },
            subscribed: false,
        }
    }

    /// Returns the active project's ID, if a project is set.
    #[must_use]
    pub fn resolved_project_id(&self) -> Option<ProjectId> {
        self.project.as_ref().map(|p| p.id)
    }
}

// ---------------------------------------------------------------------------
// Resource descriptor
// ---------------------------------------------------------------------------

/// Builds the static MCP resource descriptor for `tribal://session/context`.
#[must_use]
pub(crate) fn session_resource() -> Resource {
    RawResource::new(SESSION_RESOURCE_URI, "session_context")
        .with_description(
            "Current session context: active project, principal, and agent identity",
        )
        .with_mime_type("application/json")
        .no_annotation()
}

// ---------------------------------------------------------------------------
// Notification helper
// ---------------------------------------------------------------------------

/// Sends a `notifications/resources/updated` for `tribal://session/context`
/// if the client has subscribed. Fire-and-forget — notification failure is
/// silently ignored.
pub(crate) async fn notify_session_updated(
    session: &RwLock<SessionContext>,
    peer: &Peer<RoleServer>,
) {
    let subscribed = { session.read().await.subscribed };
    if subscribed {
        let _ = peer
            .notify_resource_updated(ResourceUpdatedNotificationParam::new(
                SESSION_RESOURCE_URI,
            ))
            .await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_context_new_defaults() {
        let ctx = SessionContext::new(None, "user:sam".into());

        assert!(ctx.project.is_none());
        assert_eq!(ctx.principal_key, "user:sam");
        assert!(ctx.actor.client_name.is_none());
        assert!(ctx.actor.client_version.is_none());
        assert!(ctx.actor.model.is_none());
        assert!(ctx.actor.provider.is_none());
        assert!(!ctx.subscribed);
    }

    #[test]
    fn test_resolved_project_id_none() {
        let ctx = SessionContext::new(None, "user:sam".into());
        assert!(ctx.resolved_project_id().is_none());
    }

    #[test]
    fn test_resolved_project_id_some() {
        let id = ProjectId::new();
        let project = SessionProject {
            id,
            name: "tribal".into(),
            git_remote: "git@github.com:user/tribal.git".into(),
        };
        let ctx = SessionContext::new(Some(project), "user:sam".into());
        assert_eq!(ctx.resolved_project_id(), Some(id));
    }

    #[test]
    fn test_session_resource_descriptor() {
        let resource = session_resource();
        assert_eq!(resource.uri, SESSION_RESOURCE_URI);
        assert_eq!(resource.name, "session_context");
        assert_eq!(
            resource.description.as_deref(),
            Some("Current session context: active project, principal, and agent identity"),
        );
        assert_eq!(resource.mime_type.as_deref(), Some("application/json"));
        assert!(resource.annotations.is_none());
    }
}
