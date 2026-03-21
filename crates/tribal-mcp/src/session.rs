use rmcp::{
    model::{AnnotateAble, RawResource, Resource, ResourceUpdatedNotificationParam},
    service::{Peer, RoleServer},
};
use tokio::sync::RwLock;
use tribal_domain::{GitRemote, ProjectId};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const SESSION_RESOURCE_URI: &str = "tribal://session/context";

// ---------------------------------------------------------------------------
// SessionProject
// ---------------------------------------------------------------------------

/// Project identity stored on the session.
///
/// Holds a domain-level `ProjectId` whose underlying value is a UUID. When
/// rendered as a string (including in MCP JSON), it uses the `proj_<uuid>` form.
#[derive(Debug, PartialEq)]
pub struct SessionProject {
    pub id: ProjectId,
    pub name: String,
    pub git_remote: GitRemote,
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
    pub(crate) actor: SessionActor,
    pub(crate) subscribed: bool,
}

impl SessionContext {
    /// Creates a new session with the given project.
    ///
    /// Actor fields default to `None`; subscription starts inactive.
    #[must_use]
    pub fn new(project: Option<SessionProject>) -> Self {
        Self {
            project,
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
        .with_description("Current session context: active project, principal, and agent identity")
        .with_mime_type("application/json")
        .no_annotation()
}

// ---------------------------------------------------------------------------
// Notification helper
// ---------------------------------------------------------------------------

/// Sends a `notifications/resources/updated` for `tribal://session/context`
/// if the client has subscribed. Best-effort — the notification is awaited
/// but its result is silently discarded.
pub(crate) async fn notify_session_updated(
    session: &RwLock<SessionContext>,
    peer: &Peer<RoleServer>,
) {
    let subscribed = { session.read().await.subscribed };
    if subscribed {
        let _ = peer
            .notify_resource_updated(ResourceUpdatedNotificationParam::new(SESSION_RESOURCE_URI))
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
        let ctx = SessionContext::new(None);

        assert!(ctx.project.is_none());
        assert!(ctx.actor.client_name.is_none());
        assert!(ctx.actor.client_version.is_none());
        assert!(ctx.actor.model.is_none());
        assert!(ctx.actor.provider.is_none());
        assert!(!ctx.subscribed);
    }

    #[test]
    fn test_resolved_project_id_none() {
        let ctx = SessionContext::new(None);
        assert!(ctx.resolved_project_id().is_none());
    }

    #[test]
    fn test_resolved_project_id_some() {
        let id = ProjectId::new();
        let project = SessionProject {
            id,
            name: "tribal".into(),
            git_remote: "git@github.com:user/tribal.git"
                .parse()
                .expect("valid test git remote"),
        };
        let ctx = SessionContext::new(Some(project));
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
