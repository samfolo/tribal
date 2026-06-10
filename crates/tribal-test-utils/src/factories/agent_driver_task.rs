use chrono::Utc;
use tribal_domain::{
    AgentDriverTask, AgentDriverTaskId, AgentDriverTaskKind, AgentDriverTaskState, AgentThreadId,
};

define_factory! {
    /// Factory for [`AgentDriverTask`] instances.
    pub struct AgentDriverTaskFactory for AgentDriverTask {
        id: AgentDriverTaskId = AgentDriverTaskId::new(),
        thread_id: AgentThreadId = AgentThreadId::new(),
        kind: AgentDriverTaskKind = AgentDriverTaskKind::Drive,
        state: AgentDriverTaskState = AgentDriverTaskState::Pending,
        attempt: u32 = 0,
        max_attempts: u32 = 3,
        available_at: chrono::DateTime<Utc> = Utc::now(),
        claim_token: Option<uuid::Uuid> = None,
        claimed_by: Option<String> = None,
        claimed_at: Option<chrono::DateTime<Utc>> = None,
        heartbeat_at: Option<chrono::DateTime<Utc>> = None,
        last_error: Option<String> = None,
        created_at: chrono::DateTime<Utc> = Utc::now(),
        updated_at: chrono::DateTime<Utc> = Utc::now(),
        completed_at: Option<chrono::DateTime<Utc>> = None,
    }
}

/// Returns an [`AgentDriverTaskFactory`] with sensible defaults.
pub fn an_agent_driver_task() -> AgentDriverTaskFactory {
    AgentDriverTaskFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let driver = an_agent_driver_task().build();
        assert_eq!(driver.state(), AgentDriverTaskState::Pending);
        assert_eq!(driver.kind(), AgentDriverTaskKind::Drive);
        assert_eq!(driver.attempt(), 0);
    }
}
