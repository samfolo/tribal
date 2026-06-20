use chrono::Utc;
use tribal_domain::{
    AGENT_THREAD_FORMAT_VERSION, AgentBindingVersionId, AgentDriverTaskId, AgentThread,
    AgentThreadId, AgentThreadRecord, AgentThreadRecordId, AgentThreadRecordKind,
    AgentThreadRecordSeq, AgentThreadStatus, AgentThreadSuspension, JobId, PrincipalId, TaskId,
    TaskType,
};

define_factory! {
    /// Factory for [`AgentThread`] instances.
    ///
    /// Defaults satisfy the XOR invariant by driving the thread through a
    /// stage task; clear it and set `driver_task_id` for driver-family
    /// threads.
    pub struct AgentThreadFactory for AgentThread {
        id: AgentThreadId = AgentThreadId::new(),
        parent_thread_id: Option<AgentThreadId> = None,
        pipeline_stage: TaskType = TaskType::Extraction,
        binding_version_id: AgentBindingVersionId = AgentBindingVersionId::new(),
        stage_task_id: Option<TaskId> = Some(TaskId::new()),
        driver_task_id: Option<AgentDriverTaskId> = None,
        principal_id: PrincipalId = PrincipalId::new(),
        job_id: Option<JobId> = None,
        status: AgentThreadStatus = AgentThreadStatus::Queued,
        suspension: Option<AgentThreadSuspension> = None,
        cancel_requested_at: Option<chrono::DateTime<Utc>> = None,
        cancel_requested_by: Option<String> = None,
        recovery_attempts: u32 = 0,
        format_version: u32 = AGENT_THREAD_FORMAT_VERSION,
        wake_at: Option<chrono::DateTime<Utc>> = None,
        fidelity: Option<serde_json::Value> = None,
        execution_spend: Option<serde_json::Value> = None,
        completed_at: Option<chrono::DateTime<Utc>> = None,
        created_at: chrono::DateTime<Utc> = Utc::now(),
        updated_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns an [`AgentThreadFactory`] with sensible defaults.
pub fn an_agent_thread() -> AgentThreadFactory {
    AgentThreadFactory::new()
}

define_factory! {
    /// Factory for [`AgentThreadRecord`] instances.
    pub struct AgentThreadRecordFactory for AgentThreadRecord {
        id: AgentThreadRecordId = AgentThreadRecordId::new(),
        thread_id: AgentThreadId = AgentThreadId::new(),
        seq: AgentThreadRecordSeq = AgentThreadRecordSeq::FIRST,
        kind: AgentThreadRecordKind = AgentThreadRecordKind::Input,
        requesting_seq: Option<AgentThreadRecordSeq> = None,
        tool_call_id: Option<String> = None,
        content: serde_json::Value = serde_json::json!({}),
        usage: Option<serde_json::Value> = None,
        trace_id: Option<String> = None,
        span_id: Option<String> = None,
        committed_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns an [`AgentThreadRecordFactory`] with sensible defaults.
pub fn an_agent_thread_record() -> AgentThreadRecordFactory {
    AgentThreadRecordFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let thread = an_agent_thread().build();
        assert_eq!(thread.status(), AgentThreadStatus::Queued);
        assert_eq!(thread.format_version(), AGENT_THREAD_FORMAT_VERSION);
        assert!(thread.stage_task_id().is_some());
        assert!(thread.driver_task_id().is_none());
    }

    #[test]
    fn test_record_builds_with_defaults() {
        let record = an_agent_thread_record().build();
        assert_eq!(record.seq(), AgentThreadRecordSeq::FIRST);
        assert_eq!(record.kind(), AgentThreadRecordKind::Input);
        assert!(record.requesting_seq().is_none());
    }
}
