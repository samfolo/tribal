use chrono::Utc;
use tribal_db::NewTask;
use tribal_domain::{JobId, Task, TaskId, TaskStatus, TaskType};

define_factory! {
    /// Factory for [`Task`] instances.
    pub struct TaskFactory for Task {
        id: TaskId = TaskId::new(),
        job_id: JobId = JobId::new(),
        task_type: TaskType = TaskType::Extraction,
        status: TaskStatus = TaskStatus::Queued,
        batch_index: Option<u32> = None,
        claim_token: Option<uuid::Uuid> = None,
        available_at: chrono::DateTime<Utc> = Utc::now(),
        claimed_by: Option<String> = None,
        claimed_at: Option<chrono::DateTime<Utc>> = None,
        heartbeat_at: Option<chrono::DateTime<Utc>> = None,
        retry_count: u32 = 0,
        error_kind: Option<tribal_domain::TaskErrorKind> = None,
        error_message: Option<String> = None,
        created_at: chrono::DateTime<Utc> = Utc::now(),
        updated_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns a [`TaskFactory`] with sensible defaults.
pub fn a_task() -> TaskFactory {
    TaskFactory::new()
}

define_factory! {
    /// Factory for [`NewTask`] instances used in repository insert operations.
    pub struct NewTaskFactory for NewTask {
        job_id: JobId = JobId::new(),
        task_type: TaskType = TaskType::Extraction,
        batch_index: Option<u32> = None,
    }
}

/// Returns a [`NewTaskFactory`] with sensible defaults.
pub fn a_new_task() -> NewTaskFactory {
    NewTaskFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let t = a_task().build();
        assert_eq!(t.task_type(), TaskType::Extraction);
        assert_eq!(t.status(), TaskStatus::Queued);
        assert_eq!(t.retry_count(), 0);
    }

    #[test]
    fn test_new_task_builds_with_defaults() {
        let new = a_new_task().build();
        assert_eq!(new.task_type.as_str(), "extraction");
        assert!(new.batch_index.is_none());
    }
}
