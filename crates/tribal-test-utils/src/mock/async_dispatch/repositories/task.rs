//! Mock implementation of [`TaskRepository`].

use chrono::{DateTime, Utc};
use tribal_db::{NewTask, ReclaimOutcome, TaskRepository, TaskStatusCount};
use tribal_domain::{JobId, Task, TaskErrorKind, TaskId, TaskType};

use super::mock_repository;

mock_repository! {
    MockTaskRepository for TaskRepository, tribal_db::DbError {
        insert(NewTask => Task)
            (new_task: &NewTask) { new_task.clone() };
        find_by_id(TaskId => Task)
            (id: TaskId) { id };
        find_by_job_id(JobId => Vec<Task>)
            (job_id: JobId) { job_id };
        claim((u32, String) => Vec<Task>)
            (limit: u32, claimed_by: &str) { (limit, claimed_by.to_owned()) };
        heartbeat((TaskId, uuid::Uuid) => u64)
            (id: TaskId, claim_token: uuid::Uuid) { (id, claim_token) };
        reset_retry_count((TaskId, uuid::Uuid) => u64)
            (id: TaskId, claim_token: uuid::Uuid) { (id, claim_token) };
        complete((TaskId, uuid::Uuid) => u64)
            (id: TaskId, claim_token: uuid::Uuid) { (id, claim_token) };
        fail((TaskId, uuid::Uuid, u32, DateTime<Utc>, TaskErrorKind, String) => u64)
            (id: TaskId, claim_token: uuid::Uuid, max_retries: u32, available_at: DateTime<Utc>, error_kind: TaskErrorKind, error_message: &str) { (id, claim_token, max_retries, available_at, error_kind, error_message.to_owned()) };
        reclaim_stale((u32, u32, u32, TaskErrorKind, String, Option<u32>) => ReclaimOutcome)
            (timeout_seconds: u32, max_retries: u32, limit: u32, error_kind: TaskErrorKind, error_message: &str, flat_backoff_seconds: Option<u32>) { (timeout_seconds, max_retries, limit, error_kind, error_message.to_owned(), flat_backoff_seconds) };
        lock_stale_thread_driving(u32 => Option<Task>)
            (timeout_seconds: u32) { timeout_seconds };
        requeue_claimed((TaskId, uuid::Uuid, u32, DateTime<Utc>, TaskErrorKind, String) => u64)
            (id: TaskId, claim_token: uuid::Uuid, retry_count: u32, available_at: DateTime<Utc>, error_kind: TaskErrorKind, error_message: &str) { (id, claim_token, retry_count, available_at, error_kind, error_message.to_owned()) };
        reclaim_requeue((TaskId, u32, u32, TaskErrorKind, String) => u64)
            (id: TaskId, retry_count: u32, backoff_seconds: u32, error_kind: TaskErrorKind, error_message: &str) { (id, retry_count, backoff_seconds, error_kind, error_message.to_owned()) };
        count_live_siblings((JobId, TaskType, TaskId) => i64)
            (job_id: JobId, task_type: TaskType, exclude_task_id: TaskId) { (job_id, task_type, exclude_task_id) };
        block((TaskId, uuid::Uuid) => u64)
            (id: TaskId, claim_token: uuid::Uuid) { (id, claim_token) };
        requeue_from_blocked(TaskId => u64)
            (id: TaskId) { id };
        holds_claim((TaskId, uuid::Uuid) => bool)
            (id: TaskId, claim_token: uuid::Uuid) { (id, claim_token) };
        dead_letter_claimed((TaskId, uuid::Uuid, TaskErrorKind, String) => u64)
            (id: TaskId, claim_token: uuid::Uuid, error_kind: TaskErrorKind, error_message: &str) { (id, claim_token, error_kind, error_message.to_owned()) };
        dead_letter_unclaimed((TaskId, TaskErrorKind, String) => u64)
            (id: TaskId, error_kind: TaskErrorKind, error_message: &str) { (id, error_kind, error_message.to_owned()) };
        upsert(NewTask => u64)
            (new_task: &NewTask) { new_task.clone() };
        count_by_status(() => Vec<TaskStatusCount>)
            () { () }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tribal_db::TaskRepository;

    use super::*;

    #[test]
    fn test_send_sync_behind_arc() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockTaskRepository>();

        let mock = MockTaskRepository::builder().build();
        let _arc: Arc<dyn TaskRepository + Send + Sync> = Arc::new(mock);
    }
}
