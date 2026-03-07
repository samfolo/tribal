//! Mock implementation of [`JobRepository`].

use tribal_db::{JobRepository, JobStatusTransition, NewJob};
use tribal_domain::{Job, JobId, ProjectId, RelationBatchId};

use super::mock_repository;

mock_repository! {
    MockJobRepository for JobRepository, tribal_db::DbError {
        insert(NewJob => Job)
            (new_job: &NewJob) { new_job.clone() };
        find_by_id(JobId => Job)
            (id: JobId) { id };
        find_by_project_id(ProjectId => Vec<Job>)
            (project_id: ProjectId) { project_id };
        update_status((JobId, JobStatusTransition) => Job)
            (id: JobId, transition: &JobStatusTransition) { (id, transition.clone()) };
        update_batch_size((JobId, u32, u32) => Job)
            (id: JobId, batch_size: u32, extraction_original_count: u32) { (id, batch_size, extraction_original_count) };
        set_committed_batch_id((JobId, RelationBatchId) => Option<Job>)
            (id: JobId, batch_id: RelationBatchId) { (id, batch_id) };
        fail_stale_dead_lettered_jobs(() => Vec<JobId>)
            () { () };
        find_stuck_triaging_jobs(() => Vec<JobId>)
            () { () }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tribal_db::JobRepository;
    use tribal_domain::JobStatus;

    use super::*;
    use crate::{
        a_job, a_job_status_transition, mock::async_dispatch::core::ExhaustBehaviour, test_context,
    };

    // -- Constants ----------------------------------------------------------

    const EXPECT_BEGIN: &str = "should begin transaction";

    // -- Tests --------------------------------------------------------------

    #[tokio::test]
    async fn test_tuple_req_update_status_dispatches_and_records_history() {
        let target_id = JobId::new();
        let job = a_job().build();

        let mock = MockJobRepository::builder()
            .on_update_status(job, None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect(EXPECT_BEGIN);

        let transition = a_job_status_transition()
            .status(JobStatus::Extracting)
            .build();

        let _ = mock
            .update_status(&mut tx, target_id, &transition)
            .await
            .unwrap();

        let history = mock.update_status_history();
        assert_eq!(history.len(), 1);

        let (recorded_id, recorded_transition) = &history[0];
        assert_eq!(*recorded_id, target_id);
        assert_eq!(recorded_transition.status, JobStatus::Extracting);
    }

    #[tokio::test]
    async fn test_tuple_req_conditional_matches_on_id() {
        let target_id = JobId::new();
        let other_id = JobId::new();
        let matched_job = a_job().build();
        let fallback_job = a_job().build();

        let mock = MockJobRepository::builder()
            .when_update_status(move |(id, _)| *id == target_id)
            .respond_with(matched_job.clone(), None)
            .on_update_status(fallback_job.clone(), None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect(EXPECT_BEGIN);
        let transition = a_job_status_transition().build();

        let r1 = mock
            .update_status(&mut tx, target_id, &transition)
            .await
            .unwrap();
        assert_eq!(r1.id(), matched_job.id());

        let r2 = mock
            .update_status(&mut tx, other_id, &transition)
            .await
            .unwrap();
        assert_eq!(r2.id(), fallback_job.id());
    }

    #[tokio::test]
    async fn test_triple_tuple_req_update_batch_size_records_all_fields() {
        let job_id = JobId::new();
        let job = a_job().build();

        let mock = MockJobRepository::builder()
            .on_update_batch_size(job, None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect(EXPECT_BEGIN);

        let _ = mock
            .update_batch_size(&mut tx, job_id, 42, 100)
            .await
            .unwrap();

        let history = mock.update_batch_size_history();
        assert_eq!(history.len(), 1);

        let (recorded_id, recorded_batch_size, recorded_original_count) = &history[0];
        assert_eq!(*recorded_id, job_id);
        assert_eq!(*recorded_batch_size, 42);
        assert_eq!(*recorded_original_count, 100);
    }

    #[tokio::test]
    async fn test_no_param_method_dispatches() {
        let id1 = JobId::new();
        let id2 = JobId::new();

        let mock = MockJobRepository::builder()
            .on_fail_stale_dead_lettered_jobs(vec![id1, id2], None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect(EXPECT_BEGIN);

        let result = mock.fail_stale_dead_lettered_jobs(&mut tx).await.unwrap();
        assert_eq!(result, vec![id1, id2]);
        assert_eq!(mock.fail_stale_dead_lettered_jobs_call_count(), 1);
    }

    #[tokio::test]
    async fn test_exhaust_repeat_last_with_tuple_req() {
        let job = a_job().build();

        let mock = MockJobRepository::builder()
            .on_update_status(job.clone(), None)
            .on_update_status_exhaust(ExhaustBehaviour::RepeatLast)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect(EXPECT_BEGIN);
        let transition = a_job_status_transition().build();

        let r1 = mock
            .update_status(&mut tx, JobId::new(), &transition)
            .await
            .unwrap();
        let r2 = mock
            .update_status(&mut tx, JobId::new(), &transition)
            .await
            .unwrap();

        assert_eq!(r1.id(), job.id());
        assert_eq!(r2.id(), job.id());
        assert_eq!(mock.update_status_call_count(), 2);
    }

    #[test]
    fn test_send_sync_behind_arc() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockJobRepository>();

        let mock = MockJobRepository::builder().build();
        let _arc: Arc<dyn JobRepository + Send + Sync> = Arc::new(mock);
    }
}
