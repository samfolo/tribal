//! Mock implementation of [`JobRepository`].

use tribal_db::{JobRepository, JobStatusTransition, NewJob};
use tribal_domain::{Job, JobId, ProjectId, RelationBatchId};

use super::mock_repository;

mock_repository! {
    MockJobRepository for JobRepository {
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
