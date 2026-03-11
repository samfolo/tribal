//! Mock implementation of [`TriageResultRepository`].

use tribal_db::{NewTriageResult, TriageResultRepository};
use tribal_domain::{JobId, TriageResult};

use super::mock_repository;

mock_repository! {
    MockTriageResultRepository for TriageResultRepository, tribal_db::DbError {
        insert(NewTriageResult => TriageResult)
            (new: &NewTriageResult) { new.clone() };
        find_by_job_id(JobId => Vec<TriageResult>)
            (job_id: JobId) { job_id };
        find_by_job_id_and_batch_index((JobId, u32) => Option<TriageResult>)
            (job_id: JobId, batch_index: u32) { (job_id, batch_index) }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tribal_db::TriageResultRepository;

    use super::*;

    #[test]
    fn test_send_sync_behind_arc() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockTriageResultRepository>();

        let mock = MockTriageResultRepository::builder().build();
        let _arc: Arc<dyn TriageResultRepository + Send + Sync> = Arc::new(mock);
    }
}
