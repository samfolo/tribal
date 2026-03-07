//! Stub repository implementations for unit tests that need trait
//! bounds satisfied without a live database.
//!
//! Every method is `unimplemented!()` — these exist solely to satisfy
//! trait objects in test harnesses where the repository methods are
//! never called.

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_db::{
    DbError, JobRepository, JobStatusTransition, KnowledgeItemRepository, NewJob, NewKnowledgeItem,
    NewProject, NewRetrievalFeedback, ProjectRepository, RetrievalFeedbackRepository,
    SemanticSearchParams, SemanticSearchResponse,
};
use tribal_domain::{
    Job, JobId, KnowledgeItem, KnowledgeItemId, Project, ProjectId, RelationBatchId,
    RetrievalFeedback, RetrievalFeedbackId,
};

// ---------------------------------------------------------------------------
// StubKnowledgeItemRepository
// ---------------------------------------------------------------------------

pub struct StubKnowledgeItemRepository;

#[async_trait]
impl KnowledgeItemRepository for StubKnowledgeItemRepository {
    async fn insert(
        &self,
        _conn: &mut PgConnection,
        _new: &NewKnowledgeItem,
    ) -> Result<KnowledgeItem, DbError> {
        unimplemented!()
    }

    async fn find_by_id(
        &self,
        _conn: &mut PgConnection,
        _id: KnowledgeItemId,
    ) -> Result<KnowledgeItem, DbError> {
        unimplemented!()
    }

    async fn find_by_ids(
        &self,
        _conn: &mut PgConnection,
        _ids: &[KnowledgeItemId],
    ) -> Result<Vec<KnowledgeItem>, DbError> {
        unimplemented!()
    }

    async fn semantic_search(
        &self,
        _conn: &mut PgConnection,
        _params: &SemanticSearchParams,
    ) -> Result<SemanticSearchResponse, DbError> {
        unimplemented!()
    }
}

// ---------------------------------------------------------------------------
// StubProjectRepository
// ---------------------------------------------------------------------------

pub struct StubProjectRepository;

#[async_trait]
impl ProjectRepository for StubProjectRepository {
    async fn insert(
        &self,
        _conn: &mut PgConnection,
        _new_project: &NewProject,
    ) -> Result<Project, DbError> {
        unimplemented!()
    }

    async fn find_by_id(
        &self,
        _conn: &mut PgConnection,
        _id: ProjectId,
    ) -> Result<Project, DbError> {
        unimplemented!()
    }

    async fn find_by_git_remote(
        &self,
        _conn: &mut PgConnection,
        _git_remote: &str,
    ) -> Result<Option<Project>, DbError> {
        unimplemented!()
    }

    async fn list(&self, _conn: &mut PgConnection) -> Result<Vec<Project>, DbError> {
        unimplemented!()
    }
}

// ---------------------------------------------------------------------------
// StubJobRepository
// ---------------------------------------------------------------------------

pub struct StubJobRepository;

#[async_trait]
impl JobRepository for StubJobRepository {
    async fn insert(&self, _conn: &mut PgConnection, _new_job: &NewJob) -> Result<Job, DbError> {
        unimplemented!()
    }

    async fn find_by_id(&self, _conn: &mut PgConnection, _id: JobId) -> Result<Job, DbError> {
        unimplemented!()
    }

    async fn find_by_project_id(
        &self,
        _conn: &mut PgConnection,
        _project_id: ProjectId,
    ) -> Result<Vec<Job>, DbError> {
        unimplemented!()
    }

    async fn update_status(
        &self,
        _conn: &mut PgConnection,
        _id: JobId,
        _transition: &JobStatusTransition,
    ) -> Result<Job, DbError> {
        unimplemented!()
    }

    async fn update_batch_size(
        &self,
        _conn: &mut PgConnection,
        _id: JobId,
        _batch_size: u32,
        _extraction_original_count: u32,
    ) -> Result<Job, DbError> {
        unimplemented!()
    }

    async fn set_committed_batch_id(
        &self,
        _conn: &mut PgConnection,
        _id: JobId,
        _batch_id: RelationBatchId,
    ) -> Result<Option<Job>, DbError> {
        unimplemented!()
    }

    async fn fail_stale_dead_lettered_jobs(
        &self,
        _conn: &mut PgConnection,
    ) -> Result<Vec<JobId>, DbError> {
        unimplemented!()
    }

    async fn find_stuck_triaging_jobs(
        &self,
        _conn: &mut PgConnection,
    ) -> Result<Vec<JobId>, DbError> {
        unimplemented!()
    }
}

// ---------------------------------------------------------------------------
// StubRetrievalFeedbackRepository
// ---------------------------------------------------------------------------

pub struct StubRetrievalFeedbackRepository;

#[async_trait]
impl RetrievalFeedbackRepository for StubRetrievalFeedbackRepository {
    async fn insert(
        &self,
        _conn: &mut PgConnection,
        _new: &NewRetrievalFeedback,
    ) -> Result<RetrievalFeedback, DbError> {
        unimplemented!()
    }

    async fn find_by_id(
        &self,
        _conn: &mut PgConnection,
        _id: RetrievalFeedbackId,
    ) -> Result<RetrievalFeedback, DbError> {
        unimplemented!()
    }
}
