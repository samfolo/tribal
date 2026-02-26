//! Shared utilities for pipeline stage implementations.

use std::sync::Arc;

use tokio::sync::Semaphore;
use tribal_db::{
    PgPromptVersionRepository, PgTagRegistryRepository, PromptVersionRepository,
    TagRegistryRepository,
};
use tribal_domain::{PromptVersion, PromptVersionId, TagRegistryEntry};

use crate::{error::StageError, worker::Worker};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const EXPECT_EXTRACTION_KEY: &str = "extraction key registered at startup";

pub(crate) const SEMAPHORE_CLOSED: &str = "semaphore closed unexpectedly";

// ---------------------------------------------------------------------------
// Shared loaders
// ---------------------------------------------------------------------------

impl Worker {
    /// Loads the full tag registry from the database.
    ///
    /// # Errors
    ///
    /// Returns [`StageError::Database`] on pool or query failure.
    pub(crate) async fn load_tag_registry(
        &self,
        stage: &str,
    ) -> Result<Vec<TagRegistryEntry>, StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "acquiring connection for tag registry".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        PgTagRegistryRepository
            .find_all(&mut conn)
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "loading tag registry".into(),
                source: e,
            })
    }

    /// Loads a prompt version by ID from the database.
    ///
    /// # Errors
    ///
    /// Returns [`StageError::Database`] on pool or query failure.
    pub(crate) async fn load_prompt_version(
        &self,
        stage: &str,
        id: PromptVersionId,
    ) -> Result<PromptVersion, StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "acquiring connection for prompt version".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        PgPromptVersionRepository
            .find_by_id(&mut conn, id)
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "loading prompt version".into(),
                source: e,
            })
    }

    /// Returns the extraction semaphore from the provider registry.
    ///
    /// # Panics
    ///
    /// Panics if the extraction key is not registered in the provider
    /// registry.
    pub(crate) fn extraction_semaphore(&self) -> &Arc<Semaphore> {
        self.provider_registry()
            .semaphore(self.extraction_key())
            .expect(EXPECT_EXTRACTION_KEY)
    }
}
