//! Mock implementation of [`PromptVersionRepository`].

use tribal_db::{NewPromptVersion, PromptVersionRepository};
use tribal_domain::{PromptVersion, PromptVersionId};

use super::mock_repository;

mock_repository! {
    MockPromptVersionRepository for PromptVersionRepository, tribal_db::DbError {
        upsert(NewPromptVersion => PromptVersion)
            (new: &NewPromptVersion) { new.clone() };
        find_by_id(PromptVersionId => PromptVersion)
            (id: PromptVersionId) { id };
        find_by_ids(Vec<PromptVersionId> => Vec<PromptVersion>)
            (ids: &[PromptVersionId]) { ids.to_vec() };
        find_by_slot_and_hash(String => Option<PromptVersion>)
            (stage: tribal_domain::PromptStage, class: tribal_domain::PromptClass, role: tribal_domain::PromptRole, content_hash: &str) { format!("{stage}:{class}:{role}:{content_hash}") }
    }
}
