//! Mock implementation of [`KnowledgeItemRepository`].

use tribal_db::{KnowledgeItemRepository, NewKnowledgeItem, SemanticSearchParams, SemanticSearchResponse};
use tribal_domain::{KnowledgeItem, KnowledgeItemId};

use super::mock_repository;

mock_repository! {
    MockKnowledgeItemRepository for KnowledgeItemRepository {
        insert(NewKnowledgeItem => KnowledgeItem)
            (new: &NewKnowledgeItem) { new.clone() };
        find_by_id(KnowledgeItemId => KnowledgeItem)
            (id: KnowledgeItemId) { id };
        find_by_ids(Vec<KnowledgeItemId> => Vec<KnowledgeItem>)
            (ids: &[KnowledgeItemId]) { ids.to_vec() };
        semantic_search(SemanticSearchParams => SemanticSearchResponse)
            (params: &SemanticSearchParams) { params.clone() }
    }
}
