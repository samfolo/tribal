//! Mock implementation of [`RetrievalFeedbackRepository`].

use tribal_db::{NewRetrievalFeedback, RetrievalFeedbackRepository};
use tribal_domain::{RetrievalFeedback, RetrievalFeedbackId};

use super::mock_repository;

mock_repository! {
    MockRetrievalFeedbackRepository for RetrievalFeedbackRepository {
        insert(NewRetrievalFeedback => RetrievalFeedback)
            (new: &NewRetrievalFeedback) { new.clone() };
        find_by_id(RetrievalFeedbackId => RetrievalFeedback)
            (id: RetrievalFeedbackId) { id }
    }
}
