//! Feedback rating classification for retrieval sessions.

use serde::{Deserialize, Serialize};

/// The rating on a retrieval feedback record.
///
/// Covers the entire retrieval session (discovery, exploration, etc.),
/// not a single tool call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackRating {
    /// The retrieval experience was useful.
    Positive,
    /// The retrieval experience was not useful.
    Negative,
}

enum_text_conversions!(FeedbackRating {
    FeedbackRating::Positive => "positive",
    FeedbackRating::Negative => "negative",
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_feedback_rating_serde_roundtrip, FeedbackRating {
        FeedbackRating::Positive => "positive",
        FeedbackRating::Negative => "negative",
    });

    enum_text_tests!(test_feedback_rating_text_roundtrip, FeedbackRating {
        FeedbackRating::Positive => "positive",
        FeedbackRating::Negative => "negative",
    });
}
