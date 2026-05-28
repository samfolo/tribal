//! Prompt legend constants and formatters.
//!
//! Single source of truth for similarity score band definitions and
//! relation suggestion descriptions used across prompt templates.
//! System prompt legends and user prompt inline labels both derive
//! from these definitions.

use std::fmt;

use tribal_domain::RelationSuggestion;

use crate::parsing::IngestionRelationKind;

// ---------------------------------------------------------------------------
// Similarity score bands
// ---------------------------------------------------------------------------

/// A named range of cosine similarity scores with a human-readable label.
///
/// Each variant encapsulates its own lower bound, upper bound, label,
/// and description. The [`Display`] implementation produces the
/// human-readable label used in prompt templates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SimilarityBand {
    Low,
    Moderate,
    High,
    VeryHigh,
}

impl SimilarityBand {
    /// All bands ordered from lowest to highest similarity.
    pub const ALL: &[Self] = &[Self::Low, Self::Moderate, Self::High, Self::VeryHigh];

    /// Inclusive lower bound of the score range.
    pub fn lower_bound(self) -> f64 {
        match self {
            Self::Low => 0.0,
            Self::Moderate => 0.3,
            Self::High => 0.6,
            Self::VeryHigh => 0.85,
        }
    }

    /// Upper bound of the score range.
    pub fn upper_bound(self) -> f64 {
        match self {
            Self::Low => Self::Moderate.lower_bound(),
            Self::Moderate => Self::High.lower_bound(),
            Self::High => Self::VeryHigh.lower_bound(),
            Self::VeryHigh => 1.0,
        }
    }

    /// Description of what scores in this range typically indicate.
    pub fn description(self) -> &'static str {
        match self {
            Self::Low => "Unlikely to be meaningfully related. Included for completeness.",
            Self::Moderate => "Topically related but almost certainly distinct claims.",
            Self::High => "Closely related. Examine the specific claims carefully.",
            Self::VeryHigh => {
                "Near-identical. Likely the same claim, but check for \
                 meaningful deltas."
            }
        }
    }
}

impl From<f64> for SimilarityBand {
    fn from(score: f64) -> Self {
        Self::ALL
            .iter()
            .rev()
            .copied()
            .find(|band| score >= band.lower_bound())
            .unwrap_or(Self::Low)
    }
}

impl From<f32> for SimilarityBand {
    fn from(score: f32) -> Self {
        Self::from(f64::from(score))
    }
}

impl fmt::Display for SimilarityBand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Low => write!(f, "low"),
            Self::Moderate => write!(f, "moderate"),
            Self::High => write!(f, "high"),
            Self::VeryHigh => write!(f, "very high"),
        }
    }
}

// ---------------------------------------------------------------------------
// Legend formatters
// ---------------------------------------------------------------------------

/// Renders the similarity score legend for injection into system prompts.
pub(crate) fn similarity_score_legend() -> String {
    let last = SimilarityBand::ALL.len() - 1;
    SimilarityBand::ALL
        .iter()
        .enumerate()
        .map(|(i, band)| {
            let upper = if i < last {
                format!("<{:.2}", band.upper_bound())
            } else {
                format!("{:.2}", band.upper_bound())
            };
            format!(
                "- **{:.2} \u{2013} {}** ({}): {}",
                band.lower_bound(),
                upper,
                band,
                band.description(),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Returns the prompt-facing description for a [`RelationSuggestion`] variant.
///
/// Exhaustive match ensures a compile error when a new variant is added
/// to the domain type.
pub(crate) fn relation_suggestion_description(suggestion: RelationSuggestion) -> &'static str {
    match suggestion {
        RelationSuggestion::Supports => {
            "The new claim reinforces or provides additional evidence \
             for the existing claim."
        }
        RelationSuggestion::Contradicts => {
            "The new claim conflicts with, corrects, or updates the \
             existing claim."
        }
        RelationSuggestion::Unrelated => {
            "Despite appearing in the search results, the new and \
             existing claims address different concerns."
        }
    }
}

/// Renders the relation suggestion legend for injection into system prompts.
pub(crate) fn relation_suggestion_legend() -> String {
    RelationSuggestion::ALL
        .iter()
        .map(|s| format!("- **{s}**: {}", relation_suggestion_description(*s)))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Returns the prompt-facing description for an [`IngestionRelationKind`] variant.
///
/// Exhaustive match ensures a compile error when a new variant is added.
pub(crate) fn ingestion_relation_kind_description(kind: IngestionRelationKind) -> &'static str {
    match kind {
        IngestionRelationKind::Supports => {
            "A provides evidence for, reinforces, or adds supporting context \
             to B. Both point in the same direction; one strengthens the claim \
             made by the other."
        }
        IngestionRelationKind::Contradicts => {
            "A conflicts with, corrects, or undermines B. They cannot both be \
             fully accurate simultaneously, or one represents a time-bound \
             update that invalidates the other."
        }
        IngestionRelationKind::DerivedFrom => {
            "A was logically derived from or builds directly upon B. This \
             tracks intellectual provenance: where a conclusion or procedure \
             came from."
        }
    }
}

/// Renders the ingestion relation kind legend for injection into system prompts.
pub(crate) fn ingestion_relation_kind_legend() -> String {
    IngestionRelationKind::ALL
        .iter()
        .map(|k| format!("- **{k}**: {}", ingestion_relation_kind_description(*k)))
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Band bounds are compile-time constants, so exact float comparison is safe.
    #[allow(clippy::float_cmp)]
    #[test]
    fn test_band_bounds_are_contiguous() {
        let bands = SimilarityBand::ALL;
        for window in bands.windows(2) {
            assert_eq!(
                window[0].upper_bound(),
                window[1].lower_bound(),
                "{} upper bound must equal {} lower bound",
                window[0],
                window[1],
            );
        }
    }

    // Band bounds are compile-time constants, so exact float comparison is safe.
    #[allow(clippy::float_cmp)]
    #[test]
    fn test_band_range_covers_unit_interval() {
        assert_eq!(SimilarityBand::ALL[0].lower_bound(), 0.0);
        assert_eq!(SimilarityBand::ALL.last().unwrap().upper_bound(), 1.0,);
    }

    #[test]
    fn test_from_score_low() {
        assert_eq!(SimilarityBand::from(0.0), SimilarityBand::Low);
        assert_eq!(SimilarityBand::from(0.15), SimilarityBand::Low);
        assert_eq!(SimilarityBand::from(0.29), SimilarityBand::Low);
    }

    #[test]
    fn test_from_score_moderate() {
        assert_eq!(SimilarityBand::from(0.3), SimilarityBand::Moderate);
        assert_eq!(SimilarityBand::from(0.45), SimilarityBand::Moderate);
        assert_eq!(SimilarityBand::from(0.59), SimilarityBand::Moderate);
    }

    #[test]
    fn test_from_score_high() {
        assert_eq!(SimilarityBand::from(0.6), SimilarityBand::High);
        assert_eq!(SimilarityBand::from(0.7), SimilarityBand::High);
        assert_eq!(SimilarityBand::from(0.84), SimilarityBand::High);
    }

    #[test]
    fn test_from_score_very_high() {
        assert_eq!(SimilarityBand::from(0.85), SimilarityBand::VeryHigh);
        assert_eq!(SimilarityBand::from(0.95), SimilarityBand::VeryHigh);
        assert_eq!(SimilarityBand::from(1.0), SimilarityBand::VeryHigh);
    }

    #[test]
    fn test_display_matches_label() {
        assert_eq!(SimilarityBand::Low.to_string(), "low");
        assert_eq!(SimilarityBand::Moderate.to_string(), "moderate");
        assert_eq!(SimilarityBand::High.to_string(), "high");
        assert_eq!(SimilarityBand::VeryHigh.to_string(), "very high");
    }

    #[test]
    fn test_similarity_score_legend_contains_all_bands() {
        let legend = similarity_score_legend();
        for band in SimilarityBand::ALL {
            assert!(
                legend.contains(&band.to_string()),
                "legend should contain label '{band}': {legend}",
            );
            assert!(
                legend.contains(band.description()),
                "legend should contain description for '{band}': {legend}",
            );
        }
    }

    #[test]
    fn test_relation_suggestion_legend_contains_all_variants() {
        let legend = relation_suggestion_legend();
        for suggestion in RelationSuggestion::ALL {
            let label = suggestion.to_string();
            assert!(
                legend.contains(&label),
                "legend should contain '{label}': {legend}",
            );
            assert!(
                legend.contains(relation_suggestion_description(*suggestion)),
                "legend should contain description for '{label}': {legend}",
            );
        }
    }

    #[test]
    fn test_ingestion_relation_kind_legend_contains_all_variants() {
        let legend = ingestion_relation_kind_legend();
        for kind in IngestionRelationKind::ALL {
            let label = kind.to_string();
            assert!(
                legend.contains(&label),
                "legend should contain '{label}': {legend}",
            );
            assert!(
                legend.contains(ingestion_relation_kind_description(*kind)),
                "legend should contain description for '{label}': {legend}",
            );
        }
    }
}
