//! Tag resolution: matching suggested tags against the tag registry.
//!
//! A standalone pure function for unit testability. Takes candidate tags
//! and the tag registry, returns resolved tags and new tags for creation.
//! Fuzzy matching uses Levenshtein distance for short tags (≤ 8 chars).

use tribal_domain::TagRegistryEntry;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Resolves suggested tags against the existing tag registry.
///
/// Returns `(resolved_tags, new_tags_to_create)` as disjoint sets:
///
/// - `resolved_tags`: tags that matched an existing registry entry
///   (exact match or fuzzy match for short tags). Uses the registry's
///   canonical form.
/// - `new_tags_to_create`: normalised tags that did not match any
///   registry entry and should be registered via `batch_upsert`.
///
/// Both lists are deduplicated.
#[allow(dead_code)]
pub(crate) fn resolve_tags(
    suggested_tags: &[String],
    registry: &[TagRegistryEntry],
) -> (Vec<String>, Vec<String>) {
    let registry_tags: Vec<&str> = registry.iter().map(TagRegistryEntry::tag).collect();

    let mut resolved = Vec::new();
    let mut new_tags = Vec::new();

    for raw in suggested_tags {
        let normalised = raw.trim().to_lowercase();
        if normalised.is_empty() {
            continue;
        }

        if let Some(canonical) = exact_match(&normalised, &registry_tags) {
            if !resolved.contains(&canonical) {
                resolved.push(canonical);
            }
        } else if normalised.len() <= 8 {
            if let Some(canonical) = fuzzy_match(&normalised, &registry_tags) {
                if !resolved.contains(&canonical) {
                    resolved.push(canonical);
                }
            } else if !new_tags.contains(&normalised) {
                new_tags.push(normalised);
            }
        } else if !new_tags.contains(&normalised) {
            new_tags.push(normalised);
        }
    }

    (resolved, new_tags)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the canonical registry form if an exact match exists.
fn exact_match(normalised: &str, registry_tags: &[&str]) -> Option<String> {
    registry_tags
        .iter()
        .find(|&&tag| tag == normalised)
        .map(|&tag| tag.to_owned())
}

/// Returns the canonical registry form if a fuzzy match exists
/// (Levenshtein distance ≤ 2). Only called for tags ≤ 8 characters.
fn fuzzy_match(normalised: &str, registry_tags: &[&str]) -> Option<String> {
    registry_tags
        .iter()
        .filter_map(|&tag| {
            let distance = strsim::levenshtein(normalised, tag);
            (distance <= 2).then_some((distance, tag))
        })
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, tag)| tag.to_owned())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_test_utils::a_tag_registry_entry;

    use super::*;

    fn registry(tags: &[&str]) -> Vec<TagRegistryEntry> {
        tags.iter()
            .map(|t| a_tag_registry_entry().tag((*t).to_owned()).build())
            .collect()
    }

    #[test]
    fn test_normalises_to_lowercase() {
        let reg = registry(&["rust"]);
        let (resolved, new_tags) = resolve_tags(&["RUST".to_owned()], &reg);
        assert_eq!(resolved, vec!["rust"]);
        assert!(new_tags.is_empty());
    }

    #[test]
    fn test_exact_match_uses_registry_form() {
        let reg = registry(&["rust"]);
        let (resolved, new_tags) = resolve_tags(&["rust".to_owned()], &reg);
        assert_eq!(resolved, vec!["rust"]);
        assert!(new_tags.is_empty());
    }

    #[test]
    fn test_fuzzy_match_short_tag_within_distance() {
        let reg = registry(&["rust"]);
        // "ruts" → Levenshtein distance 2 from "rust", tag is 4 chars (≤ 8)
        let (resolved, new_tags) = resolve_tags(&["ruts".to_owned()], &reg);
        assert_eq!(resolved, vec!["rust"]);
        assert!(new_tags.is_empty());
    }

    #[test]
    fn test_fuzzy_match_rejected_beyond_distance() {
        let reg = registry(&["rust"]);
        // "java" → Levenshtein distance 4 from "rust"
        let (resolved, new_tags) = resolve_tags(&["java".to_owned()], &reg);
        assert!(resolved.is_empty());
        assert_eq!(new_tags, vec!["java"]);
    }

    #[test]
    fn test_skips_fuzzy_for_long_tags() {
        let reg = registry(&["debugging"]);
        // "debuggign" is 9 chars (> 8), Levenshtein 2 from "debugging"
        // but fuzzy matching is skipped for long tags
        let (resolved, new_tags) = resolve_tags(&["debuggign".to_owned()], &reg);
        assert!(resolved.is_empty());
        assert_eq!(new_tags, vec!["debuggign"]);
    }

    #[test]
    fn test_unmatched_tags_returned_for_creation() {
        let reg = registry(&["rust"]);
        let (resolved, new_tags) =
            resolve_tags(&["rust".to_owned(), "newconcept".to_owned()], &reg);
        assert_eq!(resolved, vec!["rust"]);
        assert_eq!(new_tags, vec!["newconcept"]);
    }

    #[test]
    fn test_deduplicates_resolved_tags() {
        let reg = registry(&["rust"]);
        let (resolved, new_tags) = resolve_tags(
            &["rust".to_owned(), "Rust".to_owned(), "RUST".to_owned()],
            &reg,
        );
        assert_eq!(resolved, vec!["rust"]);
        assert!(new_tags.is_empty());
    }
}
