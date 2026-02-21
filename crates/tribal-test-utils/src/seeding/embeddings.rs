//! Deterministic embedding generation for seed scenarios.
//!
//! Items within the same embedding group receive vectors with high
//! cosine similarity. Items in different groups are assigned to
//! distinct dominant dimensions via hashing, producing dissimilar
//! vectors.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

// ---------------------------------------------------------------------------
// Group assigner
// ---------------------------------------------------------------------------

/// Assigns embedding group labels to dominant vector dimensions.
///
/// Each unique group label is mapped to a dimension index via hashing.
/// Within a group, successive items receive incrementing position values
/// used to add small perturbations for deterministic search ordering.
pub(crate) struct EmbeddingGroupAssigner {
    /// Label → (dominant dimension index, next position within group).
    groups: HashMap<String, (usize, usize)>,
    dimensions: usize,
}

impl EmbeddingGroupAssigner {
    pub(crate) fn new(dimensions: usize) -> Self {
        Self {
            groups: HashMap::new(),
            dimensions,
        }
    }

    /// Returns `(dimension_index, position_in_group)` for the given
    /// group label.
    pub(crate) fn assign(&mut self, group: &str) -> (usize, usize) {
        let dims = self.dimensions;
        let entry = self
            .groups
            .entry(group.to_owned())
            .or_insert_with(|| {
                let mut hasher = DefaultHasher::new();
                group.hash(&mut hasher);
                #[allow(clippy::cast_possible_truncation)]
                let dim_index = (hasher.finish() as usize) % dims;
                (dim_index, 0)
            });
        let position = entry.1;
        entry.1 += 1;
        (entry.0, position)
    }
}

// ---------------------------------------------------------------------------
// Vector generation
// ---------------------------------------------------------------------------

/// Generates a deterministic embedding vector for an item.
///
/// Position 0 produces the canonical basis vector for the group's
/// dominant dimension. Subsequent positions add small perturbations
/// in adjacent dimensions, reducing similarity to the canonical vector
/// and producing a deterministic ordering within the group.
pub(crate) fn make_group_embedding(
    group_index: usize,
    position_in_group: usize,
    dimensions: usize,
) -> Vec<f32> {
    let mut v = vec![0.0f32; dimensions];
    let dominant = group_index % dimensions;
    v[dominant] = 1.0;
    if position_in_group > 0 {
        let noise_dim = (dominant + position_in_group) % dimensions;
        if noise_dim != dominant {
            #[allow(clippy::cast_precision_loss)]
            let perturbation = 0.1 / (position_in_group as f32);
            v[noise_dim] = perturbation;
        }
    }
    v
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_group_embedding_canonical_vector() {
        let v = make_group_embedding(5, 0, 768);
        assert_eq!(v.len(), 768);
        assert!((v[5] - 1.0).abs() < f32::EPSILON);
        for (i, &val) in v.iter().enumerate() {
            if i != 5 {
                assert!((val - 0.0).abs() < f32::EPSILON, "dim {i} should be 0.0");
            }
        }
    }

    #[test]
    fn test_make_group_embedding_perturbation() {
        let v = make_group_embedding(5, 1, 768);
        assert!((v[5] - 1.0).abs() < f32::EPSILON);
        assert!((v[6] - 0.1).abs() < f32::EPSILON);
    }

    #[test]
    fn test_make_group_embedding_increasing_perturbation_reduces_similarity() {
        let v0 = make_group_embedding(5, 0, 768);
        let v1 = make_group_embedding(5, 1, 768);
        let v2 = make_group_embedding(5, 2, 768);

        let cos_01 = cosine_similarity(&v0, &v1);
        let cos_02 = cosine_similarity(&v0, &v2);

        assert!(
            cos_01 > cos_02,
            "position 1 ({cos_01}) should be more similar to canonical \
             than position 2 ({cos_02})"
        );
    }

    #[test]
    fn test_embedding_group_assigner_same_group_same_dim() {
        let mut assigner = EmbeddingGroupAssigner::new(768);
        let (dim_a, pos_a) = assigner.assign("group-a");
        let (dim_b, pos_b) = assigner.assign("group-a");
        assert_eq!(dim_a, dim_b);
        assert_eq!(pos_a, 0);
        assert_eq!(pos_b, 1);
    }

    #[test]
    fn test_embedding_group_assigner_different_groups() {
        let mut assigner = EmbeddingGroupAssigner::new(768);
        let (dim_a, _) = assigner.assign("caching");
        let (dim_b, _) = assigner.assign("design");
        assert_ne!(
            dim_a, dim_b,
            "different groups should map to different dimensions"
        );
    }

    #[test]
    fn test_embedding_group_assigner_auto_position_increment() {
        let mut assigner = EmbeddingGroupAssigner::new(768);
        let (_, p0) = assigner.assign("x");
        let (_, p1) = assigner.assign("x");
        let (_, p2) = assigner.assign("x");
        assert_eq!(p0, 0);
        assert_eq!(p1, 1);
        assert_eq!(p2, 2);
    }

    fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        dot / (norm_a * norm_b)
    }
}
