//! Response validation shared across embedding provider implementations.

use crate::InferenceError;

// ---------------------------------------------------------------------------
// Embedding validation
// ---------------------------------------------------------------------------

/// Validates an embeddings array and extracts the single vector.
///
/// Embedding providers return arrays of vectors, but Tribal always sends
/// a single input per request.  This function enforces: non-empty,
/// exactly one element, and correct dimensionality.
pub(crate) fn validate_embeddings(
    embeddings: Vec<Vec<f32>>,
    expected_dimensions: u32,
    model: &str,
) -> Result<Vec<f32>, InferenceError> {
    if embeddings.is_empty() {
        return Err(InferenceError::EmbeddingFailed {
            model: model.to_owned(),
            context: "embeddings array is empty".to_owned(),
            source: None,
        });
    }
    if embeddings.len() > 1 {
        return Err(InferenceError::ResponseParseFailed {
            expected_shape: "embeddings array length == 1".to_owned(),
            actual: format!("embeddings array length == {}", embeddings.len()),
        });
    }

    let vector = embeddings.into_iter().next().expect("checked non-empty");
    let expected = expected_dimensions as usize;

    if vector.len() != expected {
        tracing::error!(
            expected,
            actual = vector.len(),
            "dimension mismatch — configuration error",
        );
        return Err(InferenceError::ResponseParseFailed {
            expected_shape: format!("embedding vector length == {expected}"),
            actual: format!("embedding vector length == {}", vector.len()),
        });
    }

    Ok(vector)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_embeddings_single_correct() {
        let embeddings = vec![vec![0.1, 0.2, 0.3]];
        let result = validate_embeddings(embeddings, 3, "model").unwrap();
        assert_eq!(result, vec![0.1, 0.2, 0.3]);
    }

    #[test]
    fn test_validate_embeddings_empty_returns_embedding_failed() {
        let err = validate_embeddings(vec![], 3, "test-model").unwrap_err();
        assert!(matches!(
            err,
            InferenceError::EmbeddingFailed {
                ref model,
                ref context,
                ..
            }
            if model == "test-model" && context == "embeddings array is empty"
        ));
    }

    #[test]
    fn test_validate_embeddings_multiple_returns_response_parse_failed() {
        let embeddings = vec![vec![0.1, 0.2], vec![0.3, 0.4]];
        let err = validate_embeddings(embeddings, 2, "model").unwrap_err();
        assert!(matches!(
            err,
            InferenceError::ResponseParseFailed {
                ref expected_shape,
                ref actual,
            }
            if expected_shape == "embeddings array length == 1"
                && actual == "embeddings array length == 2"
        ));
    }

    #[test]
    fn test_validate_embeddings_dimension_mismatch_returns_response_parse_failed() {
        let embeddings = vec![vec![0.1, 0.2, 0.3, 0.4, 0.5]];
        let err = validate_embeddings(embeddings, 3, "model").unwrap_err();
        assert!(matches!(
            err,
            InferenceError::ResponseParseFailed {
                ref expected_shape,
                ref actual,
            }
            if expected_shape == "embedding vector length == 3"
                && actual == "embedding vector length == 5"
        ));
    }
}
