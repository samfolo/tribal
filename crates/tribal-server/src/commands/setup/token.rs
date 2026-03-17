//! Bearer token generation.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngExt;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Number of cryptographically random bytes for token generation.
const TOKEN_BYTE_LENGTH: usize = 32;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Generates a cryptographically random bearer token.
///
/// Returns a base64url-encoded string (no padding) of [`TOKEN_BYTE_LENGTH`]
/// random bytes, producing exactly 43 characters.
pub(super) fn generate_raw_token() -> String {
    let mut bytes = [0u8; TOKEN_BYTE_LENGTH];
    rand::rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_raw_token_length() {
        let token = generate_raw_token();
        assert_eq!(
            token.len(),
            43,
            "32 bytes base64url-encoded (no padding) should be 43 chars"
        );
    }

    #[test]
    fn test_generate_raw_token_uniqueness() {
        let a = generate_raw_token();
        let b = generate_raw_token();
        assert_ne!(a, b, "two generated tokens should differ");
    }

    #[test]
    fn test_generate_raw_token_is_valid_base64url() {
        let token = generate_raw_token();
        let decoded = URL_SAFE_NO_PAD
            .decode(&token)
            .expect("token should be valid base64url");
        assert_eq!(decoded.len(), TOKEN_BYTE_LENGTH);
    }
}
