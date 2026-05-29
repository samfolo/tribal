//! PKCE S256 verification per RFC 7636.
//!
//! Provides [`CodeVerifier`] and [`CodeChallenge`] newtypes validated
//! at construction time, plus an S256 verification operation that
//! uses constant-time byte comparison so the timing side-channel
//! warned against in RFC 7636 §8.5 is closed.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const VERIFIER_MIN_LEN: usize = 43;
const VERIFIER_MAX_LEN: usize = 128;
const CHALLENGE_LEN_S256: usize = 43;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Failure parsing a PKCE verifier or challenge.
///
/// Variants are fieldless so a partial credential is not echoed in
/// error messages.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum PkceParseError {
    /// The verifier is outside the 43..=128 length window or contains
    /// disallowed characters.
    #[error("invalid PKCE code verifier")]
    InvalidVerifier,
    /// The challenge is not 43 unreserved base64url characters.
    #[error("invalid PKCE code challenge")]
    InvalidChallenge,
}

// ---------------------------------------------------------------------------
// CodeVerifier
// ---------------------------------------------------------------------------

/// Validated PKCE code verifier (43-128 unreserved characters).
#[derive(Debug, Clone)]
pub struct CodeVerifier(String);

impl CodeVerifier {
    /// Parses and validates a raw verifier string.
    ///
    /// # Errors
    ///
    /// Returns [`PkceParseError::InvalidVerifier`] when the input is
    /// outside `43..=128` length or contains characters outside the
    /// unreserved set per RFC 7636 §4.1.
    pub fn parse(raw: &str) -> Result<Self, PkceParseError> {
        if !is_valid_verifier(raw) {
            return Err(PkceParseError::InvalidVerifier);
        }
        Ok(Self(raw.to_owned()))
    }

    /// Returns the raw verifier string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// ---------------------------------------------------------------------------
// CodeChallenge
// ---------------------------------------------------------------------------

/// Validated PKCE code challenge (43 unreserved base64url characters).
#[derive(Debug, Clone)]
pub struct CodeChallenge(String);

impl CodeChallenge {
    /// Parses and validates a raw challenge string.
    ///
    /// # Errors
    ///
    /// Returns [`PkceParseError::InvalidChallenge`] when the input is
    /// not exactly 43 unreserved base64url characters with no padding.
    pub fn parse(raw: &str) -> Result<Self, PkceParseError> {
        if !is_valid_challenge(raw) {
            return Err(PkceParseError::InvalidChallenge);
        }
        Ok(Self(raw.to_owned()))
    }

    /// Returns the raw challenge string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Derives the S256 challenge from a verifier.
    ///
    /// `BASE64URL-NO-PAD(SHA256(ASCII(verifier)))` per RFC 7636 §4.2.
    #[must_use]
    pub fn derive_s256(verifier: &CodeVerifier) -> Self {
        let digest = Sha256::digest(verifier.0.as_bytes());
        Self(URL_SAFE_NO_PAD.encode(digest))
    }

    /// Verifies a presented verifier against this challenge using S256.
    ///
    /// The byte comparison is constant-time per RFC 7636 §8.5.
    #[must_use]
    pub fn verify_s256(&self, verifier: &CodeVerifier) -> bool {
        let digest = Sha256::digest(verifier.0.as_bytes());
        // Encoding a 32-byte digest as base64url-no-pad always yields the
        // 43-byte string that parses as a valid CodeChallenge.
        let derived = URL_SAFE_NO_PAD.encode(digest);
        bool::from(derived.as_bytes().ct_eq(self.0.as_bytes()))
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

fn is_unreserved_pkce(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~')
}

fn is_valid_verifier(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    bytes.len() >= VERIFIER_MIN_LEN
        && bytes.len() <= VERIFIER_MAX_LEN
        && bytes.iter().copied().all(is_unreserved_pkce)
}

fn is_valid_challenge(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    bytes.len() == CHALLENGE_LEN_S256 && bytes.iter().copied().all(is_unreserved_pkce)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_verifier() -> CodeVerifier {
        // 43-byte random string of unreserved chars.
        CodeVerifier::parse("ZyXwVuTsRqPoNmLkJiHgFeDcBa0987654321ABCDEFG").unwrap()
    }

    #[test]
    fn test_parse_verifier_accepts_43_chars() {
        let v = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ"; // 43 chars
        assert!(CodeVerifier::parse(v).is_ok());
    }

    #[test]
    fn test_parse_verifier_accepts_128_chars_with_unreserved_punctuation() {
        let v = "a".repeat(125) + "-._";
        assert_eq!(v.len(), 128);
        assert!(CodeVerifier::parse(&v).is_ok());
    }

    #[test]
    fn test_parse_verifier_rejects_42_chars() {
        let v = "a".repeat(42);
        assert_eq!(
            CodeVerifier::parse(&v).unwrap_err(),
            PkceParseError::InvalidVerifier,
        );
    }

    #[test]
    fn test_parse_verifier_rejects_129_chars() {
        let v = "a".repeat(129);
        assert_eq!(
            CodeVerifier::parse(&v).unwrap_err(),
            PkceParseError::InvalidVerifier,
        );
    }

    #[test]
    fn test_parse_verifier_rejects_invalid_characters() {
        let v = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN!@#"; // 43 with bad chars
        assert!(matches!(
            CodeVerifier::parse(v).unwrap_err(),
            PkceParseError::InvalidVerifier,
        ));
    }

    #[test]
    fn test_parse_challenge_accepts_43_unreserved() {
        let c = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ";
        assert!(CodeChallenge::parse(c).is_ok());
    }

    #[test]
    fn test_parse_challenge_rejects_padding() {
        let c = "a".repeat(42) + "=";
        assert!(matches!(
            CodeChallenge::parse(&c).unwrap_err(),
            PkceParseError::InvalidChallenge,
        ));
    }

    #[test]
    fn test_derive_s256_then_verify_succeeds() {
        let v = sample_verifier();
        let c = CodeChallenge::derive_s256(&v);
        assert!(c.verify_s256(&v));
    }

    #[test]
    fn test_verify_s256_rejects_mismatching_verifier() {
        let v = sample_verifier();
        let c = CodeChallenge::derive_s256(&v);
        let other = CodeVerifier::parse("ZyXwVuTsRqPoNmLkJiHgFeDcBa0987654321ABCDEFH").unwrap();
        assert!(!c.verify_s256(&other));
    }

    #[test]
    fn test_derive_s256_is_43_chars() {
        let v = sample_verifier();
        let c = CodeChallenge::derive_s256(&v);
        assert_eq!(c.as_str().len(), 43);
    }

    #[test]
    fn test_known_vector_from_rfc_7636() {
        // RFC 7636 Appendix B test vector.
        let verifier = CodeVerifier::parse("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk").unwrap();
        let derived = CodeChallenge::derive_s256(&verifier);
        assert_eq!(
            derived.as_str(),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
        );
    }
}
