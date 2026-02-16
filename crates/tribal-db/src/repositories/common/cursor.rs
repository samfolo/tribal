//! Cursor encoding and decoding for paginated queries.
//!
//! Cursors are opaque hex-encoded strings that encode a position within
//! an ordered result set.  The binary layout is:
//!
//! ```text
//! [ similarity: f64 (8 bytes, big-endian) ][ id: UUID (16 bytes) ]
//! ```
//!
//! yielding 24 bytes → 48 hex characters.  Similarity is stored as f64
//! to match Postgres's double-precision computation exactly, avoiding
//! f32/f64 round-trip mismatches in cursor-based pagination.

use crate::DbError;

/// Number of bytes in the binary cursor representation.
const CURSOR_BYTES: usize = 24;

/// Size of the f64 similarity prefix in bytes.
const SIMILARITY_BYTES: usize = 8;

/// Encodes a (similarity, id) pair into a hex-encoded cursor string.
pub(crate) fn encode_cursor(similarity: f64, id: uuid::Uuid) -> String {
    let mut buf = [0u8; CURSOR_BYTES];
    buf[..SIMILARITY_BYTES].copy_from_slice(&similarity.to_be_bytes());
    buf[SIMILARITY_BYTES..].copy_from_slice(id.as_bytes());

    let mut hex = String::with_capacity(CURSOR_BYTES * 2);
    for byte in &buf {
        use std::fmt::Write;
        write!(hex, "{byte:02x}").expect("writing to String is infallible");
    }
    hex
}

/// Decodes a hex-encoded cursor string into a (similarity, id) pair.
///
/// # Errors
///
/// Returns [`DbError::InvalidCursor`] if the cursor has the wrong
/// length or contains invalid hex characters.
pub(crate) fn decode_cursor(cursor: &str) -> Result<(f64, uuid::Uuid), DbError> {
    let expected_hex_len = CURSOR_BYTES * 2;
    if cursor.len() != expected_hex_len {
        return Err(DbError::InvalidCursor {
            detail: format!(
                "expected {expected_hex_len} hex characters, got {}",
                cursor.len(),
            ),
        });
    }

    let mut bytes = [0u8; CURSOR_BYTES];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&cursor[i * 2..i * 2 + 2], 16).map_err(|e| {
            DbError::InvalidCursor {
                detail: format!("invalid hex at position {}: {e}", i * 2),
            }
        })?;
    }

    let similarity = f64::from_be_bytes(
        bytes[..SIMILARITY_BYTES]
            .try_into()
            .expect("slice length verified above"),
    );
    let id = uuid::Uuid::from_bytes(
        bytes[SIMILARITY_BYTES..]
            .try_into()
            .expect("slice length verified above"),
    );
    Ok((similarity, id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cursor_roundtrip() {
        let similarity = 0.875_f64;
        let id = uuid::Uuid::new_v4();
        let encoded = encode_cursor(similarity, id);
        let (decoded_sim, decoded_id) = decode_cursor(&encoded).expect("decode");
        assert_eq!(decoded_sim.to_bits(), similarity.to_bits());
        assert_eq!(decoded_id, id);
    }

    #[test]
    fn test_cursor_hex_length() {
        let encoded = encode_cursor(0.5, uuid::Uuid::nil());
        assert_eq!(encoded.len(), 48);
    }

    #[test]
    fn test_decode_cursor_wrong_length() {
        let result = decode_cursor("abcd");
        assert!(matches!(result, Err(DbError::InvalidCursor { .. })));
    }

    #[test]
    fn test_decode_cursor_invalid_hex() {
        let result = decode_cursor(&"zz".repeat(24));
        assert!(matches!(result, Err(DbError::InvalidCursor { .. })));
    }

    #[test]
    fn test_cursor_zero_similarity() {
        let id = uuid::Uuid::new_v4();
        let encoded = encode_cursor(0.0, id);
        let (sim, decoded_id) = decode_cursor(&encoded).expect("decode");
        assert_eq!(sim.to_bits(), 0.0_f64.to_bits());
        assert_eq!(decoded_id, id);
    }

    #[test]
    fn test_cursor_nil_uuid() {
        let encoded = encode_cursor(1.0, uuid::Uuid::nil());
        let (sim, id) = decode_cursor(&encoded).expect("decode");
        assert_eq!(sim.to_bits(), 1.0_f64.to_bits());
        assert_eq!(id, uuid::Uuid::nil());
    }
}
