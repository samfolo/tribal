//! Method- and revision-bound inventory cursor encoding.

use base64::Engine as _;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tribal_wire::management::{ConfigRevision, PageCursor};

/// Space retained for the response envelope and newline framing.
pub(super) const RESPONSE_ENVELOPE_RESERVE_BYTES: usize = 4 * 1024;
pub(super) const INVENTORY_RESULT_BUDGET: usize =
    crate::management::socket::MAX_FRAME_BYTES - RESPONSE_ENVELOPE_RESERVE_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum InventoryMethod {
    ProjectList,
    TokenList,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct InventoryPosition {
    pub(super) created_at: DateTime<Utc>,
    pub(super) id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct InventoryCursor {
    method: InventoryMethod,
    revision: ConfigRevision,
    pub(super) high_water: InventoryPosition,
    pub(super) after: InventoryPosition,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum InventoryCursorError {
    #[error("inventory cursor is malformed")]
    Malformed,
    #[error("inventory cursor belongs to another method")]
    WrongMethod,
    #[error("inventory cursor belongs to another configuration revision")]
    Stale {
        expected: ConfigRevision,
        actual: ConfigRevision,
    },
}

impl InventoryCursor {
    pub(super) fn new(
        method: InventoryMethod,
        revision: ConfigRevision,
        high_water: InventoryPosition,
        after: InventoryPosition,
    ) -> Self {
        Self {
            method,
            revision,
            high_water,
            after,
        }
    }

    pub(super) fn encode(&self) -> PageCursor {
        let bytes = serde_json::to_vec(self).expect("inventory cursor is serialisable");
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        PageCursor::try_from(encoded).expect("encoded cursor is non-empty")
    }

    pub(super) fn decode(
        cursor: &PageCursor,
        method: InventoryMethod,
        revision: &ConfigRevision,
    ) -> Result<Self, InventoryCursorError> {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(cursor.as_str())
            .map_err(|_| InventoryCursorError::Malformed)?;
        let decoded: Self =
            serde_json::from_slice(&bytes).map_err(|_| InventoryCursorError::Malformed)?;
        if decoded.method != method {
            return Err(InventoryCursorError::WrongMethod);
        }
        if &decoded.revision != revision {
            return Err(InventoryCursorError::Stale {
                expected: decoded.revision,
                actual: revision.clone(),
            });
        }
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use tribal_wire::management::{ConfigDigest, ConfigRevision};

    use super::*;

    fn revision(bytes: &[u8]) -> ConfigRevision {
        ConfigRevision::from_digest(&ConfigDigest::from_bytes(bytes))
    }

    fn position(id: &str) -> InventoryPosition {
        InventoryPosition {
            created_at: Utc::now(),
            id: id.to_owned(),
        }
    }

    #[test]
    fn cursor_refuses_cross_method_and_stale_reuse() {
        let current = revision(b"current");
        let cursor = InventoryCursor::new(
            InventoryMethod::ProjectList,
            current.clone(),
            position("high"),
            position("after"),
        )
        .encode();

        assert!(matches!(
            InventoryCursor::decode(&cursor, InventoryMethod::TokenList, &current),
            Err(InventoryCursorError::WrongMethod)
        ));
        assert!(matches!(
            InventoryCursor::decode(&cursor, InventoryMethod::ProjectList, &revision(b"later")),
            Err(InventoryCursorError::Stale { .. })
        ));
    }
}
