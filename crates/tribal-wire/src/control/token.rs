//! The `token.list` crossing: the metadata of issued tokens.
//!
//! Only a token's hash is stored and the raw value is discarded at issuance, so
//! neither a prefix nor the value is recoverable — the bridge reports principal,
//! scopes, and expiry, and offers no minting or revocation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// token.list
// ---------------------------------------------------------------------------

/// The non-secret metadata of one issued token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TokenInfo {
    /// The principal the token authenticates as.
    pub principal: String,
    /// The scopes the token grants.
    pub scopes: Vec<String>,
    /// When the token was issued.
    pub created_at: DateTime<Utc>,
    /// When the token expires, absent for a non-expiring token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
}

/// Every issued token's metadata, the result of `token.list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TokenList {
    /// The issued tokens, never their secret values.
    pub tokens: Vec<TokenInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_token_list_round_trips() {
        let list = TokenList {
            tokens: vec![TokenInfo {
                principal: "principal:local".to_owned(),
                scopes: vec!["mcp".to_owned(), "control".to_owned()],
                created_at: DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp"),
                expires_at: None,
            }],
        };
        let parsed: TokenList =
            serde_json::from_str(&serde_json::to_string(&list).unwrap()).unwrap();
        assert_eq!(parsed, list);
    }

    #[test]
    fn test_token_info_carries_no_secret_field() {
        let info = TokenInfo {
            principal: "principal:local".to_owned(),
            scopes: vec![],
            created_at: DateTime::from_timestamp(0, 0).expect("epoch"),
            expires_at: None,
        };
        let json = serde_json::to_value(&info).unwrap();
        for forbidden in ["value", "token", "hash", "prefix"] {
            assert!(
                json.get(forbidden).is_none(),
                "token metadata must not carry a {forbidden} field",
            );
        }
    }
}
