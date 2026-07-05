//! Opaque identifiers the gateway contract carries across the wire.
//!
//! Each is a distinct newtype over a string so an account cannot be passed
//! where a principal is expected. They are opaque tokens — the platform assigns
//! `account`/`principal` and the runtime derives a `position_key` from a run's
//! committed tail — so the seam wraps them for type-safety without asserting
//! structure it cannot enforce across a database boundary.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Reference newtypes
// ---------------------------------------------------------------------------

/// The identifier of a priced model. Open by design: a priced model is a
/// rate-card row, not a closed enum value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(
    feature = "schema",
    derive(schemars::JsonSchema),
    schemars(transparent)
)]
#[serde(transparent)]
pub struct ModelId(String);

/// The opaque platform account a metered call is billed to — the platform
/// `account_…` id, a cross-database pointer with no enforceable foreign key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(
    feature = "schema",
    derive(schemars::JsonSchema),
    schemars(transparent)
)]
#[serde(transparent)]
pub struct AccountReference(String);

/// The opaque platform user a contribution is attributed to — the linkage key
/// bridged to a local principal by `PrincipalMap`, never the `IdP` subject.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(
    feature = "schema",
    derive(schemars::JsonSchema),
    schemars(transparent)
)]
#[serde(transparent)]
pub struct PrincipalReference(String);

/// The identity of one model call within a run — derived from the run's
/// committed tail, so a resumed run re-presents the same key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(
    feature = "schema",
    derive(schemars::JsonSchema),
    schemars(transparent)
)]
#[serde(transparent)]
pub struct PositionKey(String);

macro_rules! string_reference {
    ($type:ty) => {
        impl $type {
            /// Wraps an opaque token.
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the token as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_reference!(ModelId);
string_reference!(AccountReference);
string_reference!(PrincipalReference);
string_reference!(PositionKey);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reference_round_trips_transparently() {
        let model = ModelId::new("claude-sonnet-5");
        let json = serde_json::to_string(&model).expect("serialises");
        assert_eq!(
            json, "\"claude-sonnet-5\"",
            "wraps its token, adds no envelope"
        );
        let parsed: ModelId = serde_json::from_str(&json).expect("round-trips");
        assert_eq!(parsed, model);
    }

    #[test]
    fn as_str_returns_the_wrapped_token() {
        assert_eq!(AccountReference::new("account_01").as_str(), "account_01");
    }
}
