//! The metering shape the gateway writes for a settled call.
//!
//! Mirrors the platform's normalised usage event and its per-dimension
//! quantities: one event carries the rate-card key axes and its token counts,
//! so a settle resolves to exactly one rate and cross-dimension disagreement is
//! unrepresentable. The attribution fields (`principal`, `account`) are what the
//! gateway *writes*, derived from the run's credential — a wire request never
//! asserts its own tenancy, which would make the spend cap opt-in.

use serde::{Deserialize, Serialize};

use crate::gateway::{
    reference::{AccountReference, ModelId, PositionKey, PrincipalReference},
    vocabulary::{CacheTier, Mode, Operation, Quality, UsageDimension},
};

// ---------------------------------------------------------------------------
// Metering DTO
// ---------------------------------------------------------------------------

/// A settled call's metered usage: the rate-card key, its attribution, and the
/// token counts it incurred.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GatewayUsage {
    /// The contributor this usage is attributed to; absent for unattributed
    /// automation the platform allows.
    pub principal: Option<PrincipalReference>,
    /// The account billed for the call.
    pub account: AccountReference,
    /// The model served.
    pub model: ModelId,
    /// The operation axis of the rate-card key.
    pub operation: Operation,
    /// The quality axis of the rate-card key.
    pub quality: Quality,
    /// The mode axis of the rate-card key.
    pub mode: Mode,
    /// The call this usage settles, scoped to its run.
    pub position_key: PositionKey,
    /// The token counts, one entry per `(dimension, tier)` measured.
    pub quantities: Vec<UsageQuantity>,
}

/// One metered quantity on one `(dimension, tier)` axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct UsageQuantity {
    /// The token axis measured.
    pub dimension: UsageDimension,
    /// The tier the quantity falls in.
    pub tier: CacheTier,
    /// How many units on this axis.
    pub quantity: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_round_trips_with_its_quantities() {
        let usage = GatewayUsage {
            principal: Some(PrincipalReference::new("user_01")),
            account: AccountReference::new("account_01"),
            model: ModelId::new("claude-sonnet-5"),
            operation: Operation::Completion,
            quality: Quality::Standard,
            mode: Mode::Interactive,
            position_key: PositionKey::new("thread_01:42"),
            quantities: vec![
                UsageQuantity {
                    dimension: UsageDimension::InputToken,
                    tier: CacheTier::Untiered,
                    quantity: 1_200,
                },
                UsageQuantity {
                    dimension: UsageDimension::CacheWriteToken,
                    tier: CacheTier::FiveMinute,
                    quantity: 800,
                },
            ],
        };

        let json = serde_json::to_string(&usage).expect("serialises");
        let parsed: GatewayUsage = serde_json::from_str(&json).expect("round-trips");
        assert_eq!(parsed, usage);
    }

    #[test]
    fn unattributed_usage_carries_no_principal() {
        let usage = GatewayUsage {
            principal: None,
            account: AccountReference::new("account_01"),
            model: ModelId::new("text-embedding-3"),
            operation: Operation::Embedding,
            quality: Quality::Standard,
            mode: Mode::Scheduled,
            position_key: PositionKey::new("reindex_7:3"),
            quantities: vec![UsageQuantity {
                dimension: UsageDimension::InputToken,
                tier: CacheTier::Untiered,
                quantity: 4_096,
            }],
        };

        let parsed: GatewayUsage =
            serde_json::from_str(&serde_json::to_string(&usage).unwrap()).unwrap();
        assert_eq!(parsed.principal, None);
    }
}
