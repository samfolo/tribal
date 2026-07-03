//! The closed rate-card and metering vocabulary the gateway contract rates on.
//!
//! Each axis is a closed v1 value set. Growth is gated by the contract version
//! ([`GATEWAY_CONTRACT_VERSION`](super::GATEWAY_CONTRACT_VERSION)), never a silent
//! new value: an unknown value on any axis is rejected at deserialization rather
//! than accepted, so nothing unpriceable is ever admitted through the seam.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Rate-card key axes
// ---------------------------------------------------------------------------

/// The kind of provider call, one of the rate-card lookup axes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    /// A chat/text completion call.
    Completion,
    /// An embedding-generation call.
    Embedding,
}

/// The quality band a call was served at, one of the rate-card lookup axes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Quality {
    /// The default quality band.
    Standard,
}

/// Whether a call is interactive or scheduled, one of the rate-card lookup
/// axes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// An on-demand call a user is waiting on.
    Interactive,
    /// A call made by unattended, scheduled work.
    Scheduled,
}

// ---------------------------------------------------------------------------
// Metered quantity axes
// ---------------------------------------------------------------------------

/// A billable token axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum UsageDimension {
    /// Input tokens consumed, including any cached.
    InputToken,
    /// Output tokens generated.
    OutputToken,
    /// Input tokens served from the provider's prompt cache.
    CacheReadToken,
    /// Input tokens written into the provider's prompt cache.
    CacheWriteToken,
}

/// The tier a metered quantity falls in — the cache-write time-to-live bands,
/// the large-context band, and `none` for an untiered dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CacheTier {
    /// The five-minute cache-write band.
    FiveMinute,
    /// The one-hour cache-write band.
    OneHour,
    /// The band for input above the large-context threshold.
    #[serde(rename = "above_200k")]
    Above200k,
    /// No tier applies to this dimension.
    #[serde(rename = "none")]
    Untiered,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serialises to the exact wire token, and rejects any value outside the
    /// closed set — the forward-compatibility guarantee, asserted per axis.
    fn assert_closed<T>(cases: &[(T, &str)])
    where
        T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug + Copy,
    {
        for &(value, token) in cases {
            let json = serde_json::to_string(&value).expect("serialises");
            assert_eq!(json, format!("\"{token}\""), "wire token for {value:?}");
            let parsed: T = serde_json::from_str(&json).expect("round-trips");
            assert_eq!(parsed, value);
        }
        assert!(
            serde_json::from_str::<T>("\"frobnicate\"").is_err(),
            "an unknown value must be rejected, never silently accepted",
        );
    }

    #[test]
    fn operation_is_the_v1_closed_set() {
        assert_closed(&[
            (Operation::Completion, "completion"),
            (Operation::Embedding, "embedding"),
        ]);
    }

    #[test]
    fn quality_is_the_v1_closed_set() {
        assert_closed(&[(Quality::Standard, "standard")]);
    }

    #[test]
    fn mode_is_the_v1_closed_set() {
        assert_closed(&[
            (Mode::Interactive, "interactive"),
            (Mode::Scheduled, "scheduled"),
        ]);
    }

    #[test]
    fn usage_dimension_is_the_v1_closed_set() {
        assert_closed(&[
            (UsageDimension::InputToken, "input_token"),
            (UsageDimension::OutputToken, "output_token"),
            (UsageDimension::CacheReadToken, "cache_read_token"),
            (UsageDimension::CacheWriteToken, "cache_write_token"),
        ]);
    }

    #[test]
    fn cache_tier_is_the_v1_closed_set() {
        assert_closed(&[
            (CacheTier::FiveMinute, "five_minute"),
            (CacheTier::OneHour, "one_hour"),
            (CacheTier::Above200k, "above_200k"),
            (CacheTier::Untiered, "none"),
        ]);
    }
}
