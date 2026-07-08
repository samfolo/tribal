//! `models.catalogue`: static model descriptors the config facade can offer.

use serde::{Deserialize, Serialize};
use tribal_domain::ProviderKind;

/// One known model option.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct KnownModelEntry {
    /// Provider this row belongs to.
    pub provider: ProviderKind,
    /// Provider-native model identifier.
    pub model: String,
    /// Operator-facing display name.
    pub display_name: String,
}

/// Static model catalogue served by `models.catalogue`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ModelsCatalogue {
    /// Known model rows.
    pub models: Vec<KnownModelEntry>,
}
