use serde::{Deserialize, Serialize};

/// The classification of a reference attached to a knowledge item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceKind {
    /// Project-relative file path (sigil `//`).
    FilePath,
    /// External URL.
    Url,
    /// Abstract reference too general for a path or URL.
    Concept,
    /// Code symbol (function, type, module).
    Symbol,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enum_serde_tests;

    enum_serde_tests!(test_reference_kind_serde_roundtrip, ReferenceKind {
        ReferenceKind::FilePath => "file_path",
        ReferenceKind::Url => "url",
        ReferenceKind::Concept => "concept",
        ReferenceKind::Symbol => "symbol",
    });
}
