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

    #[test]
    fn test_reference_kind_serde_roundtrip() {
        let variants = [
            (ReferenceKind::FilePath, "\"file_path\""),
            (ReferenceKind::Url, "\"url\""),
            (ReferenceKind::Concept, "\"concept\""),
            (ReferenceKind::Symbol, "\"symbol\""),
        ];
        for (variant, expected_json) in variants {
            let json = serde_json::to_string(&variant).expect("should serialise");
            assert_eq!(json, expected_json, "serialised form of {variant:?}");
            let parsed: ReferenceKind =
                serde_json::from_str(&json).expect("should deserialise");
            assert_eq!(parsed, variant);
        }
    }
}
