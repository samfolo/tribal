//! Permission scope type and satisfaction logic.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SCOPE_ROOT: &str = "tribal";

const INVALID_SCOPE: &str = "invalid scope";
const EXPECT_HARDCODED_SCOPE: &str = "invariant: hard-coded scope literal is valid";

// ---------------------------------------------------------------------------
// ScopeParseError
// ---------------------------------------------------------------------------

/// Error returned when a scope string fails validation.
#[derive(Debug, thiserror::Error)]
pub enum ScopeParseError {
    /// The input does not conform to scope syntax.
    #[error("{INVALID_SCOPE}: {input:?}")]
    InvalidScope {
        /// The raw input that failed validation.
        input: String,
    },
}

// ---------------------------------------------------------------------------
// Scope
// ---------------------------------------------------------------------------

/// A validated permission scope in `resource.path:operation` form.
///
/// Constructed via [`Scope::parse`], [`FromStr`], or [`TryFrom<&str>`],
/// which enforce syntax rules: exactly one colon separating a
/// dot-segmented resource path (starting with `tribal`, lowercase
/// ASCII only) from an operation (`read` or `write`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Scope(String);

impl Scope {
    /// Root read scope — grants read access to all resources.
    pub const FULL_ACCESS_READ: &str = "tribal:read";

    /// Root write scope — grants write access to all resources.
    pub const FULL_ACCESS_WRITE: &str = "tribal:write";

    /// Parses and validates a raw scope string.
    ///
    /// # Errors
    ///
    /// Returns [`ScopeParseError::InvalidScope`] if the input does not
    /// conform to scope syntax.
    pub fn parse(raw: &str) -> Result<Self, ScopeParseError> {
        if is_valid_scope(raw) {
            Ok(Self(raw.to_owned()))
        } else {
            Err(ScopeParseError::InvalidScope {
                input: raw.to_owned(),
            })
        }
    }

    /// Returns the raw scope string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Checks whether this (granted) scope satisfies a required scope.
    ///
    /// A granted scope satisfies a required scope when the operations
    /// match exactly and the granted resource path is equal to, or a
    /// dot-boundary prefix of, the required resource path.
    ///
    /// Dot-boundary prefix means `tribal` satisfies `tribal.knowledge`
    /// but `tribal.know` does **not** satisfy `tribal.knowledge`.
    ///
    /// # Panics
    ///
    /// Panics if either scope's inner string lacks a colon. This cannot
    /// happen for values constructed through [`Scope::parse`].
    #[must_use]
    pub fn satisfies(&self, required: &Scope) -> bool {
        let (granted_resource, granted_op) = self.0.split_once(':').expect("validated scope");
        let (required_resource, required_op) = required.0.split_once(':').expect("validated scope");

        if granted_op != required_op {
            return false;
        }

        required_resource == granted_resource
            || (required_resource.starts_with(granted_resource)
                && required_resource.as_bytes().get(granted_resource.len()) == Some(&b'.'))
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Scope {
    type Err = ScopeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl TryFrom<&str> for Scope {
    type Error = ScopeParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<String> for Scope {
    type Error = ScopeParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl From<Scope> for String {
    fn from(scope: Scope) -> Self {
        scope.0
    }
}

// ---------------------------------------------------------------------------
// Public functions
// ---------------------------------------------------------------------------

/// Returns `true` when any granted scope satisfies the required scope.
#[must_use]
pub fn is_authorised(granted: &[Scope], required: &Scope) -> bool {
    granted.iter().any(|g| g.satisfies(required))
}

/// Returns the two root scopes granting full access.
///
/// # Panics
///
/// Panics if the hard-coded root scope literals are invalid. The values
/// are known-good constants — this is an invariant, not a runtime risk.
#[must_use]
pub fn full_access_scopes() -> Vec<Scope> {
    vec![
        Scope::parse(Scope::FULL_ACCESS_READ).expect(EXPECT_HARDCODED_SCOPE),
        Scope::parse(Scope::FULL_ACCESS_WRITE).expect(EXPECT_HARDCODED_SCOPE),
    ]
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Validates a raw scope string against syntax rules.
///
/// A valid scope has exactly one colon separating a resource path from
/// an operation. The resource path must start with the tribal prefix,
/// contain only lowercase ASCII letters and dots, must not start or end
/// with a dot, and must not contain consecutive dots. The operation must
/// be `read` or `write`.
fn is_valid_scope(raw: &str) -> bool {
    let Some((resource, operation)) = raw.split_once(':') else {
        return false;
    };

    // Reject if there is a second colon in the operation part.
    if operation.contains(':') {
        return false;
    }

    if !matches!(operation, "read" | "write") {
        return false;
    }

    if resource.is_empty() {
        return false;
    }

    // Must be exactly the root or start with "tribal." (prevents
    // "tribalist" matching without allocating).
    if resource != SCOPE_ROOT
        && !(resource.starts_with(SCOPE_ROOT)
            && resource.as_bytes().get(SCOPE_ROOT.len()) == Some(&b'.'))
    {
        return false;
    }

    // Only lowercase ASCII letters and dots.
    if !resource.chars().all(|c| c.is_ascii_lowercase() || c == '.') {
        return false;
    }

    // No leading dot, trailing dot, or consecutive dots.
    if resource.starts_with('.') || resource.ends_with('.') || resource.contains("..") {
        return false;
    }

    true
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Parsing -----------------------------------------------------------

    #[test]
    fn test_parse_valid_scopes() {
        let valid = [
            "tribal:read",
            "tribal:write",
            "tribal.knowledge:read",
            "tribal.knowledge:write",
            "tribal.knowledge.facts:read",
            "tribal.knowledge.facts:write",
            "tribal.jobs:read",
            "tribal.jobs:write",
        ];
        for raw in valid {
            assert!(Scope::parse(raw).is_ok(), "expected valid: {raw}");
        }
    }

    #[test]
    fn test_parse_rejects_invalid() {
        let invalid = [
            ("", "empty string"),
            ("tribal", "missing colon"),
            ("tribal:admin", "invalid operation"),
            ("other:read", "wrong prefix"),
            ("tribal.know ledge:read", "space in resource"),
            ("Tribal:read", "uppercase prefix"),
            (":read", "empty resource"),
            ("tribal:", "empty operation"),
            ("tribal::write", "double colon"),
            ("tribal.Knowledge:read", "uppercase segment"),
            ("tribal.read", "dot instead of colon"),
            ("tribal:read.", "trailing dot in operation"),
            ("tribal: read", "space in operation"),
            ("tribal.knowledge.write", "dot instead of colon separator"),
            ("tribal..knowledge:read", "consecutive dots"),
            ("tribalist:read", "prefix that extends beyond dot boundary"),
            ("tribal:Write", "capitalised operation"),
            ("tribal:/read", "slash in operation"),
            ("tribal:read,tribal:write", "comma-separated compound"),
            ("tribal.:read", "trailing dot in resource"),
            (".tribal:read", "leading dot in resource"),
            ("tribal:read\n", "trailing newline"),
            ("tribal:read ", "trailing space"),
            (" tribal:read", "leading space"),
        ];
        for (raw, description) in invalid {
            assert!(
                Scope::parse(raw).is_err(),
                "expected invalid ({description}): {raw:?}"
            );
        }
    }

    #[test]
    fn test_parse_error_display() {
        let err = Scope::parse("bad").unwrap_err();
        assert_eq!(err.to_string(), format!("{INVALID_SCOPE}: {:?}", "bad"));
    }

    // -- Satisfaction (table tests) ----------------------------------------

    #[test]
    fn test_satisfied_pairs() {
        let cases = [
            ("tribal:read", "tribal:read"),
            ("tribal:write", "tribal:write"),
            ("tribal:read", "tribal.knowledge:read"),
            ("tribal:read", "tribal.knowledge.facts:read"),
            ("tribal.knowledge:read", "tribal.knowledge:read"),
            ("tribal.knowledge:read", "tribal.knowledge.facts:read"),
            ("tribal:write", "tribal.knowledge:write"),
            ("tribal:write", "tribal.jobs:write"),
        ];
        for (granted, required) in cases {
            let g = Scope::parse(granted).unwrap();
            let r = Scope::parse(required).unwrap();
            assert!(g.satisfies(&r), "expected {granted} to satisfy {required}");
        }
    }

    #[test]
    fn test_unsatisfied_pairs() {
        let cases = [
            ("tribal:read", "tribal:write"),
            ("tribal:read", "tribal.knowledge:write"),
            ("tribal.knowledge:read", "tribal.jobs:read"),
            ("tribal.know:read", "tribal.knowledge:read"),
            ("tribal.knowledge:write", "tribal.knowledge:read"),
            ("tribal.knowledge.facts:read", "tribal.knowledge:read"),
            ("tribal.jobs:read", "tribal:read"),
        ];
        for (granted, required) in cases {
            let g = Scope::parse(granted).unwrap();
            let r = Scope::parse(required).unwrap();
            assert!(
                !g.satisfies(&r),
                "expected {granted} NOT to satisfy {required}"
            );
        }
    }

    // -- is_authorised -----------------------------------------------------

    #[test]
    fn test_is_authorised_any_match() {
        let granted = vec![
            Scope::parse("tribal.knowledge:read").unwrap(),
            Scope::parse("tribal:write").unwrap(),
        ];
        let required = Scope::parse("tribal.knowledge:read").unwrap();
        assert!(is_authorised(&granted, &required));
    }

    #[test]
    fn test_is_authorised_none_match() {
        let granted = vec![Scope::parse("tribal.knowledge:read").unwrap()];
        let required = Scope::parse("tribal:write").unwrap();
        assert!(!is_authorised(&granted, &required));
    }

    #[test]
    fn test_is_authorised_empty_grants() {
        let required = Scope::parse("tribal:read").unwrap();
        assert!(!is_authorised(&[], &required));
    }

    // -- Display / FromStr / TryFrom roundtrip -----------------------------

    #[test]
    fn test_display_and_from_str_roundtrip() {
        let scope = Scope::parse("tribal.knowledge:read").unwrap();
        let display = scope.to_string();
        assert_eq!(display, "tribal.knowledge:read");

        let parsed: Scope = display.parse().unwrap();
        assert_eq!(parsed, scope);
    }

    #[test]
    fn test_try_from_str() {
        let scope = Scope::try_from("tribal:read").unwrap();
        assert_eq!(scope.as_str(), "tribal:read");

        assert!(Scope::try_from("bad").is_err());
    }

    // -- Serde roundtrip ---------------------------------------------------

    #[test]
    fn test_serde_roundtrip() {
        let scope = Scope::parse("tribal.knowledge:read").unwrap();
        let json = serde_json::to_string(&scope).unwrap();
        assert_eq!(json, "\"tribal.knowledge:read\"");

        let parsed: Scope = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, scope);
    }

    #[test]
    fn test_serde_rejects_invalid_scope() {
        let result = serde_json::from_str::<Scope>("\"bad\"");
        assert!(result.is_err());
    }

    // -- full_access_scopes ------------------------------------------------

    #[test]
    fn test_full_access_scopes() {
        let scopes = full_access_scopes();
        assert_eq!(scopes.len(), 2);
        assert_eq!(scopes[0].as_str(), Scope::FULL_ACCESS_READ);
        assert_eq!(scopes[1].as_str(), Scope::FULL_ACCESS_WRITE);
    }
}
