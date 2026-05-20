//! Collector for [`ValidationError`] diagnostics produced by the
//! configuration validator.
//!
//! Section validators take `&mut Diagnostics` and push typed errors.
//! Affordances mirror `Vec<ValidationError>` (`push`, `iter`, `len`,
//! `is_empty`, `into_inner`) plus collector-style composition via
//! [`Add`], [`AddAssign`], [`Sum`], and [`FromIterator`] so sub-
//! collections can be built independently and concatenated.
//!
//! Presentation-neutral by design: callers iterate the collection and
//! compose output for their own context.

use std::{
    iter::Sum,
    ops::{Add, AddAssign},
};

use super::error::ValidationError;

// ---------------------------------------------------------------------------
// Diagnostics
// ---------------------------------------------------------------------------

/// A collected set of configuration-validation diagnostics.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Diagnostics(Vec<ValidationError>);

impl Diagnostics {
    /// Appends a single diagnostic.
    pub fn push(&mut self, diagnostic: ValidationError) {
        self.0.push(diagnostic);
    }

    /// Returns `true` if no diagnostics have been collected.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of diagnostics collected.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Iterates over the collected diagnostics in push order.
    pub fn iter(&self) -> std::slice::Iter<'_, ValidationError> {
        self.0.iter()
    }

    /// Borrows the collection as a slice.  Lets callers reach for the
    /// standard `&[T]` API (`first`, `split_at`, `chunks`, …) without
    /// the wrapper widening its own surface to mirror every accessor.
    #[must_use]
    pub fn as_slice(&self) -> &[ValidationError] {
        &self.0
    }

    /// Consumes the collection into its inner vector.
    #[must_use]
    pub fn into_inner(self) -> Vec<ValidationError> {
        self.0
    }
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

impl From<Vec<ValidationError>> for Diagnostics {
    fn from(diagnostics: Vec<ValidationError>) -> Self {
        Self(diagnostics)
    }
}

impl FromIterator<ValidationError> for Diagnostics {
    fn from_iter<I: IntoIterator<Item = ValidationError>>(iter: I) -> Self {
        Self(Vec::from_iter(iter))
    }
}

// ---------------------------------------------------------------------------
// IntoIterator
// ---------------------------------------------------------------------------

impl<'a> IntoIterator for &'a Diagnostics {
    type IntoIter = std::slice::Iter<'a, ValidationError>;
    type Item = &'a ValidationError;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for Diagnostics {
    type IntoIter = std::vec::IntoIter<ValidationError>;
    type Item = ValidationError;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

// ---------------------------------------------------------------------------
// Arithmetic composition
// ---------------------------------------------------------------------------

impl Add for Diagnostics {
    type Output = Self;

    fn add(mut self, other: Self) -> Self {
        self.0.extend(other.0);
        self
    }
}

impl AddAssign for Diagnostics {
    fn add_assign(&mut self, other: Self) {
        self.0.extend(other.0);
    }
}

impl Sum for Diagnostics {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |acc, d| acc + d)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::error::ConfigPath;

    /// Cheap test fixture — `Empty` is the simplest variant and is
    /// readily distinguished by its `field` path in assertions.
    fn empty_diagnostic(path: &'static str) -> ValidationError {
        ValidationError::Empty {
            field: ConfigPath::from_static(path),
        }
    }

    // -- Basic operations ---------------------------------------------------

    #[test]
    fn test_default_is_empty() {
        let d = Diagnostics::default();
        assert!(d.is_empty());
        assert_eq!(d.len(), 0);
    }

    #[test]
    fn test_push_grows_collection_and_preserves_order() {
        let mut d = Diagnostics::default();
        d.push(empty_diagnostic("a.b"));
        d.push(empty_diagnostic("c.d"));
        assert!(!d.is_empty());
        assert_eq!(d.len(), 2);
        let collected: Vec<_> = d.iter().collect();
        assert!(matches!(
            collected[0],
            ValidationError::Empty { field } if field.as_str() == "a.b",
        ));
        assert!(matches!(
            collected[1],
            ValidationError::Empty { field } if field.as_str() == "c.d",
        ));
    }

    #[test]
    fn test_as_slice_borrows_underlying_view() {
        let mut d = Diagnostics::default();
        d.push(empty_diagnostic("a"));
        d.push(empty_diagnostic("b"));
        let s = d.as_slice();
        assert_eq!(s.len(), 2);
        assert!(matches!(
            &s[0],
            ValidationError::Empty { field } if field.as_str() == "a",
        ));
        // The collection is unchanged after borrowing.
        assert_eq!(d.len(), 2);
    }

    #[test]
    fn test_into_inner_returns_underlying_vec() {
        let mut d = Diagnostics::default();
        d.push(empty_diagnostic("a"));
        d.push(empty_diagnostic("b"));
        let v = d.into_inner();
        assert_eq!(v.len(), 2);
    }

    // -- Conversions --------------------------------------------------------

    #[test]
    fn test_from_vec_wraps_inner_in_order() {
        let v = vec![empty_diagnostic("a"), empty_diagnostic("b")];
        let d = Diagnostics::from(v);
        let collected: Vec<_> = d.iter().collect();
        assert_eq!(collected.len(), 2);
        assert!(matches!(
            collected[0],
            ValidationError::Empty { field } if field.as_str() == "a",
        ));
    }

    #[test]
    fn test_from_iter_collects_into_diagnostics() {
        let d: Diagnostics = [empty_diagnostic("a"), empty_diagnostic("b")]
            .into_iter()
            .collect();
        assert_eq!(d.len(), 2);
    }

    // -- IntoIterator -------------------------------------------------------

    #[test]
    fn test_into_iter_owned_consumes_collection() {
        let mut d = Diagnostics::default();
        d.push(empty_diagnostic("a"));
        d.push(empty_diagnostic("b"));
        let items: Vec<ValidationError> = d.into_iter().collect();
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn test_into_iter_borrowed_yields_references() {
        let mut d = Diagnostics::default();
        d.push(empty_diagnostic("a"));
        let items: Vec<&ValidationError> = (&d).into_iter().collect();
        assert_eq!(items.len(), 1);
        // `d` is still usable since we borrowed.
        assert_eq!(d.len(), 1);
    }

    // -- Arithmetic composition --------------------------------------------

    #[test]
    fn test_add_concatenates_two_diagnostics() {
        let left = Diagnostics::from(vec![empty_diagnostic("a")]);
        let right = Diagnostics::from(vec![empty_diagnostic("b"), empty_diagnostic("c")]);
        let combined = left + right;
        assert_eq!(combined.len(), 3);
    }

    #[test]
    fn test_add_assign_extends_in_place() {
        let mut d = Diagnostics::from(vec![empty_diagnostic("a")]);
        let other = Diagnostics::from(vec![empty_diagnostic("b")]);
        d += other;
        assert_eq!(d.len(), 2);
    }

    #[test]
    fn test_sum_aggregates_iterator_of_diagnostics() {
        let groups = vec![
            Diagnostics::from(vec![empty_diagnostic("a")]),
            Diagnostics::from(vec![empty_diagnostic("b"), empty_diagnostic("c")]),
            Diagnostics::default(),
            Diagnostics::from(vec![empty_diagnostic("d")]),
        ];
        let total: Diagnostics = groups.into_iter().sum();
        assert_eq!(total.len(), 4);
    }

    #[test]
    fn test_sum_of_empty_iterator_is_default() {
        let groups: Vec<Diagnostics> = vec![];
        let total: Diagnostics = groups.into_iter().sum();
        assert!(total.is_empty());
    }
}
