//! Column-list formatting for SQL queries.
//!
//! Repositories declare their columns as a `Columns` constant and
//! interpolate it directly into `format!()` strings via its `Display`
//! implementation.  For CTE queries that need a table qualifier,
//! `Columns::qualified` produces `"t.id, t.name, …"`.

use std::fmt;

/// A static list of column names for a repository table.
///
/// Implements [`Display`] to produce a comma-separated list suitable
/// for `SELECT` and `RETURNING` clauses.
///
/// ```text
/// const COLUMNS: Columns = Columns(&["id", "name"]);
/// format!("SELECT {COLUMNS} FROM table")  // => "SELECT id, name FROM table"
/// ```
pub(crate) struct Columns(pub(crate) &'static [&'static str]);

impl fmt::Display for Columns {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut iter = self.0.iter();
        if let Some(first) = iter.next() {
            write!(f, "{first}")?;
            for col in iter {
                write!(f, ", {col}")?;
            }
        }
        Ok(())
    }
}

impl Columns {
    /// Formats each column with a table-qualifier prefix.
    ///
    /// Used in CTE `RETURNING` clauses where the table alias is required.
    ///
    /// ```text
    /// COLUMNS.qualified("t")  // => "t.id, t.name"
    /// ```
    pub(crate) fn qualified(&self, qualifier: &str) -> String {
        self.0
            .iter()
            .map(|c| format!("{qualifier}.{c}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_joins_with_commas() {
        let cols = Columns(&["id", "name", "created_at"]);
        assert_eq!(cols.to_string(), "id, name, created_at");
    }

    #[test]
    fn test_display_single_element() {
        let cols = Columns(&["id"]);
        assert_eq!(cols.to_string(), "id");
    }

    #[test]
    fn test_qualified_prefixes_each() {
        let cols = Columns(&["id", "name"]);
        assert_eq!(cols.qualified("t"), "t.id, t.name");
    }
}
