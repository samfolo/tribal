//! Column-list formatting helpers for SQL queries.
//!
//! Repositories declare their columns as `&[&str]` slices and use these
//! helpers to produce comma-separated lists for `SELECT` and `RETURNING`
//! clauses.

/// Joins column names into a comma-separated list.
///
/// ```text
/// columns(&["id", "name"]) => "id, name"
/// ```
pub(crate) fn columns(cols: &[&str]) -> String {
    cols.join(", ")
}

/// Joins column names with a table qualifier prefix.
///
/// Used in CTE `RETURNING` clauses where the table alias is required.
///
/// ```text
/// qualified_columns(&["id", "name"], "t") => "t.id, t.name"
/// ```
pub(crate) fn qualified_columns(cols: &[&str], qualifier: &str) -> String {
    cols.iter()
        .map(|c| format!("{qualifier}.{c}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_columns_joins_with_commas() {
        assert_eq!(
            columns(&["id", "name", "created_at"]),
            "id, name, created_at"
        );
    }

    #[test]
    fn test_columns_single_element() {
        assert_eq!(columns(&["id"]), "id");
    }

    #[test]
    fn test_qualified_columns_prefixes_each() {
        assert_eq!(qualified_columns(&["id", "name"], "t"), "t.id, t.name",);
    }
}
