//! Errors from the runtime database layer.

/// A failure interacting with the runtime database. Outcomes that are part of
/// normal operation — a lost claim, a saturated tenant — are returned as typed
/// results by the repositories, never as errors.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeDbError {
    /// A query failed against the database.
    #[error("runtime-db query failed [{context}]")]
    QueryFailed {
        /// What the layer was doing when the query failed.
        context: String,
        /// The underlying sqlx error.
        #[source]
        source: sqlx::Error,
    },

    /// A stored value did not parse back into its typed form — a row this layer
    /// wrote is no longer well-formed.
    #[error("runtime-db row is malformed [{context}]")]
    Malformed {
        /// What the layer was reading when the value failed to parse.
        context: String,
    },
}
