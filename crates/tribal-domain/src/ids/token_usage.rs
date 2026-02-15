use super::common::define_id;

define_id!(
    /// Unique identifier for a token usage record.
    TokenUsageId,
    "tu"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(TokenUsageId, "tu");
}
