use super::common::define_id;

define_id!(
    /// Unique identifier for a content-addressed prompt version.
    PromptVersionId,
    "pv"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(PromptVersionId, "pv");
}
