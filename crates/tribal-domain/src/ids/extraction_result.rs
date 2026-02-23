use super::common::define_id;

define_id!(
    /// Unique identifier for an extraction result.
    ExtractionResultId,
    "exr"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(ExtractionResultId, "exr");
}
