use super::common::define_id;

/// Unique identifier for a reference.
define_id!(ReferenceId, "ref");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(ReferenceId, "ref");
}
