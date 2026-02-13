use super::common::define_id;

/// Unique identifier for a project.
define_id!(ProjectId, "proj");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(ProjectId, "proj");
}
