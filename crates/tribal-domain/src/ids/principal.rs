use super::common::define_id;

define_id!(
    /// Unique identifier for a principal (user or agent identity).
    PrincipalId,
    "prin"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(PrincipalId, "prin");
}
