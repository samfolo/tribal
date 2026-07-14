use super::common::define_id;

define_id!(
    /// Durable generation shared by a default-credential mapping and envelope.
    CredentialGenerationId,
    "cg"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(CredentialGenerationId, "cg");
}
