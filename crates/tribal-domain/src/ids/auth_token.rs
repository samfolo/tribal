use super::common::define_id;

define_id!(
    /// Unique identifier for an authentication token.
    AuthTokenId,
    "at"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(AuthTokenId, "at");
}
