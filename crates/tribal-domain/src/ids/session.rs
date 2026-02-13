use super::common::define_id;

/// Unique identifier for a session.
define_id!(SessionId, "sess");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(SessionId, "sess");
}
