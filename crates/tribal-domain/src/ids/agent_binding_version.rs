use super::common::define_id;

define_id!(
    /// Unique identifier for one content-addressed agent binding version.
    ///
    /// A binding version pins what a thread ran: the stage's agent
    /// definition and its serialised tool descriptors, hashed together.
    /// Threads reference the version they were admitted under.
    AgentBindingVersionId,
    "abv"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(AgentBindingVersionId, "abv");
}
