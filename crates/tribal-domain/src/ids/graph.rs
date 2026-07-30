use super::common::define_id;

define_id!(
    /// Stable identity of one logical knowledge graph.
    ///
    /// Created and stored by the graph database itself; endpoint,
    /// credential, project, account, and device identities never
    /// substitute for it.
    GraphId,
    "graph"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(GraphId, "graph");
}
