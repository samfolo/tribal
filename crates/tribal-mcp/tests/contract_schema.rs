//! Drift gate: the committed advertised schemas and client contract equal
//! the typed registry's own rendering, so a DTO or declaration change
//! fails the suite until `just mcp-schema` recommits the projection.
#![cfg(feature = "schema")]

use tribal_mcp::contract::schema::{advertised_tool_schemas, client_contract};

#[test]
fn test_the_committed_tool_schemas_match_the_registry() {
    for tool in advertised_tool_schemas() {
        for (kind, generated) in [("input", &tool.input), ("output", &tool.output)] {
            let path = format!(
                "{}/src/schemas/{}/{kind}.json",
                env!("CARGO_MANIFEST_DIR"),
                tool.directory,
            );
            let committed =
                std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {path}: {e}"));
            assert_eq!(
                &committed, generated,
                "{path} drifted from the registry; run `just mcp-schema`",
            );
        }
    }
}

#[test]
fn test_the_committed_client_contract_matches_the_registry() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/contracts/client.json");
    let committed = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {path}: {e}"));
    assert_eq!(
        committed,
        client_contract(),
        "contracts/client.json drifted from the registry; run `just mcp-schema`",
    );
}
