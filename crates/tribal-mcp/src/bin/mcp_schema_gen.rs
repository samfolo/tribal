//! Regenerates the advertised MCP tool schemas and the first-party client
//! contract from the typed registry. Invoked by `just mcp-schema`; the
//! in-suite drift test (`tests/contract_schema.rs`) proves the committed
//! files equal this output.

use std::{fs, path::Path};

use tribal_mcp::contract::schema::{advertised_tool_schemas, client_contract};

/// Directory (relative to the crate root) the per-tool schema pairs live
/// under — the same paths `tools.rs` reads with `include_str!`.
const SCHEMAS_DIR: &str = "src/schemas";

/// The first-party client contract, relative to the crate root.
const CLIENT_CONTRACT: &str = "contracts/client.json";

fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for tool in advertised_tool_schemas() {
        let tool_dir = root.join(SCHEMAS_DIR).join(tool.directory);
        fs::create_dir_all(&tool_dir).expect("create the tool's schema directory");
        write(&tool_dir.join("input.json"), &tool.input);
        write(&tool_dir.join("output.json"), &tool.output);
    }
    let contract_path = root.join(CLIENT_CONTRACT);
    fs::create_dir_all(contract_path.parent().expect("contract path has a parent"))
        .expect("create the contracts directory");
    write(&contract_path, &client_contract());
    println!("\u{2713} mcp-schema: regenerated tool schemas and the client contract");
}

fn write(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap_or_else(|e| {
        panic!("writing {}: {e}", path.display());
    });
}
