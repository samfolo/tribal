use std::{env, error::Error, fs, path::PathBuf};

use serde::Deserialize;

const METADATA_PATH: &str = "management-contract.json";
const METADATA: &str = include_str!("management-contract.json");

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManagementContractMetadata {
    version: u16,
    bootstrap_timeout_seconds: u64,
    request_timeout_seconds: u64,
}

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed={METADATA_PATH}");
    let metadata: ManagementContractMetadata = serde_json::from_str(METADATA)?;
    let generated = format!(
        "/// The version of the public local-management contract.\n\
         pub const MANAGEMENT_CONTRACT_VERSION: u16 = {};\n\
         /// Deadline for admitting a management connection.\n\
         pub const MANAGEMENT_BOOTSTRAP_TIMEOUT_SECONDS: u64 = {};\n\
         /// Client deadline for one accepted management application call.\n\
         pub const MANAGEMENT_REQUEST_TIMEOUT_SECONDS: u64 = {};\n",
        metadata.version, metadata.bootstrap_timeout_seconds, metadata.request_timeout_seconds,
    );
    fs::write(
        PathBuf::from(env::var("OUT_DIR")?).join("management_contract_metadata.rs"),
        generated,
    )?;
    Ok(())
}
