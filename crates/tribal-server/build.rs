//! Build script for `tribal-server`.
//!
//! Embeds `TRIBAL_GIT_DESCRIBE` into the binary for `tribal --version` and
//! the MCP system fingerprint (`crates/tribal-mcp/src/fingerprint.rs`).
//! Cascade: env var (set by Docker builds, which strip `.git/`) →
//! `git describe --always --tags` → `CARGO_PKG_VERSION`. `--dirty` is
//! omitted because tracking working-tree state via `rerun-if-changed`
//! would mean declaring every workspace source as an input; the embedded
//! value reflects the last commit regardless of dirtiness.

fn main() {
    println!("cargo:rerun-if-env-changed=TRIBAL_GIT_DESCRIBE");
    // `rerun-if-changed` paths are interpreted relative to the package
    // manifest directory (`crates/tribal-server/`), so the workspace's
    // `.git/` is two levels up.
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs");
    println!("cargo:rerun-if-changed=../../.git/packed-refs");

    let build_version = std::env::var("TRIBAL_GIT_DESCRIBE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(git_describe)
        .unwrap_or_else(|| {
            std::env::var("CARGO_PKG_VERSION")
                .expect("CARGO_PKG_VERSION is set by Cargo during build")
        });

    println!("cargo:rustc-env=TRIBAL_GIT_DESCRIBE={build_version}");
}

fn git_describe() -> Option<String> {
    std::process::Command::new("git")
        .args(["describe", "--always", "--tags"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|value| !value.is_empty())
}
