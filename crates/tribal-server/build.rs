//! Build script for `tribal-server`.
//!
//! Embeds a build-version string into the binary so that `tribal --version`,
//! the MCP system fingerprint (`crates/tribal-mcp/src/fingerprint.rs`), and the
//! `system_fingerprints` table can distinguish releases.
//!
//! The value is resolved in three tiers, in order:
//!
//! 1. `TRIBAL_GIT_DESCRIBE` from the environment. Builders without access to
//!    `.git/` (notably the Docker image build, which excludes `.git/` via
//!    `.dockerignore`) inject the host's `git describe` output via a build
//!    argument so the embedded value reflects the release commit.
//! 2. `git describe --always --dirty --tags` run against the current checkout.
//!    Standard path for `cargo build` inside a git working tree.
//! 3. `CARGO_PKG_VERSION` (the workspace package version). Last-resort
//!    fallback that still produces a meaningful, fingerprint-distinct value
//!    rather than a literal `"unknown"` — a binary built in a non-git context
//!    without an explicit env override at least carries its workspace version.

fn main() {
    println!("cargo:rerun-if-env-changed=TRIBAL_GIT_DESCRIBE");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/");

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
        .args(["describe", "--always", "--dirty", "--tags"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|value| !value.is_empty())
}
