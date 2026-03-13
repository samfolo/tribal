//! Build script for `tribal-server`.
//!
//! Embeds the short git description into the binary so that
//! `tribal --version` can display the exact commit.

fn main() {
    let git_describe = std::process::Command::new("git")
        .args(["describe", "--always", "--dirty", "--tags"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=TRIBAL_GIT_DESCRIBE={git_describe}");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/");
}
