//! Builds the shared `tribal_template` test database on the server pointed to
//! by `DATABASE_URL`.
//!
//! Run once by the `just test` wrapper before `cargo nextest`, so every test
//! process clones from a ready template rather than racing to build it. The
//! build is idempotent (advisory-locked), so re-running is harmless.

use std::process::ExitCode;

fn main() -> ExitCode {
    let Ok(base_url) = std::env::var("DATABASE_URL") else {
        eprintln!("build-test-template: DATABASE_URL must be set");
        return ExitCode::FAILURE;
    };

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("build-test-template: failed to start tokio runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(tribal_test_utils::build_test_template(
        &base_url,
        &tribal_db::MIGRATOR,
    )) {
        Ok(()) => {
            println!("build-test-template: template ready");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("build-test-template: {error}");
            ExitCode::FAILURE
        }
    }
}
