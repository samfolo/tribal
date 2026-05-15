#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Tribal server binary — CLI entry point.

use std::process::ExitCode;

use tribal::App;

fn main() -> ExitCode {
    match App::parse().run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            u8::try_from(err.exit_code()).map_or(ExitCode::FAILURE, ExitCode::from)
        }
    }
}
