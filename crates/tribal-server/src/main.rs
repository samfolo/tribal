#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Tribal server binary — CLI entry point.

use std::process;

use tribal::App;

fn main() {
    if let Err(err) = App::parse().run() {
        eprintln!("{err}");
        process::exit(err.exit_code());
    }
}
