#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Tribal server binary — CLI entry point.

mod app;
mod cli;
mod error;
mod startup;

use std::process;

use app::App;

fn main() {
    if let Err(err) = App::new().run() {
        eprintln!("{err}");
        process::exit(err.exit_code());
    }
}
