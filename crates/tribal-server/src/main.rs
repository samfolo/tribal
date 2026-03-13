#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Tribal server binary — CLI entry point.

mod app;
mod cli;
mod error;

use std::process;

use app::App;

fn main() {
    if let Err(err) = App::new().run() {
        eprintln!("{err}");
        process::exit(1);
    }
}
