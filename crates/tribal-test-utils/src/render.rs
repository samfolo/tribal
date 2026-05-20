//! Snapshot-friendly capture helper for themed and JSON writers.
//!
//! Test fixtures across the workspace share the same boilerplate when
//! exercising a `write_*` function: allocate a `Vec<u8>`, wrap it in
//! `AutoStream` with SGR escapes disabled, invoke the writer, decode
//! the bytes as UTF-8.  This helper centralises that pattern.

use std::io::{self, Write};

use anstream::{AutoStream, ColorChoice};

/// Captures `writer` output into a `String`.
///
/// The writer runs against an `AutoStream` configured with
/// [`ColorChoice::Never`], so SGR escapes are stripped and snapshots
/// stay byte-stable across terminal capabilities.
///
/// # Panics
///
/// Panics if `writer` returns an error or its output is not valid
/// UTF-8.  Both are programmer errors in a test fixture — neither is a
/// recoverable runtime condition.
pub fn render_to_string<F>(writer: F) -> String
where
    F: FnOnce(&mut dyn Write) -> io::Result<()>,
{
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut stream = AutoStream::new(&mut buf, ColorChoice::Never);
        writer(&mut stream).expect("test-fixture writer succeeds");
    }
    String::from_utf8(buf).expect("test-fixture writer output is utf8")
}
