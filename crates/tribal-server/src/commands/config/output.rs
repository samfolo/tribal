//! Terminal output for `tribal config` subcommands.

/// Prints the resolved configuration YAML to stdout.
pub(super) fn resolved_config(yaml: &str) {
    print!("{yaml}");
}
