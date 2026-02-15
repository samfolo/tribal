# Tribal

A personal knowledge context engine for software development. Tribal captures
learnings, debugging insights, heuristics, and decision rationale from
development work, then makes that knowledge retrievable by coding agents via
MCP (Model Context Protocol).

## Prerequisites

- [Rust](https://rustup.rs/) (stable toolchain, 1.93+)
- [Rust nightly](https://rustup.rs/) (for rustfmt — `rustup toolchain install nightly`)
- [Docker](https://www.docker.com/) (for local Postgres and test containers)
- [just](https://github.com/casey/just) — `cargo install just`
- [sqlx-cli](https://github.com/launchbadge/sqlx) — `cargo install sqlx-cli --no-default-features --features postgres`
- [cargo-deny](https://github.com/EmbarkStudios/cargo-deny) — `cargo install cargo-deny`

## Setup

```bash
git clone git@github.com:samfolo/tribal.git
cd tribal
just db-up
just db-migrate
just check
just test
```

## Running

```bash
# Run the MCP server (stdio transport)
just serve

# Run with a specific project
just serve-project <project_id>
```

## Development

```bash
# Format code (requires nightly rustfmt)
just fmt

# Lint and format check
just check

# Run all tests
just test

# Run unit tests only (no database required)
just test-unit

# Full pre-push check (format, lint, test)
just pre-push

# Regenerate sqlx offline query metadata
just sqlx-prepare
```

## Licence

MIT
