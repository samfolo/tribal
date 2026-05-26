# Contributing

Tribal is a Rust workspace. Development tasks run through [`just`](https://github.com/casey/just); the `justfile` is the source of truth for the full command list (`just --list`).

## Prerequisites

- [Rust](https://rustup.rs/): stable toolchain, 1.93 or higher
- Rust nightly: required for `rustfmt` (`rustup toolchain install nightly`)
- [Docker](https://www.docker.com/): local Postgres and test containers
- [`just`](https://github.com/casey/just): `cargo install just`
- [`sqlx-cli`](https://github.com/launchbadge/sqlx): `cargo install sqlx-cli --no-default-features --features postgres`
- [`cargo-deny`](https://github.com/EmbarkStudios/cargo-deny): `cargo install cargo-deny`

## Setup

```bash
git clone git@github.com:tribal-memory/tribal.git
cd tribal
just db-up        # start local Postgres
just db-migrate   # apply migrations
just check        # format check and lint
just test         # full test suite
```

## Common commands

```bash
just fmt            # format (requires nightly rustfmt)
just check          # lint and format check
just test           # all tests
just test-unit      # unit tests only (no database required)
just pre-push       # full pre-push gate: fmt, check, sqlx-prepare, test
just sqlx-prepare   # regenerate sqlx offline query metadata
just serve          # run the MCP server (stdio transport)
```

Run `just --list` for the complete set.
