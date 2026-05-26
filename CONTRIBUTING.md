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

## Releasing

Releases are tag-driven. Pushing a `vX.Y.Z` tag builds the binaries, installers, and Homebrew formula (via cargo-dist) and publishes the Docker image. The GitHub Release body is taken from the matching `CHANGELOG.md` section, so the changelog *is* the release notes.

To cut a release:

1. Update `CHANGELOG.md`: rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD`, curate the entries for readers (group under Added / Changed / Fixed / Removed, drop internal churn), add a fresh empty `## [Unreleased]`, and update the compare links at the bottom. `git log vPREV..HEAD` is a useful starting point, since commits follow Conventional Commits.
2. Bump `workspace.package.version` in `Cargo.toml`, then run `cargo update --workspace` to sync `Cargo.lock`.
3. Bump the pinned image tag in `docker-compose.yml` to the same version. The README and the installation skill fetch the compose file from the release tag, so it must reference the image that tag publishes.
4. Land the above on `main`, then tag that commit `vX.Y.Z` and push the tag.

The Docker workflow refuses to publish if the tag does not match `workspace.package.version`, so steps 2 and 4 must agree.
