FROM rust:bookworm AS chef
# Pinned so future cargo-chef releases cannot change recipe semantics
# without an intentional bump here; Dependabot (or a manual sweep) can
# advance the version when needed.
RUN cargo install cargo-chef --version 0.1.77 --locked
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
# `.git/` is excluded from the build context (see `.dockerignore`), so
# `build.rs` cannot derive the build version from `git describe` here.
# Callers inject the host's `git describe` output via this build arg;
# `build.rs` reads `TRIBAL_GIT_DESCRIBE` and embeds it into the binary
# (with a `CARGO_PKG_VERSION` fallback if the arg is empty). Without
# this plumbing every Docker image would share the same fingerprint
# `build_version`, collapsing eval/feedback rows across releases.
ARG TRIBAL_GIT_DESCRIBE=""
ENV SQLX_OFFLINE=true \
    TRIBAL_GIT_DESCRIBE=${TRIBAL_GIT_DESCRIBE}
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
COPY prompts/ ./prompts/
COPY .sqlx/ ./.sqlx/
RUN cargo build --release -p tribal

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates bash jq curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 tribal \
    && useradd --system --uid 10001 --gid tribal --home-dir /var/lib/tribal --no-create-home --shell /usr/sbin/nologin tribal \
    && mkdir -p /var/lib/tribal \
    && chown -R tribal:tribal /var/lib/tribal
COPY --from=builder /app/target/release/tribal /usr/local/bin/tribal
COPY --chmod=0755 scripts/tribal-entrypoint /usr/local/bin/tribal-entrypoint
USER tribal:tribal
# MCP Registry ownership: this value must equal server.json's `name`, so the
# registry can verify we own the published image (see server.json + the
# publish-mcp-registry job in .github/workflows/docker.yml).
LABEL io.modelcontextprotocol.server.name="io.github.tribal-memory/tribal"
EXPOSE 8725
ENTRYPOINT ["/usr/local/bin/tribal-entrypoint"]
