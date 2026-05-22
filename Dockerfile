FROM rust:bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ENV SQLX_OFFLINE=true
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
ENTRYPOINT ["/usr/local/bin/tribal-entrypoint"]
