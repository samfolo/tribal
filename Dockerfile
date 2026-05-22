FROM rust:bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p tribal

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates bash jq curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/tribal /usr/local/bin/tribal
COPY scripts/tribal-entrypoint /usr/local/bin/tribal-entrypoint
RUN chmod +x /usr/local/bin/tribal-entrypoint
ENTRYPOINT ["/usr/local/bin/tribal-entrypoint"]
