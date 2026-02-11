FROM rust:1-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p tribal-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/tribal-server /usr/local/bin/tribal
ENTRYPOINT ["tribal"]
CMD ["serve", "--transport", "http"]
