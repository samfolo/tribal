# Run all tests
test:
    cargo test --workspace

# Run only unit tests (no database required)
test-unit:
    cargo test --workspace --lib

# Format and lint check (no live database required)
check:
    cargo +nightly fmt --all -- --check
    SQLX_OFFLINE=true cargo clippy --workspace --all-targets -- -D warnings

# Format code
fmt:
    cargo +nightly fmt --all

# Start local Postgres (pgvector) via Docker
db-up:
    #!/usr/bin/env bash
    set -euo pipefail
    if docker ps -q --filter "name=^tribal-postgres$" | grep -q .; then
        echo "tribal-postgres is already running"
    elif docker ps -aq --filter "name=^tribal-postgres$" | grep -q .; then
        docker start tribal-postgres
        echo "tribal-postgres started"
    else
        docker run -d --name tribal-postgres \
            -e POSTGRES_USER=tribal \
            -e POSTGRES_PASSWORD=tribal \
            -e POSTGRES_DB=tribal \
            -p 5432:5432 \
            ankane/pgvector:latest
        echo "tribal-postgres created and started"
    fi

# Stop and remove local Postgres
db-down:
    docker rm -f tribal-postgres

# Run database migrations against local Postgres
db-migrate:
    cargo run -p tribal-server -- setup

# Regenerate sqlx offline query metadata
sqlx-prepare:
    cargo sqlx prepare --workspace

# Full pre-push check (what CI will run)
pre-push: fmt check sqlx-prepare test

# Run the MCP server locally (stdio mode)
serve:
    cargo run -p tribal-server -- serve --transport stdio

# Run the MCP server with a specific project
serve-project project_id:
    cargo run -p tribal-server -- serve --transport stdio --project {{project_id}}
