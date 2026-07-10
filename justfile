# Run the full test suite under nextest with template-clone-per-test isolation.
test:
    #!/usr/bin/env bash
    # Starts one ephemeral pgvector server, builds the migrated template once,
    # then runs every test in its own cloned database (fully parallel). The
    # server is torn down on exit; use `test-cargo` for the in-process fallback.
    set -euo pipefail
    # Compile against cached sqlx metadata; runtime queries hit each test's
    # cloned database, not the bare maintenance DB in DATABASE_URL.
    export SQLX_OFFLINE=true
    if ! command -v cargo-nextest >/dev/null 2>&1; then
        echo "cargo-nextest is required: cargo binstall cargo-nextest, or" >&2
        echo "  curl -LsSf https://get.nexte.st/latest/mac | tar zxf - -C \"\$HOME/.cargo/bin\"" >&2
        exit 1
    fi
    # Unique container name + a random host port, so concurrent runs (local or
    # a CI matrix) never collide on the name or the published port.
    name="cortex-testdb-$$"
    cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
    trap cleanup EXIT
    # Ephemeral server tuned for fast clones (fsync off — data is disposable).
    docker run -d --name "$name" --label org.cortex.testdb \
        -e POSTGRES_USER=tribal \
        -e POSTGRES_PASSWORD=tribal \
        -e POSTGRES_DB=postgres \
        -p 127.0.0.1::5432 \
        pgvector/pgvector:0.8.2-pg17 \
        -c max_connections=500 \
        -c fsync=off \
        -c full_page_writes=off \
        -c synchronous_commit=off >/dev/null
    # Discover the random host port Docker assigned.
    port=$(docker port "$name" 5432 | head -n1 | sed 's/.*://')
    export DATABASE_URL="postgres://tribal:tribal@localhost:${port}/postgres"
    echo "waiting for database on port ${port}..."
    ready=false
    for _ in $(seq 1 120); do
        if docker exec "$name" pg_isready -U tribal -d postgres >/dev/null 2>&1; then ready=true; break; fi
        sleep 0.5
    done
    if [ "$ready" != true ]; then
        echo "database did not become ready in time" >&2
        exit 1
    fi
    cargo run -q -p tribal-test-utils --bin build-test-template
    # Worker/server/db/auth/mcp run with the test-helpers feature; e2e uses real
    # HTTP mocks (wiremock) and must NOT enable the inference test-helper.
    # The wire and config schema features ride the main pass: one feature set
    # means one compiled graph — a feature-flipped pass rebuilds it twice.
    cargo nextest run --workspace --exclude tribal-e2e --features tribal/test-helpers,tribal-wire/schema,tribal-config/schema
    cargo nextest run -p tribal-e2e
    # nextest does not run doctests; run them separately (none require a database).
    cargo test --workspace --exclude tribal-e2e --features tribal/test-helpers,tribal-wire/schema,tribal-config/schema --doc

# Run the suite via plain `cargo test` (in-process testcontainers fallback).
test-cargo:
    SQLX_OFFLINE=true cargo test --workspace --exclude tribal-e2e --features tribal/test-helpers,tribal-wire/schema,tribal-config/schema
    SQLX_OFFLINE=true cargo test -p tribal-e2e

# Run only unit tests
test-unit:
    cargo test --workspace --lib --features tribal/test-helpers

# Run one package's tests, forwarding optional Cargo arguments after the package.
test-package package *args:
    SQLX_OFFLINE=true cargo test -p {{quote(package)}} {{args}}

# Lint one package, forwarding optional Cargo arguments before the lint floor.
check-package package *args:
    SQLX_OFFLINE=true cargo clippy -p {{quote(package)}} --all-targets {{args}} -- -D warnings

# Format and lint check (no live database required)
check:
    cargo +nightly fmt --all -- --check
    # The wire and config schema features ride the one workspace pass; a second
    # feature-flipped clippy would recompile the graph.
    SQLX_OFFLINE=true cargo clippy --workspace --all-targets --features tribal-wire/schema,tribal-config/schema -- -D warnings

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
        # Raise the connection ceiling clear of the stock 100 so a parallel
        # test run pointed at this container does not exhaust it. Mirrors
        # TEST_DB_MAX_CONNECTIONS in tribal-test-utils; keep the two in step.
        docker run -d --name tribal-postgres \
            -e POSTGRES_USER=tribal \
            -e POSTGRES_PASSWORD=tribal \
            -e POSTGRES_DB=tribal \
            -p 5432:5432 \
            pgvector/pgvector:0.8.2-pg17 \
            -c max_connections=500
        echo "tribal-postgres created and started"
    fi

# Stop and remove local Postgres
db-down:
    docker rm -f tribal-postgres

# Run database migrations against local Postgres
db-migrate:
    cargo run -p tribal -- setup

# Regenerate sqlx offline query metadata
sqlx-prepare:
    cargo sqlx prepare --workspace

# Full pre-push check (what CI will run)
pre-push: fmt check sqlx-prepare test

# Run the MCP server locally (stdio mode)
serve:
    cargo run -p tribal -- serve --transport stdio

# Run the MCP server with a specific project
serve-project project_id:
    cargo run -p tribal -- serve --transport stdio --project {{project_id}}
