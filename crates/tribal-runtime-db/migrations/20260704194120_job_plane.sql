--------------------------------------------------------------------------------
-- The managed job plane
--------------------------------------------------------------------------------
-- run_job is the per-tenant work queue a managed worker claims off; tenant_slot
-- is the single-row admission anchor that turns a concurrency cap into a hard
-- stop. Platform-operated, account-scoped, never joined to the ledger: a row's
-- account_id is an opaque platform reference with no enforceable foreign key
-- across the database boundary.

CREATE TABLE run_job (
    id                TEXT        PRIMARY KEY,
    account_id        TEXT        NOT NULL,
    kind              TEXT        NOT NULL CHECK (kind IN ('consolidate', 'cron', 'probe')),
    payload           JSONB       NOT NULL,
    idempotency_key   TEXT        NOT NULL,
    priority          SMALLINT    NOT NULL DEFAULT 0,
    state             TEXT        NOT NULL DEFAULT 'queued'
                          CHECK (state IN ('queued', 'running', 'suspended', 'settling', 'done', 'cancelled')),
    cancel_requested  BOOLEAN     NOT NULL DEFAULT FALSE,
    claim_token       UUID,
    lease_expires_at  TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- A re-enqueue with a key already seen returns the existing job.
    UNIQUE (account_id, idempotency_key),
    -- A running job is held under a claim token and a live lease; both are
    -- cleared on every exit from running.
    CONSTRAINT run_job_running_requires_token CHECK (state <> 'running' OR claim_token IS NOT NULL),
    CONSTRAINT run_job_running_requires_lease CHECK (state <> 'running' OR lease_expires_at IS NOT NULL)
);

-- Cross-tenant claim order is age only.
CREATE INDEX run_job_claimable ON run_job (created_at) WHERE state = 'queued';
-- Within a tenant, priority then age orders the claim.
CREATE INDEX run_job_tenant_claimable ON run_job (account_id, priority DESC, created_at) WHERE state = 'queued';

-- One row per tenant: the single-row compare-and-swap admission serialises on.
-- cap is the plan's concurrency ceiling, refreshed by cap sync. The running >= 0
-- guard and the admission's running < cap predicate together make both
-- over-admission and a negative count unrepresentable.
CREATE TABLE tenant_slot (
    account_id  TEXT    PRIMARY KEY,
    running     INTEGER NOT NULL DEFAULT 0 CHECK (running >= 0),
    cap         INTEGER NOT NULL           CHECK (cap >= 0)
);
