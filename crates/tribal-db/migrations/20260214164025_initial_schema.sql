-- Initial schema migration for Tribal.
-- Creates the pgvector extension and all application tables in FK-dependency
-- order, followed by all indexes (B-tree, GIN, partial unique). Per-profile
-- HNSW indexes on the embedding tables are not created here: they are built at
-- runtime by first-boot provisioning and the reindex worker, because each needs
-- its profile UUID inlined and a non-transactional CREATE INDEX CONCURRENTLY.

--------------------------------------------------------------------------------
-- pgvector extension
--------------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS vector;

--------------------------------------------------------------------------------
-- principals
--------------------------------------------------------------------------------

CREATE TABLE principals (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    principal_key  TEXT NOT NULL UNIQUE,
    display_name   TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

--------------------------------------------------------------------------------
-- projects
--------------------------------------------------------------------------------

CREATE TABLE projects (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    git_remote      TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    default_branch  TEXT NOT NULL,
    project_type    TEXT,
    schema_version  INT NOT NULL,
    settings        JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

--------------------------------------------------------------------------------
-- prompt_versions: content-addressed prompt storage
--------------------------------------------------------------------------------

CREATE TABLE prompt_versions (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stage         TEXT NOT NULL CHECK (stage IN ('extraction', 'triage', 'relation')),
    content_hash  VARCHAR(64) NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    content       TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(stage, content_hash)
);

--------------------------------------------------------------------------------
-- tag_registry: global tag catalogue (TEXT PK)
--------------------------------------------------------------------------------

CREATE TABLE tag_registry (
    tag            TEXT PRIMARY KEY,
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

--------------------------------------------------------------------------------
-- knowledge_items: append-only
--------------------------------------------------------------------------------

CREATE TABLE knowledge_items (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id),
    principal_id    UUID NOT NULL REFERENCES principals(id),
    kind            TEXT NOT NULL CHECK (kind IN ('fact', 'heuristic', 'procedure', 'decision_record')),
    content         TEXT NOT NULL,
    tags            TEXT[] NOT NULL,
    confidence      TEXT NOT NULL CHECK (confidence IN ('verified', 'inferred', 'uncertain')),
    claim_context   JSONB,
    source_context  JSONB NOT NULL,
    episode_id      UUID,
    capture_commit  TEXT,
    capture_branch  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

--------------------------------------------------------------------------------
-- embedding_profiles: append-only, epoch-ordered activation log
--------------------------------------------------------------------------------
-- Each row is one activation of one embedding geometry, immutable once written.
-- The active profile is derived (highest-epoch 'complete'), never stored.
-- Identity is not unique; epoch is. Created before embeddings for the FK.

CREATE SEQUENCE embedding_profile_epoch_seq;

CREATE TABLE embedding_profiles (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    epoch                BIGINT NOT NULL UNIQUE
                             DEFAULT nextval('embedding_profile_epoch_seq'),
    provider_kind        TEXT NOT NULL,
    normalised_base_url  TEXT NOT NULL,
    model                TEXT NOT NULL,
    dimensions           INT NOT NULL CHECK (dimensions > 0 AND dimensions <= 4000),
    distance_metric      TEXT NOT NULL DEFAULT 'cosine'
                             CHECK (distance_metric IN ('cosine')),
    revision_token       TEXT NOT NULL DEFAULT '',
    probe_digest         TEXT,
    state                TEXT NOT NULL
                             CHECK (state IN ('building', 'complete', 'failed', 'superseded')),
    fingerprint_hash     TEXT NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at         TIMESTAMPTZ,

    CONSTRAINT chk_completed_state CHECK (
        (state = 'building' AND completed_at IS NULL)
        OR (state IN ('complete', 'failed', 'superseded') AND completed_at IS NOT NULL)
    )
);

--------------------------------------------------------------------------------
-- embeddings: append-only, write-once per (knowledge_item, profile)
--------------------------------------------------------------------------------
-- Bare halfvec (untyped, mixed-dimension across profiles). model is retained as
-- denormalised lineage, no longer an identity key. dimensions is recoverable via
-- vector_dims(embedding) and is fixed per profile, so it is not stored.

CREATE TABLE embeddings (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    knowledge_item_id     UUID NOT NULL REFERENCES knowledge_items(id),
    embedding_profile_id  UUID NOT NULL REFERENCES embedding_profiles(id),
    model                 TEXT NOT NULL,
    embedding             halfvec NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_embedding_item_profile UNIQUE (knowledge_item_id, embedding_profile_id),
    CONSTRAINT chk_nonzero_vector CHECK (l2_norm(embedding) > 0)
);

-- The bare halfvec column cannot itself bind a stored vector's width to its
-- profile's declared dimensions, so without a guard a wrong-width row (a
-- provider bug, a misconfigured dimension override) would be accepted and then
-- poison the profile's partial HNSW index build and every read cast to that
-- dimension, in a store with no row delete. This trigger makes the dimension
-- invariant a database fact on every embedding-writing path. The function is
-- generic over any table with (embedding, embedding_profile_id) columns, so
-- tag_embeddings reuses it.
CREATE FUNCTION assert_embedding_matches_profile_dimension() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    declared INT;
BEGIN
    SELECT dimensions INTO declared
        FROM embedding_profiles
        WHERE id = NEW.embedding_profile_id;
    IF vector_dims(NEW.embedding) <> declared THEN
        RAISE EXCEPTION
            'embedding dimension % does not match embedding_profile % declared dimension %',
            vector_dims(NEW.embedding), NEW.embedding_profile_id, declared
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_embeddings_dimension
    BEFORE INSERT ON embeddings
    FOR EACH ROW EXECUTE FUNCTION assert_embedding_matches_profile_dimension();

--------------------------------------------------------------------------------
-- item_external_references: append-only
--------------------------------------------------------------------------------

CREATE TABLE item_external_references (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    knowledge_item_id  UUID NOT NULL REFERENCES knowledge_items(id),
    kind               TEXT NOT NULL CHECK (kind IN ('file_path', 'url', 'concept', 'symbol')),
    value              TEXT NOT NULL,
    description        TEXT,
    project_id         UUID NOT NULL REFERENCES projects(id),
    commit             TEXT,
    branch             TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

--------------------------------------------------------------------------------
-- knowledge_item_relations: append-only
--------------------------------------------------------------------------------

CREATE TABLE knowledge_item_relations (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    relation_batch_id UUID NOT NULL,
    source_id         UUID NOT NULL REFERENCES knowledge_items(id),
    target_id         UUID NOT NULL REFERENCES knowledge_items(id),
    relation_type     TEXT NOT NULL CHECK (relation_type IN ('supports', 'contradicts', 'supersedes', 'derived_from')),
    principal_id      UUID NOT NULL REFERENCES principals(id),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

--------------------------------------------------------------------------------
-- item_observations: append-only
--------------------------------------------------------------------------------

CREATE TABLE item_observations (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    knowledge_item_id  UUID NOT NULL REFERENCES knowledge_items(id),
    principal_id       UUID NOT NULL REFERENCES principals(id),
    source_type        TEXT NOT NULL CHECK (source_type IN ('agent_mediated', 'manual_capture', 'derived', 'file_watch')),
    observed_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

--------------------------------------------------------------------------------
-- jobs
--------------------------------------------------------------------------------

CREATE TABLE jobs (
    id                            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id                    UUID NOT NULL REFERENCES projects(id),
    principal_id                  UUID NOT NULL REFERENCES principals(id),
    actor_id                      UUID REFERENCES principals(id),
    source_context                JSONB NOT NULL,
    correlation_id                UUID,
    status                        TEXT NOT NULL DEFAULT 'queued'
                                      CHECK (status IN ('queued', 'extracting', 'triaging', 'relating', 'completed', 'failed')),
    outcome                       TEXT CHECK (outcome IN ('success', 'partial', 'empty', 'failure')),
    batch_size                    INT,
    extraction_original_count     INT,
    committed_batch_id            UUID,
    error_message                 TEXT,
    extraction_prompt_version_id  UUID NOT NULL REFERENCES prompt_versions(id),
    triage_prompt_version_id      UUID NOT NULL REFERENCES prompt_versions(id),
    relation_prompt_version_id    UUID NOT NULL REFERENCES prompt_versions(id),
    trace_context                 TEXT CHECK (trace_context IS NULL OR char_length(trace_context) <= 128),
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at                  TIMESTAMPTZ,

    CONSTRAINT chk_terminal_outcome CHECK (
        (status IN ('completed', 'failed') AND outcome IS NOT NULL)
        OR (status NOT IN ('completed', 'failed') AND outcome IS NULL)
    ),
    CONSTRAINT chk_completed_outcome CHECK (
        status != 'completed' OR outcome IN ('success', 'partial', 'empty')
    ),
    CONSTRAINT chk_failed_outcome CHECK (
        status != 'failed' OR outcome = 'failure'
    ),
    CONSTRAINT chk_failed_has_error CHECK (
        status != 'failed' OR error_message IS NOT NULL
    ),
    CONSTRAINT chk_completed_no_error CHECK (
        status != 'completed' OR error_message IS NULL
    )
);

--------------------------------------------------------------------------------
-- tasks
--------------------------------------------------------------------------------

CREATE TABLE tasks (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id        UUID NOT NULL REFERENCES jobs(id),
    task_type     TEXT NOT NULL CHECK (task_type IN ('extraction', 'triage', 'relation')),
    status        TEXT NOT NULL DEFAULT 'queued'
                      CHECK (status IN ('queued', 'claimed', 'completed', 'dead_letter')),
    batch_index   INT,
    claim_token   UUID,
    available_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_by    TEXT,
    claimed_at    TIMESTAMPTZ,
    heartbeat_at  TIMESTAMPTZ,
    retry_count   INT NOT NULL DEFAULT 0,
    error_kind    TEXT CHECK (error_kind IN (
                      'provider_error', 'semaphore_timeout', 'parse_error',
                      'heartbeat_expired', 'startup_reclaim', 'ownership_lost', 'timeout'
                  )),
    error_message TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT claimed_requires_token     CHECK (status != 'claimed' OR claim_token IS NOT NULL),
    CONSTRAINT claimed_requires_owner     CHECK (status != 'claimed' OR claimed_by IS NOT NULL),
    CONSTRAINT claimed_requires_heartbeat CHECK (status != 'claimed' OR heartbeat_at IS NOT NULL),
    CONSTRAINT triage_requires_batch_index CHECK (task_type != 'triage' OR batch_index IS NOT NULL),
    CONSTRAINT non_triage_no_batch_index   CHECK (task_type = 'triage' OR batch_index IS NULL)
);

--------------------------------------------------------------------------------
-- triage_results: append-only
--------------------------------------------------------------------------------

CREATE TABLE triage_results (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id             UUID NOT NULL REFERENCES jobs(id),
    batch_index        INT NOT NULL,
    outcome_type       TEXT NOT NULL CHECK (outcome_type IN ('created', 'duplicate', 'failed')),
    knowledge_item_id  UUID REFERENCES knowledge_items(id),
    observation_id     UUID REFERENCES item_observations(id),
    matched_item_id    UUID REFERENCES knowledge_items(id),
    error_message      TEXT,
    retryable          BOOLEAN,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE(job_id, batch_index),

    CONSTRAINT created_has_item CHECK (
        outcome_type != 'created' OR knowledge_item_id IS NOT NULL
    ),
    CONSTRAINT duplicate_has_ids CHECK (
        outcome_type != 'duplicate' OR (observation_id IS NOT NULL AND matched_item_id IS NOT NULL)
    ),
    CONSTRAINT failed_no_item CHECK (
        outcome_type != 'failed' OR knowledge_item_id IS NULL
    )
);

--------------------------------------------------------------------------------
-- triage_similar_item_decisions: append-only
--------------------------------------------------------------------------------

CREATE TABLE triage_similar_item_decisions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id              UUID NOT NULL REFERENCES jobs(id),
    batch_index         INT NOT NULL,
    matched_item_id     UUID NOT NULL REFERENCES knowledge_items(id),
    similarity_score    REAL NOT NULL,
    suggested_relation  TEXT NOT NULL CHECK (suggested_relation IN ('supports', 'contradicts', 'unrelated')),
    justification_text  TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE(job_id, batch_index, matched_item_id)
);

--------------------------------------------------------------------------------
-- token_usage: append-only
--------------------------------------------------------------------------------

CREATE TABLE token_usage (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id             UUID REFERENCES jobs(id),
    task_id            UUID REFERENCES tasks(id),
    attempt            INT NOT NULL DEFAULT 0,
    stage              TEXT NOT NULL CHECK (stage IN ('extraction', 'triage', 'relation', 'embedding')),
    purpose            TEXT CHECK (purpose IN ('candidate', 'query')),
    provider           TEXT NOT NULL,
    model              TEXT NOT NULL,
    tokens_input       INT NOT NULL,
    tokens_output      INT NOT NULL,
    tokens_cache_read  INT NOT NULL DEFAULT 0,
    tokens_cache_write INT NOT NULL DEFAULT 0,
    tokens_total       INT NOT NULL,
    latency_ms         INT NOT NULL,
    prompt_version_id  UUID REFERENCES prompt_versions(id),
    trace_id           TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT token_total_check CHECK (tokens_total = tokens_input + tokens_output),
    CONSTRAINT cache_read_subset CHECK (tokens_cache_read <= tokens_input),
    CONSTRAINT purpose_stage_check CHECK (
        (stage = 'embedding' AND purpose IS NOT NULL) OR
        (stage != 'embedding' AND purpose IS NULL)
    )
);

--------------------------------------------------------------------------------
-- reindex_runs: operator-facing lifecycle of a model migration, retained as
-- run history. Single-flight per embeddings table via uq_reindex_run_live.
--------------------------------------------------------------------------------

CREATE TABLE reindex_runs (
    id                         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    target_profile_id          UUID NOT NULL REFERENCES embedding_profiles(id),
    epoch                      BIGINT NOT NULL,
    state                      TEXT NOT NULL
                                   CHECK (state IN ('queued', 'running', 'completed',
                                                    'superseded', 'aborted', 'failed')),
    initiated_by_principal_id  UUID NOT NULL REFERENCES principals(id),
    -- The *_enumerated columns are single backlog snapshots, so INT suffices.
    -- The *_embedded and *_quarantined columns are cumulative tallies summed
    -- across every catch-up pass, so they can exceed the corpus size and must be
    -- BIGINT to stay clear of the INT ceiling at scale. Editing the initial
    -- schema is the branch convention: databases are recreated, there are no
    -- users.
    items_enumerated           INT,
    items_embedded             BIGINT NOT NULL DEFAULT 0,
    tags_enumerated            INT,
    tags_embedded              BIGINT NOT NULL DEFAULT 0,
    items_quarantined          BIGINT NOT NULL DEFAULT 0,
    tags_quarantined           BIGINT NOT NULL DEFAULT 0,
    error_message              TEXT,
    trace_context              TEXT CHECK (trace_context IS NULL OR char_length(trace_context) <= 128),
    started_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at               TIMESTAMPTZ,

    CONSTRAINT chk_terminal_completed CHECK (
        (state IN ('completed', 'superseded', 'aborted', 'failed') AND completed_at IS NOT NULL)
        OR (state IN ('queued', 'running') AND completed_at IS NULL)
    )
);

--------------------------------------------------------------------------------
-- reindex_tasks: the lease protocol for a reindex run's per-batch work.
-- Derived from the ingestion task claim, with a distinct state set and
-- attempt/max_attempts columns. The driver claims and processes one task per
-- cycle synchronously; a crashed worker's task is re-done by the set-difference
-- catch-up, so there is no stale-lease reclaim.
--------------------------------------------------------------------------------

CREATE TABLE reindex_tasks (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    reindex_run_id    UUID NOT NULL REFERENCES reindex_runs(id),
    kind              TEXT NOT NULL CHECK (kind IN ('item', 'tag')),
    target_ref        TEXT NOT NULL,
    state             TEXT NOT NULL
                          CHECK (state IN ('pending', 'claimed', 'completed', 'dead_letter')),
    attempt           INT NOT NULL DEFAULT 0,
    max_attempts      INT NOT NULL DEFAULT 8,
    available_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    claim_token       UUID,
    claimed_by        TEXT,
    claimed_at        TIMESTAMPTZ,
    heartbeat_at      TIMESTAMPTZ,
    last_error        TEXT,
    last_error_class  TEXT CHECK (last_error_class IS NULL OR last_error_class IN
                          ('rate_limited', 'overloaded', 'transient', 'permanent')),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at      TIMESTAMPTZ,

    CONSTRAINT uq_reindex_task             UNIQUE (reindex_run_id, kind, target_ref),
    CONSTRAINT claimed_requires_token      CHECK (state != 'claimed' OR claim_token IS NOT NULL),
    CONSTRAINT claimed_requires_owner      CHECK (state != 'claimed' OR claimed_by IS NOT NULL),
    CONSTRAINT claimed_requires_heartbeat  CHECK (state != 'claimed' OR heartbeat_at IS NOT NULL)
);

--------------------------------------------------------------------------------
-- reindex_quarantine: durable permanent-failure relation, keyed by the target
-- profile so the reindex set-difference excludes it directly.
--------------------------------------------------------------------------------

CREATE TABLE reindex_quarantine (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    reindex_run_id     UUID NOT NULL REFERENCES reindex_runs(id),
    target_profile_id  UUID NOT NULL REFERENCES embedding_profiles(id),
    kind               TEXT NOT NULL CHECK (kind IN ('item', 'tag')),
    entity_ref         TEXT NOT NULL,
    error_class        TEXT NOT NULL,
    error_message      TEXT,
    quarantined_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_reindex_quarantine UNIQUE (target_profile_id, kind, entity_ref)
);

--------------------------------------------------------------------------------
-- auth_tokens: principal FK uses ON DELETE RESTRICT
--------------------------------------------------------------------------------

CREATE TABLE auth_tokens (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash    VARCHAR(64) NOT NULL UNIQUE CHECK (token_hash ~ '^[0-9a-f]{64}$'),
    principal_id  UUID NOT NULL REFERENCES principals(id) ON DELETE RESTRICT,
    expires_at    TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at    TIMESTAMPTZ
);

--------------------------------------------------------------------------------
-- retrieval_feedback: append-only
--------------------------------------------------------------------------------

CREATE TABLE retrieval_feedback (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trace_id             TEXT NOT NULL,
    query_text           TEXT NOT NULL,
    embedding_model      TEXT NOT NULL,
    embedding_profile_id UUID NOT NULL REFERENCES embedding_profiles(id),
    returned_item_ids    UUID[] NOT NULL,
    explored_anchor_ids  UUID[] NOT NULL,
    policy_version       TEXT,
    principal_id         UUID NOT NULL REFERENCES principals(id),
    rating               TEXT NOT NULL CHECK (rating IN ('positive', 'negative')),
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

--------------------------------------------------------------------------------
-- INDEXES
--------------------------------------------------------------------------------

-- knowledge_items
CREATE INDEX idx_knowledge_items_project    ON knowledge_items(project_id);
CREATE INDEX idx_knowledge_items_kind       ON knowledge_items(kind);
CREATE INDEX idx_knowledge_items_episode    ON knowledge_items(episode_id);
CREATE INDEX idx_knowledge_items_tags       ON knowledge_items USING gin(tags);
CREATE INDEX idx_knowledge_items_created    ON knowledge_items(created_at);

-- embedding_profiles
CREATE INDEX idx_embedding_profiles_state_epoch ON embedding_profiles(state, epoch DESC);
CREATE INDEX idx_embedding_profiles_fingerprint ON embedding_profiles(fingerprint_hash);

-- embeddings: the profile-only index keeps prune and the reindex set-difference
-- off a sequential scan; embedding_profile_id is the trailing column of
-- uq_embedding_item_profile, which therefore cannot serve a profile-scoped scan.
-- Per-profile partial HNSW indexes are built at runtime, not here.
CREATE INDEX idx_embeddings_profile ON embeddings(embedding_profile_id);

-- item_external_references
CREATE INDEX idx_external_refs_item  ON item_external_references(knowledge_item_id);
CREATE INDEX idx_external_refs_value ON item_external_references(value);

-- knowledge_item_relations
CREATE INDEX idx_relations_source              ON knowledge_item_relations(source_id);
CREATE INDEX idx_relations_target              ON knowledge_item_relations(target_id);
CREATE INDEX idx_relations_target_type         ON knowledge_item_relations(target_id, relation_type);
CREATE INDEX idx_relations_target_type_created ON knowledge_item_relations(target_id, relation_type, created_at DESC);
CREATE INDEX idx_relations_batch               ON knowledge_item_relations(relation_batch_id);

-- item_observations
CREATE INDEX idx_observations_item ON item_observations(knowledge_item_id);

-- jobs
CREATE INDEX idx_jobs_project_status ON jobs(project_id, status);
CREATE INDEX idx_jobs_created        ON jobs(created_at);

-- tasks
CREATE UNIQUE INDEX idx_tasks_unique_singleton ON tasks(job_id, task_type)
    WHERE task_type IN ('extraction', 'relation');
CREATE UNIQUE INDEX idx_tasks_unique_triage ON tasks(job_id, task_type, batch_index)
    WHERE task_type = 'triage';
CREATE INDEX idx_tasks_claimable ON tasks(available_at, created_at)
    WHERE status = 'queued';
CREATE INDEX idx_tasks_job_id    ON tasks(job_id);
CREATE INDEX idx_tasks_claimed_by ON tasks(claimed_by, status)
    WHERE claimed_by IS NOT NULL;
CREATE INDEX idx_tasks_error_kind ON tasks(error_kind)
    WHERE error_kind IS NOT NULL;

-- triage_results
CREATE INDEX idx_triage_results_job ON triage_results(job_id);

-- triage_similar_item_decisions
CREATE INDEX idx_triage_decisions_job       ON triage_similar_item_decisions(job_id);
CREATE INDEX idx_triage_decisions_job_batch ON triage_similar_item_decisions(job_id, batch_index);
CREATE INDEX idx_triage_decisions_item      ON triage_similar_item_decisions(matched_item_id);

-- token_usage
CREATE INDEX idx_token_usage_job           ON token_usage(job_id);
CREATE INDEX idx_token_usage_stage_created ON token_usage(stage, created_at);
CREATE INDEX idx_token_usage_model_created ON token_usage(model, created_at);

-- reindex_runs: single-flight backstop (one live run per embeddings table).
CREATE UNIQUE INDEX uq_reindex_run_live ON reindex_runs ((true))
    WHERE state IN ('queued', 'running');

-- reindex_tasks: pickup index over the claimable rows, scanned by the worker's
-- claim query (claimed rows are never pickup-scanned, so the partition is
-- pending-only).
CREATE INDEX idx_reindex_tasks_pickup ON reindex_tasks(available_at)
    WHERE state = 'pending';

-- retrieval_feedback
CREATE INDEX idx_feedback_trace     ON retrieval_feedback(trace_id);
CREATE INDEX idx_feedback_principal ON retrieval_feedback(principal_id, created_at);
CREATE INDEX idx_feedback_rating    ON retrieval_feedback(rating, created_at);
