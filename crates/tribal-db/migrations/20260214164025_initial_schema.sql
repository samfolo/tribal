-- Initial schema migration for Tribal.
-- Creates the pgvector extension and all 16 tables in FK-dependency order,
-- followed by all indexes (B-tree, GIN, HNSW, partial unique).

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
    git_remote      TEXT NOT NULL,
    name            TEXT NOT NULL,
    default_branch  TEXT NOT NULL,
    project_type    TEXT,
    schema_version  INT NOT NULL,
    settings        JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

--------------------------------------------------------------------------------
-- prompt_versions — content-addressed prompt storage
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
-- tag_registry — global tag catalogue (TEXT PK)
--------------------------------------------------------------------------------

CREATE TABLE tag_registry (
    tag            TEXT PRIMARY KEY,
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

--------------------------------------------------------------------------------
-- knowledge_items — append-only
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
-- embeddings — append-only
--------------------------------------------------------------------------------

CREATE TABLE embeddings (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    knowledge_item_id  UUID NOT NULL REFERENCES knowledge_items(id),
    model              TEXT NOT NULL,
    dimensions         INT NOT NULL,
    embedding          vector(768) NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

--------------------------------------------------------------------------------
-- item_external_references — append-only
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
-- knowledge_item_relations — append-only
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
-- item_observations — append-only
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
-- triage_results — append-only
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
-- triage_similar_item_decisions — append-only
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
-- token_usage — append-only
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
-- auth_tokens — principal FK uses ON DELETE RESTRICT
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
-- retrieval_feedback — append-only
--------------------------------------------------------------------------------

CREATE TABLE retrieval_feedback (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trace_id            TEXT NOT NULL,
    query_text          TEXT NOT NULL,
    embedding_model     TEXT NOT NULL,
    returned_item_ids   UUID[] NOT NULL,
    explored_anchor_ids UUID[] NOT NULL,
    policy_version      TEXT,
    principal_id        UUID NOT NULL REFERENCES principals(id),
    rating              TEXT NOT NULL CHECK (rating IN ('positive', 'negative')),
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
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

-- embeddings
CREATE INDEX idx_embeddings_item_model ON embeddings(knowledge_item_id, model);
CREATE INDEX idx_embeddings_hnsw       ON embeddings USING hnsw(embedding vector_cosine_ops);

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

-- retrieval_feedback
CREATE INDEX idx_feedback_trace     ON retrieval_feedback(trace_id);
CREATE INDEX idx_feedback_principal ON retrieval_feedback(principal_id, created_at);
CREATE INDEX idx_feedback_rating    ON retrieval_feedback(rating, created_at);
