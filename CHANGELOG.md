# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.1] - 2026-06-28

### Changed

- Release tooling and CI hardening; no functional changes to the binary.

## [0.4.0] - 2026-06-20

### Changed

- **Forward-only schema addition (agentic triage):** prompt versions gain an executor-class axis (the launched one-shot pair, the agentic loop's pair, and the verifier child's pair are the same two roles under different classes), and the task error vocabulary gains a budget-exhausted kind. Both are additive: launched rows backfill to the one-shot class and the one-shot path is byte-for-byte unchanged. As with the rest of this release, upgrade the binary before the database under the single-server topology. ([#210](https://github.com/tribal-memory/tribal/issues/210))
- **Forward-only schema addition:** tasks gained a `blocked` status, held by a task that drives a suspended agent thread and is unclaimable until the suspension resolves. Binaries predating this release must not run against a database where blocked rows exist; with the single-server topology and automatic migrations, upgrading the binary first is the only supported order. ([#208](https://github.com/tribal-memory/tribal/issues/208))
- **Breaking (telemetry):** inference telemetry now follows the OpenTelemetry GenAI semantic conventions, replacing the launched custom vocabulary rather than running alongside it. Inference spans are exported as `chat {model}` and `embeddings {model}` with `gen_ai.*` attributes (operation and provider name, request model, temperature, input/output and cache token counts, embedding dimensions); the `tribal.llm.call`, `tribal.llm.probe`, `tribal.embedding.generate`, and `tribal.embedding.probe` span names and their `tribal.llm.*` attributes are gone, and provider probes now export a `tribal.provider.probe` span wrapping the inner GenAI span. The `tribal.provider.call_ms` histogram (milliseconds, `provider`/`model`/`stage` labels) is replaced by the conventions' `gen_ai.client.operation.duration` (seconds) and `gen_ai.client.token.usage` (`{token}`, classed by `gen_ai.token.type`), both carrying `tribal.stage` and, for embeddings, `tribal.embedding.purpose`. Stage spans keep their names but their stage and prompt-version attributes move to `tribal.stage`, `tribal.prompt.system_version_id`, and `tribal.prompt.user_version_id`. Dashboards or trace queries keyed on the old names need a one-time migration. ([#206](https://github.com/tribal-memory/tribal/issues/206))
- Every completion and embedding call now routes through one internal inference gateway owning credentials, concurrency permits, provider caching, and accounting, and the provider layer gained native streaming support behind it; pipeline behaviour and outputs are unchanged. ([#206](https://github.com/tribal-memory/tribal/issues/206))
- The agentic loop's default turn cap rose from 8 to 25. It is a runaway guard, not a thinking budget; the per-thread token cap remains the economic limit, and the loop prompts carry the budget guidance the model paces against. ([#223](https://github.com/tribal-memory/tribal/pull/223))

### Added

- **Agentic relation and extraction, completing the per-stage executor surface.** Setting `agents.relation.executor` or `agents.extraction.executor` to `loop` runs that stage as a turn loop, alongside the triage loop. The relation loop investigates across the project fence (cross-project semantic search, item and neighbourhood reads) and submits its edges by item id through `submit_result`, with deterministic validators that bounce a self-edge, a duplicate edge, or an endpoint the stage never offered as citable. The extraction loop is degenerate (it submits the candidates it extracts in a single turn) and carries the deterministic submission validators and the seam for a deferred source-grounding verifier. Every stage stays `one_shot` by default and byte-for-byte unchanged. Under the loop the relation stage verifies each submitted edge with a fresh-context reviewer child, on by default, checking it against an unsupported, wrong-direction, wrong-type, or spurious rubric with endpoints resolved across the project fence; the extraction verifier remains inert and is flagged as such at startup. ([#214](https://github.com/tribal-memory/tribal/issues/214), [#223](https://github.com/tribal-memory/tribal/pull/223))
- **Opt-in agentic triage.** Setting `agents.triage.executor` to `loop` runs the triage stage as an in-process turn loop rather than a single call. The stage gains a small set of read tools (project-scoped semantic search, item and neighbourhood reads, the tag registry, sibling-thread and job context) and a `submit_result` tool, so it can investigate before classifying a candidate as novel or duplicate. A submission clears deterministic validators (every referenced id must appear in what the model was shown; the duplicate target must still exist and be unsuperseded) and then, unless `agents.triage.verifier` is set false, a fresh-context verifier child that either accepts the submission or returns a critique the loop acts on, bounded by a verify-round budget. Per-thread turn, token, and wall-clock budgets bound execution, each overridable under `agents.triage`; the notes a submission curates for downstream reach the relation stage. Triage stays one-shot by default and byte-for-byte unchanged; the loop needs a tool-capable model and surfaces a provider that cannot honour tools as a legible stage error rather than degrading silently. ([#210](https://github.com/tribal-memory/tribal/issues/210))
- Every pipeline stage execution now runs as a durable agent thread: an append-only record log in Postgres carrying the rendered prompt as sent and the model response with its usage, committed in the same transactions as the work itself. Configuration, the MCP schemas, and `tribal_job_status` semantics are unchanged; durability and recording are additions underneath. Inference spend gains thread and record attribution in the `token_usage` ledger. ([#208](https://github.com/tribal-memory/tribal/issues/208))
- `tribal threads prune`, the sanctioned way to reclaim storage from the durable thread tables. It deletes terminal threads and their record logs older than `--older-than-days`, refusing any candidate whose subtree still holds a live thread; `--stage` narrows the pass to one pipeline stage, `--cascade` extends it to the terminal descendants of accepted candidates, and `--dry-run` reports what a pass would collect without deleting anything. ([#208](https://github.com/tribal-memory/tribal/issues/208))
- **Data reset (system fingerprints):** the `system_fingerprints` table is recreated around content-addressed stage binding versions — the composite is now the build version, the three stage binding hashes, the embedding identity with its dimensions, and the job-level pipeline parameters. Historic fingerprint rows are dropped (they cannot name binding versions that did not exist when they were written), so identical configuration produces a different fingerprint hash across this upgrade, and jobs or feedback recorded earlier keep hash strings no fingerprint row dereferences. Evaluation comparisons spanning this release must treat the fingerprint as a new lineage. ([#208](https://github.com/tribal-memory/tribal/issues/208))
- Three narrow behavioural deltas ride the thread runtime, all in failure or restart windows: a stage retried after a server restart now runs with the sampling parameters its thread recorded at first claim rather than the job's enqueue-time values; a task-level failure can no longer move an already-failed or completed job back to a live status (terminal job states are final under races); and task dead-letters arising from reclaim or from a cancelled thread now count in the dead-letter and job-failure metrics and fail the job in the same transaction, where previously reclaim dead-letters were uncounted and healed on the next sweep cycle. Steady-state pipeline output is byte-identical. ([#208](https://github.com/tribal-memory/tribal/issues/208))
- The `token_usage` ledger now records every billable call, closing accidental gaps: discover query embeddings, provider probes (boot, `tribal check`, and reindex target resolution, attributed under a new `probe` stage and embedding purpose), and calls whose pipeline attempt later failed are all ledgered; previously these spent tokens invisibly. Rows written outside a pipeline job, such as reindex and backfill embeds, now carry the live trace identity rather than none, so their spend joins to traces. ([#206](https://github.com/tribal-memory/tribal/issues/206))
- Success-path observability for the agentic runtime. Each thread-advancing execution opens an `invoke_agent` span nested under its stage span, with a per-tool `execute_tool` span and the inference spans beneath it, and the lifecycle emits events for thread resume, suspension by reason, child launch and terminal, deferred death, budget admission, and loop outcome, plus a per-cycle availability-sweep span carrying its convergence counts. The same transitions are aggregated as OpenTelemetry counters: suspensions by reason, budget admissions by decision, child launches and terminals, resumes, conflict retries, and sweep actions by kind. Spans, events, and counters carry identifiers and metadata only, never claim content, candidate text, justifications, or rendered prompts. ([#221](https://github.com/tribal-memory/tribal/issues/221), [#225](https://github.com/tribal-memory/tribal/pull/225))

## [0.3.1] - 2026-06-07

### Fixed

- Ollama structured-output ingestion. The Ollama JSON Schema dialect now shares the grammar subset with Anthropic, rewriting the single-element `allOf`-with-`$ref` and `oneOf` enum forms `schemars` emits into shapes llama.cpp's grammar builder compiles. The extraction, triage, and relation stages no longer dead-letter with a parse error against local models. ([#195](https://github.com/tribal-memory/tribal/issues/195))
- Pipeline responses wrapped in a Markdown code fence now parse. The extraction, triage, and relation stages deserialise strictly first and retry once with the fence stripped only when that fails, so a provider that ignores the structured-output schema and fences its JSON no longer dead-letters, while a valid payload, including one whose strings contain backticks, is never altered. ([#195](https://github.com/tribal-memory/tribal/issues/195))
- Strict-harness interop for the MCP tools. `tribal_discover` now treats an empty pagination cursor as the first page rather than rejecting it, and tool error results no longer carry structured content, so a harness that validates structured content against a tool's success output schema surfaces the real error message instead of a generic schema mismatch. ([#200](https://github.com/tribal-memory/tribal/pull/200))

## [0.3.0] - 2026-06-01

This release makes the embedding geometry configurable and adds zero-downtime reindexing of the embedding space. It is a breaking release: the initial database schema is revised for the embedding-profile model and the embedding configuration keys have moved, so there is no in-place upgrade from 0.2.x. Provision a fresh database and update the configuration (see Changed).

### Added

- Configurable embedding geometry. The embedding model, output dimension, and provider endpoint are config-driven rather than fixed at 768 dimensions. Each activation is recorded in an append-only log of embedding profiles; the active profile is derived from that log and is the live identity for every read and write. Embeddings are stored as `halfvec`, and a database trigger rejects any vector whose dimension does not match its profile.
- `tribal reindex`, a zero-downtime migration of the embedding space to a new model, dimension, or endpoint. The run is a background, single-flight, crash-safe catch-up that embeds the corpus into a new profile while reads and writes continue against the active one, then cuts over atomically; an unchanged target is a no-op. The CLI exposes `run` (`--provider` and `--model`, optional `--dimensions` and `--base-url`, with `--dry-run` to estimate the item and tag counts before spending), `cancel`, and `prune` (supersede the non-active and failed profiles and reclaim their storage). `tribal check` now reports the active embedding profile.
- The same three reindex operations as operator MCP tools (`tribal_reindex`, `tribal_reindex_cancel`, `tribal_reindex_prune`), so an authorised agent can drive a migration without shell access.
- A narrow `tribal.embedding:execute` OAuth scope that gates the reindex tools, and a repeatable `--scope` flag on `tribal token create` to mint scoped tokens. Read and write scopes plus `tribal.embedding:execute` are mintable; root and uncatalogued execute scopes are refused. The local stdio principal is granted the scope automatically, so bootstrap and the `tribal token create` defaults are unchanged (full read and write).

### Changed

- The embedding configuration has moved. The flat `[embedding]` section is replaced by `init.embedding`, a genesis seed applied only when a corpus is first created (once the corpus exists the active profile is the live identity, so later edits to `init` are inert and `tribal check` reports any divergence as informational state), and by a `credentials` catalogue that binds a named `(provider, base_url)` endpoint to an API key so a migrated corpus keeps its key reachable. Environment overrides are renamed to match: `TRIBAL_EMBEDDING__*` becomes `TRIBAL_INIT__EMBEDDING__*`, and a catalogue key is set with `TRIBAL_CREDENTIALS__<NAME>__API_KEY`. The Docker Compose and `.env.example` samples are updated accordingly.
- `tribal_discover` results now carry `embedding_profile_id`, the active profile that produced them, and `tribal_feedback` accepts it so the local retrieval-feedback log records the producing profile. A discover pagination cursor is bound to the embedding profile that issued it.
- The worker pipeline prompts are reframed around tacit knowledge (the reasoning, the rejected alternatives, and the bounding constraints behind a decision) rather than a generic knowledge base, and the model-facing vocabulary is unified on "claim". The few-shot examples and the structured-output guards are unchanged.
- The Docker Compose Postgres image is pinned to `pgvector/pgvector:0.8.2-pg17`, the minimum that provides the `halfvec` operations the embedding store now relies on.

## [0.2.5] - 2026-05-30

### Added

- OAuth 2.1 authentication for the HTTP and SSE transports, in a new `tribal-auth` crate. Tribal now runs as an OAuth authorisation server: Dynamic Client Registration (RFC 7591), PKCE, an authorisation-code flow with an explicit consent step, the RFC 8414 and RFC 9728 discovery metadata endpoints, and audience-bound bearer tokens. An OAuth-capable harness registers and authenticates itself on first connect, so the loopback wire-up carries no token to copy.

### Changed

- `tribal bootstrap` and `tribal mcp-config` choose the wire-up shape from the deployment's onboarding mode rather than always emitting a token. A loopback deployment with dynamic registration enabled advertises a URL-only OAuth snippet with nothing to copy; every other surface (reachable beyond loopback, or with registration disabled) embeds the persisted static token. Pass `--static-token` to force that token for a harness that authenticates with a bearer header only.
- `tribal check`'s token check follows the same onboarding mode: it skips on a URL-only surface, where clients authenticate over OAuth, and fails when a surface that depends on a static token has none.
- A missing bearer token on a network request now logs at DEBUG rather than WARN, so a steady-state healthcheck cycle no longer emits misleading authentication warnings.

### Security

- The unauthenticated Dynamic Client Registration endpoint is refused whenever the OAuth surface is reachable beyond loopback. With no explicit advertised URL, a wildcard bind (`0.0.0.0` or `[::]`) is treated as routable and fails closed; a loopback `server.public_mcp_url` is the trusted-exposure override for the container host-port-mapping shape. `server.public_mcp_url` is validated at load as an `http(s)` endpoint with a host and no fragment, and the same check guards the non-validating `tribal mcp-config` renderer.

## [0.2.4] - 2026-05-28

### Fixed

- Pipeline response schemas are now sent in each provider's accepted JSON Schema dialect with hard schema enforcement. OpenAI requests carry the strict subset (closed all-required objects, nullable optionals, internally-tagged enums emitted as `anyOf` of closed branches) with `strict: true`; Anthropic requests rewrite `oneOf` to `anyOf` and close every object so its grammar enforcement applies. The previous advisory mode allowed weaker cloud models, including the recommended `gpt-5.4-mini`, to return malformed shapes that dead-lettered ingest jobs after exhausting retries. Structured output is now guaranteed by the provider rather than hoped for.

### Changed

- Pipeline response-schema field descriptions are tightened across the extraction, triage, and relation stages to give the model a clearer instruction surface.
- MCP tool descriptions are rewritten to remove implementation jargon, presenting each tool through its user-facing contract rather than its internal mechanism.
- The triage stage's relation-direction explanation in the prompt is aligned with the legend, with explicit source/target binding, so the model classifies similar-item relations more consistently.

## [0.2.3] - 2026-05-27

### Fixed

- Triage now references similar items by position index rather than by their identifier, resolving them server-side. This removes a failure mode in which weaker models that could not reproduce a knowledge-item identifier failed triage and exhausted retries. An unresolvable reference is handled gracefully: an out-of-range duplicate match is treated as a novel item rather than failing the job.

## [0.2.2] - 2026-05-26

### Added

- Support for reasoning and adaptive-sampling models. Tribal resolves, per provider and model, which request fields a target accepts and shapes each request to match: OpenAI reasoning models (the o-series and the GPT-5 reasoning line) send the output cap as `max_completion_tokens` and have caller sampling parameters dropped, while Anthropic adaptive models (Opus 4.7) have `temperature` dropped but keep their required `max_tokens`. Both `tribal check --providers` and ingest now succeed for these models. An unrecognised model keeps sending every parameter, so ordinary models are unaffected.
- Per-stage `temperature` and `max_tokens` from configuration now reach the model. Previously they were silently dropped and only recorded in the system fingerprint.
- Configuration validation for sampling parameters: when set, `temperature` must be within `[0.0, 2.0]` and `max_tokens` at least 1. Model IDs may no longer be empty or whitespace-only.

### Changed

- Sampling parameters are now optional in configuration. An unset `temperature` or `max_tokens` means "use the provider default" rather than a built-in number, and the four built-in per-stage sampling defaults have been removed.
- The system fingerprint records the effective post-reconcile sampling parameters, so it reflects what is actually sent. Fingerprints change for any model whose request shape the capability layer adjusts.
- Rewrote the README around the shipped onboarding surface (`tribal bootstrap`, `tribal check`, `tribal mcp-config`, Docker Compose, and the skills), and added `CONTRIBUTING.md`.

### Fixed

- Reasoning models are no longer rejected by the provider readiness probe (`tribal check --providers`), which previously forced `temperature` and `max_tokens` values these models reject.

## [0.2.1] - 2026-05-25

### Added

- Docker Compose provider configuration through `.env`, letting the containerised path target a cloud provider (OpenAI, Anthropic) instead of only a local Ollama.

[Unreleased]: https://github.com/tribal-memory/tribal/compare/v0.4.1...HEAD
[0.4.1]: https://github.com/tribal-memory/tribal/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/tribal-memory/tribal/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/tribal-memory/tribal/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/tribal-memory/tribal/compare/v0.2.5...v0.3.0
[0.2.5]: https://github.com/tribal-memory/tribal/compare/v0.2.4...v0.2.5
[0.2.4]: https://github.com/tribal-memory/tribal/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/tribal-memory/tribal/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/tribal-memory/tribal/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/tribal-memory/tribal/releases/tag/v0.2.1
