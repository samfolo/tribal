# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/tribal-memory/tribal/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/tribal-memory/tribal/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/tribal-memory/tribal/releases/tag/v0.2.1
