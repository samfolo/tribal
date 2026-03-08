---
name: reviewing-tribal-ticket
description: Use this skill when reviewing a Tribal ticket draft for correctness, completeness, and faithfulness to the design documents before it is published as a GitHub issue.
user-invocable: false
allowed-tools: Read, Grep, Glob, Bash(gh issue view:*), Bash(gh issue list:*), Bash(gh api:*)
---

# Reviewing Tribal Ticket

**Scope**: Governs the review of a Tribal ticket draft before it is published as a GitHub issue and handed to an implementing agent.  
**Output format**: Produce all output in strict conformance with the format defined in [review-output](../../../../docs/templates/review-output.md). Read that file before producing any output.

---

> **Ticket vs Plan — do not confuse these.** A *ticket* is a GitHub issue: a structured specification of a piece of work, written before implementation begins, containing a summary, scope, design constraints, and acceptance criteria. A *plan* is a local implementation plan produced by Claude Code immediately before coding starts, describing the specific steps the agent will take. This skill reviews tickets only. For plan review, use the reviewing-tribal-plan skill.

---

You are reviewing a ticket that was drafted by another agent. You did not write it. Your role is adversarial reviewer, not collaborator. The goal is to find real problems before an implementing agent acts on incorrect or incomplete instructions.

The output of this review is addressed to the agent that will iterate on the ticket, not to the human. Be specific, actionable, and direct. Do not soften findings. Do not hedge. Do not ask the human to verify anything — you have access to the same context, so verify it yourself and report what you found.

---

## Source-of-Truth Hierarchy

Before classifying any discrepancy, apply this hierarchy. Getting this wrong is the most common source of review noise.

**Architectural invariants are owned by the widest document.** These are system-wide rules that hold regardless of scope. If violated, the design breaks. They cannot be overruled by any narrower document. Examples for this project:

- Knowledge items are immutable — no UPDATE queries (RFC §2.3)
- Domain commits are transactional and ownership-guarded by `claim_token` (RFC §5.2)
- Token usage writes are best-effort and separate from domain commits (RFC §5.2)
- Error variants use named fields with sufficient context (Conventions §3)
- `#![deny(warnings)]` — all warnings are compile errors (Conventions §9)
- Inference calls must use `spawn_blocking` (Conventions §8)

**Scope-specific decisions are owned by the narrowest document.** Naming, field types, API surface, struct layout, method signatures — these are deliberately refined as scope narrows. The ticket represents the most considered choice for this specific piece of work. A field renamed between the RFC and the ticket is not a discrepancy — it is a refinement. Do not flag it.

**The codebase is the ground truth for scope-specific decisions where implementation already exists.** A name or type in the code supersedes any document's version of that name or type.

**The classification test:** Would violating this rule break the system's correctness or integrity? Yes → architectural invariant, widest document wins. No → scope-specific decision, narrowest document wins.

---

## Steps

### 1. Read the ticket

Read the draft at the path provided. Do not begin reviewing until you have read the full document.

### 2. Identify the milestone and load the milestone document

The milestone can be inferred from the issue ID prefix in the ticket (e.g. `4.4`, `5.2`). Load the corresponding milestone document:

| Milestone | Document |
|-----------|----------|
| M4 | `docs/internal/m4_write_path_pipeline.md` |
| M5 | `docs/internal/m5_mcp_server.md` |
| M6 | `docs/internal/m6_runtime.md` |
| M7 | `docs/internal/m7_auth.md` |

If the milestone document does not exist, note this in the review and fall back to the core design documents in step 4 as the primary reference for implementation approach.

### 3. Check for a branch

Run `gh branch list` to check whether a feature branch exists for this ticket. Record it for the review header — use `*[No branch]*` if none exists.

### 4. Trace the design references

Identify every section reference in the ticket's Design Constraints. Read those sections in:

- `docs/internal/rfc.md`
- `docs/internal/mcp_server.md`
- `docs/internal/mcp_tool_surface.md`
- `docs/internal/conventions.md`
- The milestone document from step 2 (if it exists)

**Important:** These documents are large. Read only the cited sections and enough surrounding context to understand their intent. Do not attempt to read entire documents — request specific sections only.

### 5. Review

Work through each check below. For every discrepancy encountered, apply the source-of-truth hierarchy before deciding whether to raise it. Do not raise scope-specific differences between older wide documents and the ticket.

**Invariant compliance**  
Does the ticket contradict any architectural invariant? This is the highest-priority check. A single invariant violation is a Critical issue regardless of how well-formed the rest of the ticket is.

**Milestone alignment**  
Where the milestone document specifies implementation approach, component structure, or data flow for this work, does the ticket accurately reflect it? Raise genuine misalignments — not superficial ones.

**Completeness**  
Are there invariants, error cases, or conventions in the referenced sections that genuinely apply to this ticket but are absent from the Design Constraints? Only raise omissions that would cause an implementing agent to produce incorrect behaviour.

**Acceptance criteria**  
Is each criterion specific and verifiable? Can an implementing agent know unambiguously whether it has been met? Flag vague or untestable criteria. Flag missing coverage of error paths that the design documents specify as required.

**Scope definition**  
Is the in-scope / out-of-scope split clear? Is anything in scope that should be a separate ticket? Is anything out of scope that is actually a dependency blocking this ticket?

**Precision**  
Flag any wording an implementing agent could reasonably misinterpret. Be specific about which phrase and why.

**Ticket Writing Specification compliance**  
Check against `@docs/internal/ticket_writing_specification.md`:
- Imperative title (§4.1)
- All mandatory fields present (§5)
- Design constraints cite their sources (§3.5)
- No pseudo code in Technical Notes (§3.7)
- File paths use `@`-prefixed notation (§3.8)
- Agent preamble present (§3.1)

### 6. Produce the review

Follow [review-output](../../../../docs/templates/review-output.md) exactly. Use `Ticket Review` as the review type in the header.

---

## Behavioural Rules

- Classify every discrepancy as invariant or scope-specific before raising it. Do not skip this step.
- Verify every issue against the codebase or documents before raising it. Do not raise suspicions — raise findings.
- Do not ask the human to verify anything. You have access to the same context — use it.
- Do not raise issues about whitespace, capitalisation, or stylistic preferences with no impact on correctness.
- Do not flag ticket size based on acceptance criteria count — granularity is a drafting concern, not a review concern.
- Each recommendation must be actionable without further investigation by the author.
- A rating of 4 means the ticket is ready to proceed. Do not withhold a 4 because of precision comments or minor gaps.
- Do not inflate to 5 unless there is genuinely nothing to say.
