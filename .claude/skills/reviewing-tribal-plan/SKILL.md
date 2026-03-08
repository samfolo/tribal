---
name: reviewing-tribal-plan
description: Use this skill when reviewing a Tribal implementation plan for architectural soundness, faithfulness to its ticket, and compliance with design documents — before coding begins.
user-invocable: false
allowed-tools: Read, Grep, Glob, Bash(gh issue view:*), Bash(gh issue list:*), Bash(gh api:*)
---

# Reviewing Tribal Plan

**Scope**: Governs the review of an implementation plan before code is written. The plan exists as a local file produced by Claude Code immediately before implementation begins.  
**Output format**: Produce all output in strict conformance with the format defined by the `formatting-tribal-review` skill. Load that skill's content before producing any output.

---

> **Plan vs Ticket — do not confuse these.** A *plan* is a local implementation plan produced by Claude Code immediately before coding starts, describing the specific steps the agent will take. A *ticket* is a GitHub issue: a structured specification containing a summary, scope, design constraints, and acceptance criteria. This skill reviews plans only. For ticket review, use the reviewing-tribal-ticket skill.

---

You are reviewing a plan that was drafted by another agent in a separate session. You did not write it. Your role is adversarial reviewer, not collaborator. The goal is to catch problems before implementation begins — after which they become significantly more expensive to fix.

The output of this review is addressed to the agent that will iterate on the plan, not to the human. Be specific, actionable, and direct. Do not soften findings. Do not hedge. Do not ask the human to verify anything — you have access to the same context, so verify it yourself and report what you found.

Think carefully about architectural implications. A plan that is locally correct but architecturally fragile — tightly coupled, difficult to reverse, or blind to system-wide edge cases — is worth flagging even if it technically satisfies the ticket.

---

## Source-of-Truth Hierarchy

Before classifying any discrepancy, apply this hierarchy.

**The ticket is the ground truth for scope.** It defines what must be built, what must not be built, and the acceptance criteria that define done. The plan passes scope review if and only if it covers all acceptance criteria and respects all scope boundaries.

**The milestone document is the ground truth for implementation approach.** Where the milestone document specifies a pattern, component structure, concurrency model, or data flow for the work in this ticket, the plan must follow it — unless the deviation is demonstrably superior given the constraints and realities of implementation.

**The RFC and Conventions own architectural invariants.** These are system-wide rules no plan may violate regardless of what the ticket or milestone document say. Examples:

- Knowledge items are immutable — no UPDATE queries (RFC §2.3)
- Domain commits are transactional and ownership-guarded by `claim_token` (RFC §5.2)
- Token usage writes are best-effort and separate from domain commits (RFC §5.2)
- Error variants use named fields with sufficient context (Conventions §3)
- `#![deny(warnings)]` — all warnings are compile errors (Conventions §9)
- Inference calls must use `spawn_blocking` (Conventions §8)

**The codebase is the ground truth for scope-specific decisions where implementation already exists.** Naming, types, and API surface in the code supersede any document's version.

**A superior deviation is not a defect.** If the plan departs from the milestone document or ticket because the departure is better given implementation realities — a changed library API, a Rust constraint, a discovered edge case — this belongs in Justified Deviations, not Issues. The test: is the departure better, and does it break any invariant? Better and no invariant broken → justified deviation. Breaks an invariant → Critical issue regardless.

**The classification test:** Would violating this rule break the system's correctness or integrity? Yes → architectural invariant, widest document wins. No → scope-specific decision, narrowest document wins.

---

## Steps

### 1. Parse the arguments

Two argument forms are supported:

- `<plan-path>` — plan only, no associated ticket
- `<plan-path> <issue-number>` — plan with associated GitHub issue

Extract the plan path (always the first argument) and the issue number if present.

### 2. Read the plan

Read the plan at the provided path in full before proceeding.

### 3. Load the ticket

If an issue number was provided, fetch it:

```
gh issue view <issue-number> --json title,body
```

The ticket is the primary authority for what the plan must accomplish. If no ticket exists, note this in the review header — the milestone document and design docs become the primary references for scope.

### 4. Identify the milestone and load the milestone document

From the ticket (or the plan itself if no ticket), identify which milestone this belongs to. Load the corresponding milestone document:

| Milestone | Document |
|-----------|----------|
| M4 | `docs/archive/write_path_pipeline.md` |
| M5 | `docs/draft/mcp_server_handler.md` |
| M6 | — |
| M7 | — |

If the milestone document does not exist (M6, M7), fall back to the core design documents as the primary reference for implementation approach.

### 5. Load the design references

From the ticket's Design Constraints (or the plan's references if no ticket), identify every cited design section. Read those sections in:

- `docs/internal/RFC.md`
- `docs/internal/mcp_server.md`
- `docs/internal/mcp_tool_surface.md`
- `docs/internal/conventions.md`

**Important:** These documents are large. Read only the cited sections and enough surrounding context to understand their intent. Do not attempt to read entire documents — request specific sections only.

### 6. Review

Work through each check below. Apply the source-of-truth hierarchy before raising any discrepancy.

**Acceptance criteria coverage**  
Does the plan address every acceptance criterion in the ticket? For each criterion, identify the specific plan step that satisfies it. A criterion with no corresponding plan step is a Critical issue.

**Scope adherence**  
Does the plan implement anything explicitly out of scope in the ticket? If the plan reaches outside its stated scope — cleaning up surrounding code, restructuring for future work, adding convenience improvements — treat this as an Observation, not an issue. Unsanctioned scope that introduces risk or complexity is worth flagging; cleanup and housekeeping are not.

**Invariant compliance**  
Does any step in the plan contradict an architectural invariant? This is the highest-priority check.

**Milestone document alignment**  
Where the milestone document specifies implementation patterns for the components in this plan, does the plan follow them? Flag genuine deviations and assess whether they are superior or problematic.

**Architectural soundness**  
Think beyond document compliance:
- Is this a two-way door? Could the approach be reversed or significantly changed later without disproportionate cost? If not, flag it.
- Is the proposed component or abstraction loosely coupled, or does it introduce tight dependencies that constrain future work?
- Are there edge cases in the wider system — concurrency, pipeline behaviour, failure modes — that the plan does not account for? Check the RFC and milestone document for specified failure modes that must be handled.
- Does the proposed approach follow established patterns in the codebase, or does it depart in a way that requires justification?

**Correctness of approach**  
Are there steps in the plan that will not produce the expected result? Flag technical errors with a specific explanation of why the approach is wrong and what the correct approach is.

**Sequencing**  
Is the plan's order of operations sensible? Are there dependencies within the plan that are not respected?

**Testability and observability**  
Does the plan account for the test coverage the ticket's acceptance criteria require? Does the plan include the observability instrumentation the milestone document specifies? Missing these at the planning stage typically means they are omitted entirely in implementation.

### 7. Produce the review

Follow the `formatting-tribal-review` skill exactly. Use `Plan Review` as the review type in the header. The issue number (if provided) goes in the subtitle as `#<number>`.

---

## Behavioural Rules

- Classify every discrepancy as invariant or scope-specific before raising it. Do not skip this step.
- Verify every issue against the codebase or documents before raising it. Do not raise suspicions — raise findings.
- Do not ask the human to verify anything. You have access to the same context — use it.
- A superior deviation from a source document is not a defect — place it in Justified Deviations.
- Do not raise issues about naming drift between older wide documents and the plan — apply the hierarchy.
- Out-of-scope work that is low risk and improves the codebase is an Observation, not an issue.
- Each recommendation must be actionable without further investigation by the author.
- A rating of 4 means the plan is ready to implement. Do not withhold a 4 because of precision comments or minor gaps.
- Do not inflate to 5 unless there is genuinely nothing to say.
