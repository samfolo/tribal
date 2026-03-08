---
description: Review a draft Tribal ticket for consistency with design documents
argument-hint: <path-to-draft e.g. /tmp/ticket-4.4.md>
allowed-tools: Read, Grep
---

You are reviewing a ticket that was drafted by another agent. You did not write it. Your job is to be a critical reviewer, not a collaborator.

If you are not already in plan mode, enter plan mode now. If you cannot enter plan mode, stop and ask me to do so before proceeding.

If $ARGUMENTS is empty or missing, stop and ask for the path to the draft ticket file.

## 1. Read the draft

Read the ticket at **$ARGUMENTS**.

## 2. Trace the design refs

Identify every design document section referenced in the ticket's Design Constraints. Use the Read tool to read those sections in the source-of-truth documents:

- **RFC** — `docs/internal/RFC.md`
- **Server** — `docs/internal/mcp_server.md`
- **Tool Surface** — `docs/internal/mcp_tool_surface.md`
- **Conventions** — `docs/internal/conventions.md`

Also read the corresponding issue row in `docs/internal/milestones.md` to verify the ticket covers the intended scope.

### Milestone-specific documents

These are documents specific to particular milestones. They serve as higher-fidelity sources of truth for specific concerns and should be consulted if the ticket falls under the milestone:

- **4.x Write Path Pipeline** — `docs/archive/write_path_pipeline.md`
- **5.x MCP Tool Surface** - `docs/draft/mcp_server_handler.md`

## 3. Review

Check the following and report your findings:

- **Structure:** Does the ticket follow the template in §2 of `docs/internal/ticket_writing_specification.md`? Are all mandatory fields from §5 present?
- **Consistency:** Does every design constraint accurately reflect what the referenced section actually says? Flag any misstatements, omissions, or subtle drift from the source of truth.
- **Completeness:** Are there invariants, error cases, or conventions in the referenced sections that the ticket should mention but doesn't?
- **Acceptance criteria:** Is each criterion specific and verifiable? Could an implementer read it and know unambiguously whether it's been met? Flag anything vague or implicit.
- **Scope boundaries:** Is the in-scope/out-of-scope split clean? Is anything in scope that should be a separate ticket, or anything out of scope that's actually a dependency?
- **Precision of language:** Flag any wording that an implementing agent could reasonably misinterpret.

## 4. Present findings

List your findings as concrete, actionable items. For each one, cite the specific design document section that supports your point. If the ticket is solid, say so — don't invent issues for the sake of having feedback.
