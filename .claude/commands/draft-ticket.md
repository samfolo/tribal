---
description: Draft and create a GitHub issue for a Tribal implementation milestone
argument-hint: <issue-id e.g. 1.1, 4.4, 5.7>
allowed-tools: Bash(gh:*), Read, Grep, Glob
---

You are writing a ticket, not implementing one. Do not write any implementation code. If you are not already in plan mode, enter plan mode now. If you cannot enter plan mode, stop and ask me to do so before proceeding.

If $ARGUMENTS is empty or missing, stop and ask which milestone issue ID to draft (format: N.M, e.g. 2.5).

## 1. Orient

- Run `gh api repos/{owner}/{repo}/milestones --jq '.[] | "\(.number): \(.title) (\(.open_issues) open, \(.closed_issues) closed)"'` to list all GitHub milestones.
- Run `gh issue list --state all` to see what has already been created and completed.

## 2. Gather context

The issue ID is **$ARGUMENTS**. Find this issue in the Implementation Milestones document (@docs/internal/milestones.md). Read its row: title, blocked-by, crate, and refs.

Trace every design ref listed in the "Refs" column. Use the Read tool to read the referenced sections in the source-of-truth documents:

- **RFC** — `docs/internal/RFC.md`
- **Server** — `docs/internal/mcp_server.md`
- **Tool Surface** — `docs/internal/mcp_tool_surface.md`
- **Conventions** — `docs/internal/conventions.md`

Read enough surrounding context in each referenced section to fully understand the constraints, data structures, and invariants.

If the issue has blocked-by dependencies, cross-reference each dependency's title from the milestones table against the `gh issue list` output from step 1. Note any that are still open — mention this in the ticket.

## 3. Draft the ticket

Follow the Ticket Writing Specification (@docs/internal/ticket_writing_specification.md) exactly. Use the §2 template. Ensure every mandatory field from §5 is present. Observe §3 field guidance, especially:

- §3.5: Design constraints state the rule and cite the source.
- §3.6: Acceptance criteria are specific, verifiable, and testable.
- §3.7: Technical notes in prose only — no pseudo code.
- §3.8: File paths use `@`-prefixed project-root-relative notation.
- §4.1: Title is an imperative verb phrase.
- §4.2: Select the correct label (feature, fix, refactor, chore, docs).

## 4. Present for review

Show me the full draft. Do not create the GitHub issue yet. Wait for my sign-off or feedback.

## 5. Create the issue

Once I approve, create the GitHub issue:

- Assign the correct label per §4.2.
- Assign the correct GitHub milestone. The milestone issue ID prefix (the number before the dot in $ARGUMENTS) corresponds to the GitHub milestone number from step 1.
- Assign to me with `--assignee @me`.

**Critical formatting rule:** When creating the issue body with `gh issue create`, every paragraph must be a single unbroken line. Do not insert newlines to wrap text for terminal display. The `gh` CLI line-wrapping creates artificial line breaks that render badly in the GitHub UI. Write each paragraph and each list item on exactly one line, no matter how long.
