---
name: formatting-tribal-review
description: Defines the canonical output format for all Tribal review commands. Loaded automatically by review skills — not intended for direct invocation.
user-invocable: false
---

# Tribal: Review Output Format

**Scope**: Canonical output format for all Tribal review commands.
**Usage**: Every review produced by a Tribal review skill must conform to this structure exactly. Omit any section that has no content — do not write section headers with "N/A" or "None" beneath them.

---

## Document Reference Conventions

When citing design documents, always use the short-form notation below followed by a section reference. Where a file path is relevant, include it using `@`-prefixed project-root-relative notation.

| Short name | Document |
|------------|----------|
| RFC | `docs/internal/RFC.md` |
| Server | `docs/internal/mcp_server.md` |
| Tools | `docs/internal/mcp_tool_surface.md` |
| Conventions | `docs/internal/conventions.md` |
| Ticket Spec | `docs/internal/ticket_writing_specification.md` |
| Milestones | `docs/internal/milestones.md` |
| M4 | `docs/archive/write_path_pipeline.md` |
| M5 | `docs/draft/mcp_server_handler.md` |
| M6 | — |
| M7 | — |

Examples of correct reference notation:
- `RFC §5.2`
- `Server §3.10`
- `Conventions §6`
- `M4 §4.2`

---

## Section Order

1. Header
2. Verdict & Rating
3. Verified Correct *(omit if nothing to confirm)*
4. Observations *(omit if none)*
5. Justified Deviations *(omit if none)*
6. Issues *(omit if none)*
7. Open Questions *(omit if none)*
8. Issue Summary *(omit if no issues)*
9. Closing Line

---

## Divider Rhythm

A `---` divider appears before every section heading and before every item within a section. There must be a blank line on both sides of the divider — one blank line above it (separating it from the previous content) and one blank line below it (separating it from the heading or item that follows).

```
previous content

---

### Section Heading

item content

---

**Next Item**

item content
```

---

## Format Specification

---
### 1. Header

The heading states the review type and title. The subtitle line carries the issue ID (or GitHub issue number) and branch name. Use placeholder forms when fields are absent — do not leave fields blank.

**Ticket Review:**
```markdown
## **Ticket Review — [title]**
[issue-id] · [branch-name]
```

Examples:
```markdown
## **Ticket Review — Add triage stage semantic similarity matching**
5.2 · feature/52-add-triage-similarity

## **Ticket Review — Add triage stage semantic similarity matching**
5.2 · *[No branch]*
```

**Plan Review:**
```markdown
## **Plan Review — [title]**
[#issue-number or *[No ticket]*] · [branch-name]
```

Examples:
```markdown
## **Plan Review — Add triage stage semantic similarity matching**
#42 · feature/52-add-triage-similarity

## **Plan Review — Add triage stage semantic similarity matching**
*[No ticket]* · feature/52-add-triage-similarity
```

**Code Review:**
```markdown
## **Code Review — [title]**
[#issue-number or *[No ticket]*] · [branch-name]
```

Example:
```markdown
## **Code Review — Add triage stage semantic similarity matching**
#42 · feature/52-add-triage-similarity
```

**Branch name:** For ticket reviews, check with `git branch --list` whether a local branch exists before defaulting to `*[No branch]*`. For plan and code reviews, include the branch name when it is known; if it cannot be determined, use `*[No branch]*`.

---
### 2. Verdict & Rating

```markdown
**Verdict: [N]/5 — [label]**
```

| N | Label |
|---|-------|
| 1 | Not implementable |
| 2 | Poor |
| 3 | Substantive issues present |
| 4 | Ready to proceed |
| 5 | Exemplary |

Followed by a verdict paragraph (three sentences maximum):
- **Sentence 1:** Clear quality signal. Do not hedge.
- **Sentence 2:** Specific praise naming what is done well — not generic, name the thing.
- **Sentence 3:** Forward-looking summary of what follows.

Followed by a rationale line:

| N | Rationale |
|---|-----------|
| 1 | An implementing agent could not begin work from this. |
| 2 | An implementing agent would require extensive additional guidance to produce correct output. |
| 3 | The issues below must be resolved before this work proceeds. |
| 4 | Any remaining findings are refinements only — proceed once findings are consciously accepted or addressed. |
| 5 | No notes — cite this as a reference. |

**Example — strong ticket:**

> **Verdict: 4/5 — Ready to proceed**
>
> This is a strong ticket. The ownership-guard invariant is precisely stated against RFC §5.7, and the distinction between best-effort token usage writes and transactional domain commits is correctly captured in two separate acceptance criteria. The findings below are refinements, not structural problems.
>
> Any remaining findings are refinements only — proceed once findings are consciously accepted or addressed.

**Example — blocking issue:**

> **Verdict: 3/5 — Substantive issues present**
>
> This plan is directionally correct but has a blocking issue. The `commit_domain_effects` step does not include the `claim_token` guard, which means ownership loss cannot be detected at commit time. There is one critical issue that must be resolved before implementation begins, alongside two precision comments.
>
> The issues below must be resolved before this work proceeds.

**A rating of 4 or above means this work is ready to proceed.** Do not withhold a 4 because of precision comments or minor completeness gaps. Do not inflate to 5 unless there is genuinely nothing to say.

---
### 3. Verified Correct

A bulleted list confirming what the review validated as accurate. Each bullet names a specific constraint or decision, the section it was checked against, and the outcome. This is a confirmation sweep, not a praise section.

Example:

> - The four-way project ID resolution logic is correctly stated against RFC §4.1
> - The `spawn_blocking` requirement for inference calls is captured and cited against Server §1.2
> - The acceptance criteria for the happy path and the `OwnershipLost` error path both map to verifiable test assertions

---
### 4. Observations

Anything the reviewer noticed during the review that does not fit neatly into Verified Correct, Justified Deviations, or Issues. Typically: a passed item that required non-obvious reasoning to confirm, an ambiguity that resolved on closer inspection, or a pattern worth noting without raising as a formal issue.

Each observation has a short title and a single paragraph.

```markdown
---

### Observations

---

**[Short title]**

[One paragraph. What was noticed, how it was interpreted, and why it was passed
or why it is simply worth noting.]
```

---
### 5. Justified Deviations

Deviations from a source document that are warranted — not errors, but deliberate departures caused by new information: a changed library API, a Rust constraint, an implementation discovery, schema drift from the spec.

Each justified deviation must state:
- The specific document and section being deviated from
- The nature of the deviation
- The reason it is necessary
- A positive assertion that no architectural invariant is broken

```markdown
---

### Justified Deviations

---

**[Short title of deviation]**

[Description of what differs and which document it deviates from, with section
reference. Include `@`-prefixed file path if the ground truth is in the codebase.]

**Reason:** [Why the deviation is necessary.]

**Invariant check:** [Positive assertion that no architectural invariant is broken.]
```

---
### 6. Issues

Each issue is a numbered item starting from 1.

```markdown
---

### Issues

---

**[N]. [Severity] — [Short title]**

[Context: what the issue is, why it matters, which document or criterion it relates
to. Cite using short-form notation (e.g. `RFC §5.2`). Include `@`-prefixed file paths
where the ground truth is in the codebase. Use backticks for field names, type names,
trait names, and symbols. One to three sentences.]

**Recommendation:** [Imperative statement of exactly what to change or do. Specific —
name the field, sentence, criterion, or section. The reviewer has done the
verification; the author should not need to investigate further.]
```

**Severity levels:**

| Severity | Meaning |
|----------|---------|
| **Critical** | Breaks the design or violates an architectural invariant. Blocks implementation regardless of other findings. |
| **High** | Strongly recommended fix. Does not break an invariant but would likely cause incorrect or incomplete implementation. |
| **Medium** | Should be addressed. A gap or imprecision that could cause confusion or partial implementation. |
| **Low (Completeness)** | A missing detail that is low risk but worth adding. |
| **Low (Precision)** | Wording an implementing agent could reasonably misinterpret, but the likely interpretation is probably correct. |
| **Low (Informational)** | Context worth surfacing. No change strictly required. |
| **Convention** | Deviation from the ticket writing specification, naming conventions, or format rules. Orthogonal to the severity hierarchy. |

**Rules for raising issues:**
- Verify every issue against the codebase or documents before raising it. Do not raise suspicions — raise findings.
- Do not raise issues about whitespace, capitalisation, or stylistic preferences with no impact on implementation correctness.
- Each recommendation must be actionable without further investigation by the author. The reviewer has done that work.
- Use backticks for all field names, type names, trait names, method names, and symbols.
- Use `@`-prefixed file paths when the ground truth is in the codebase.

---
### 7. Open Questions

Genuine ambiguities the reviewer cannot resolve with available information. Not issues — no definitive fix is knowable from current sources. Do not use this section to avoid making a call: if the answer is findable in the codebase or documents, find it and raise it as an issue instead.

Each question is numbered independently starting from Q1.

```markdown
---

### Open Questions

---

**Q[N]. [Short title]**

[The specific ambiguity, stated precisely. Name the competing options if they exist.
Cite any document sections that are relevant but inconclusive.]
```

---
### 8. Issue Summary

Placed after all detailed sections. References the issue numbers from the Issues section exactly.

```markdown
---

### Issue Summary

| # | Severity | Action |
|---|----------|--------|
| 1 | Critical | [Short imperative describing the fix] |
| 2 | High | [Short imperative] |
| 3 | Low (Precision) | [Short imperative] |
```

---
### 9. Closing Line

Always present. One sentence. No section heading — just the line at the very end.

The closing line is a final comment on the quality of the work relative to its purpose. Match the language to what is being reviewed — whether it is ready to proceed, a sufficient implementation, or still blocked.

**Examples:**

> All findings are advisory — this ticket is ready to hand to an implementing agent as-is.

> Address #1 and #3 to reach a clean 4; the remaining findings are optional refinements.

> This implementation is a sufficient realisation of the plan; the open question above should be noted for the next milestone.

> This plan should not proceed to implementation until the Critical findings above are resolved.
