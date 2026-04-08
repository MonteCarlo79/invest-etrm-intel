# CODEX ↔ OPENCLAW BRANCH SYNC RULE

## Purpose

Prevent handoff failures where OpenClaw writes operational docs on one branch but Codex cannot see them from its local working branch.

This is a process rule, not a code change.

---

## Problem

Current recurring failure mode:
- OpenClaw writes runbooks / repair briefs / task specs on a docs or ops branch
- those docs are pushed to GitHub
- Codex continues working from a different local branch without fetching or reading that branch
- Codex then reports that the file is missing, even though it exists on GitHub

This creates avoidable confusion.

---

## Rule

When OpenClaw provides a handoff doc for Codex, the handoff is considered authoritative if all three are provided:
- branch name
- repo-relative file path
- latest commit SHA (if available)

Example:
- branch: `docs/codex-desktop-handoff-pack`
- file: `docs/openclaw/MENGXI_AGENT4_MVP_VALIDATION_RUNBOOK_SANITIZED.md`
- commit: `d5d3f71`

If Codex cannot see the file locally, it should treat this as a **branch sync issue**, not as proof the document does not exist.

---

## Required Codex behavior

Before saying a referenced OpenClaw handoff file is missing, Codex should do one of the following:

### Option A — fetch/read the authoritative branch
Fetch the named branch from origin and read the file from that branch.

### Option B — read directly from git object by branch/ref
Read the file via `git show origin/<branch>:<path>` or equivalent.

### Option C — explicitly report branch visibility limitation
If Codex cannot fetch for some reason, it should say:
- local branch cannot currently see the referenced docs branch
- it will follow the user-provided rules in the meantime

It should not frame the document as nonexistent if the issue is only local visibility.

---

## Required OpenClaw handoff format

When handing work to Codex, OpenClaw should include:
1. branch name
2. exact repo-relative path
3. short purpose statement
4. latest commit SHA if available

Preferred format:

> Authoritative handoff doc:
> - branch: `docs/codex-desktop-handoff-pack`
> - file: `docs/openclaw/...md`
> - commit: `<sha>`
> - purpose: `<one-line purpose>`

This reduces ambiguity.

---

## Working rule for mixed-branch collaboration

### OpenClaw
- may write handoff/runbook/spec docs on its own docs/ops branch
- should not assume Codex local checkout automatically sees them
- should always name branch + path + commit when handing off

### Codex
- may keep implementation work on its own feature/fix branch
- should not require OpenClaw docs to exist on the same local branch before following them
- should fetch/read the authoritative docs branch when needed

---

## Minimal operator rule

If a handoff doc exists on GitHub and Codex cannot see it locally:
- do not argue about file existence
- treat it as a sync problem
- either fetch the branch or use the user-provided summary until synced

---

## Why this matters

This keeps the workflow branch-safe while allowing:
- OpenClaw to prepare operational docs independently
- Codex to continue implementation on its own branch
- both tools to coordinate without false “file missing” confusion

---

## Immediate recommendation

Use this pattern for future handoffs:

- **Docs branch:** `docs/codex-desktop-handoff-pack`
- Every handoff message includes:
  - exact branch
  - exact path
  - latest commit SHA

Codex should read from that branch when a file is referenced there.
