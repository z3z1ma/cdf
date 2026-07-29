---
name: audit-project-file-publication
description: "Use when changing multi-file project publication, cdf.lock commit ordering, project-load stabilization, or crash recovery."
metadata:
  created: 2026-07-28
  updated: 2026-07-28
---

# Audit Project File Publication

## Objective

Prove that multi-file project authority is crash-recoverable, preserves unrelated filesystem
authority, and does not make read-only commands mutate merely by loading a project.

## Prerequisites

1. Read `.10x/knowledge/project-file-publication-recovery.md`.
2. Identify every file in the publication, the final public commit point, and the CDF mutation
   guard.
3. Inventory commands that load the project and classify each as read-only, dry-run, or mutating.

## Procedure

### 1. Trace durability order

Confirm every new target temporary and newly created directory ancestry is synced before the
private pending marker. Confirm the pending marker and its ancestry are synced before any public
target install. Confirm installed target parents are synced before the marker becomes committed.

The marker may contain relative paths, lengths, hashes, version, generation, and state. It must not
contain project contents, secrets, signed URLs, tokens, or other sensitive values.

### 2. Enforce one post-marker decision

After pending is durable, use forward recovery only. Do not add an in-memory rollback unless a
durable abort state retains and syncs all prior/new material and is itself process-loss tested at
every rollback boundary.

Recovery must run under the same mutation guard as publication. For each target:

- journaled new hash: accept idempotently and clean only a matching managed temporary;
- journaled prior hash/absence: require a matching managed temporary, then install new;
- any other value or filesystem shape: preserve it and fail `Contract`.

### 3. Separate observation from recovery

Read-only and dry-run commands observe the marker without mutation and fail closed on pending.
Only an explicit mutating retry path may recover. Sample the generation before and after parsing
and compilation; retry when it changes so a caller never receives a mixed multi-file view.

### 4. Preserve error ownership

- corrupt/missing CDF-private marker or temporary: `Internal`;
- local permissions, capacity, descriptor, or device failure: `Environment`;
- unrelated public target or caller expectation drift: `Contract`.

Keep private paths owner-only from initial creation and reject symlink/non-regular marker or
temporary shapes.

### 5. Falsify the crash and race windows

Use a child process that exits without unwinding after a non-final target is installed and before
the final commit target. Assert the pending state is visible, execution-facing read paths fail
closed without mutation, explicit retry converges, and a second retry is idempotent.

Inject an unrelated target change both before recovery and after one target install. Assert CDF
does not overwrite it. Inject missing/corrupt marker temporaries and assert `Internal`; inject host
failures and assert `Environment`.

## Validation

- Focused process-exit, forward-recovery, concurrent-authority, and private-state error-kind tests
  pass.
- Representative plan and preview snapshots are byte-for-byte unchanged while pending.
- A real mutating retry completes recovery before using the new resource.
- The complete affected CLI/project suites and strict all-feature/all-target Clippy pass.
- Formatting and `git diff --check` pass.
- `graphify update .` runs after code changes, or the unavailable executable is recorded as a
  limit.
