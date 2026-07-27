---
name: audit-error-ownership
description: "Use when auditing error kinds, reclassifying Internal failures, or changing an adapter/SDK/codec/process wrapper that can flatten typed errors or lose source provenance."
metadata:
  created: 2026-07-27
  updated: 2026-07-27
---

# Audit Error Ownership

## Objective

Prove that each error kind names the actor that can repair the primary failure, including through
foreign wrappers. Produce a reproducible site inventory, semantic classifications, focused
regressions, and honest limits without optimizing for a smaller Internal count.

## Prerequisites

1. Read `.10x/knowledge/error-ownership-taxonomy.md` and the governing error spec/ticket.
2. Resolve the exact crate/file scope and existing error helpers.
3. Identify origin boundaries: external source artifact, durable destination artifact, private CDF
   scratch, local host facility, remote provider, and CDF-owned invariant.

## Procedure

### 1. Build a complete construction inventory

Search both convenience constructors and direct enum construction. Do not assume one spelling:

```sh
rg --files -0 -g '*.rs' \
  crates/cdf-source-files crates/cdf-transport-http crates/cdf-subprocess \
  > /tmp/error-ownership-audit-files.nul
xargs -0 rg -n -- 'CdfError::internal|\.internal\(' \
  < /tmp/error-ownership-audit-files.nul
xargs -0 rg -n -- 'ErrorKind::Internal|\bInternal\b' \
  < /tmp/error-ownership-audit-files.nul
```

Replace the three example crate roots with the resolved audit scope before running the commands, and
record those exact roots. The null-delimited manifest preserves spaces and makes subsequent scans
share one frozen file set. Inspect direct `new`, struct literal, conversion, macro, helper-factory,
test assertion, and enum mapping matches. Record total files, site-bearing files, site count, and
the arithmetic remainder. A count delta is not a migration count when tests or invariant checks
were added.

### 2. Classify by owner, not keyword

Read the construction and surrounding control flow:

- Host/process facilities—permissions, physical memory/space, descriptors, devices, current
  directory, executable spawn, local TLS/resolver/runtime—are `Environment`.
- External source absence, truncation, invalid encoding, wrong shape, and symlink loops are `Data`.
- Durable destination artifact absence, truncation, invalid encoding, wrong shape, and symlink
  loops are `Destination`.
- Missing/corrupt private scratch after CDF created it, poisoned ownership, impossible lifecycle,
  serialization logic, identity drift, decoder state, and task panic/join are `Internal`.
- Invalid caller configuration is `Contract`. A configured executable rejected during validation
  is Contract; failure to spawn a validated executable is Environment.

Treat `exists()`-style APIs as suspicious: they can collapse permission/device metadata errors into
absence. Use a fallible metadata operation when the distinction changes cleanup or durability.

### 3. Preserve provenance through wrappers

Walk the complete source chain with this precedence:

1. Embedded typed `CdfError`, including one stored inside `std::io::Error`; preserve kind,
   `retry_after_ms`, and primary message while adding safe action context.
2. Explicit boundary provenance supplied by the caller: source artifact, local destination
   artifact, private scratch, or remote provider.
3. Raw I/O classification appropriate to that boundary.
4. Coarse SDK/provider retry and kind fallback.

Do not return on the first raw `std::io::Error` before checking deeper for typed CDF errors. Do not
infer local host ownership merely because a remote provider's source chain contains
`std::io::Error`; wrapper APIs must carry local-versus-provider provenance.

### 4. Test the matrix

Add the smallest regressions that falsify the boundary:

- direct typed CDF and typed CDF nested inside I/O/SDK wrappers;
- rate-limited retry delay plus at least one non-retry typed kind;
- NotFound/UnexpectedEof/InvalidData/wrong-shape/loop versus PermissionDenied/resource failure;
- external source, durable local destination, remote provider, and private scratch;
- status/protocol fallbacks that previously used Internal;
- remediation presence and secret-safe context.

Run the complete affected adapter suite after focused tests. Run representative human, headless,
and JSON CLI gates when the error reaches the product boundary. Run strict all-target/all-feature
lint, formatting, and diff checks.

### 5. Record and review

Write durable evidence with the reproducible inventory, classification families, commands, results,
and platform/fault-injection limits. Commission an independent read-only review that attempts to
falsify source-chain precedence and boundary provenance. Update counts after review-driven tests;
stale pre-repair counts are not closure evidence.

## Validation

- Every scoped Internal construction is discoverable by the recorded search or explicitly covered
  by the direct-enum/helper supplement.
- Typed errors survive direct and nested wrapper paths with retry metadata intact.
- Raw I/O produces different kinds where source, destination, private-scratch, local-host, and
  remote-provider ownership differs.
- Focused/full tests, strict lint, formatting, and `git diff --check` pass.
- `graphify update .` runs after code changes, or the missing executable is recorded as a limit.
