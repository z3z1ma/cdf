Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md
Depends-On: .10x/decisions/multi-file-discovery-aggregation-and-budget.md, .10x/tickets/done/2026-07-08-p2-ws-a1-schema-source-model-snapshot-foundation.md, .10x/tickets/done/2026-07-09-p2-ws-a8-autopin-lockfile-no-pin.md

# P2 WS-A10a discovery manifest artifact and executor budget

## Scope

Add the backward-compatible serialized foundation for multi-file discovery: validated executor budgets, typed candidate identity/strength/probe/verdict entries, a canonical content-addressed discovery-manifest sidecar, and optional snapshot/lock references that preserve every legacy v1 byte/hash contract.

This child changes artifact/model plumbing only. It does not enumerate multiple files or change discovery results.

## Acceptance criteria

- A validated executor discovery budget defaults to 64 MiB per file, 128 MiB total in flight, and 8 probes; zero, overflow, and per-file-greater-than-total shapes fail precisely.
- The resolved budget is part of canonical discovery-manifest evidence but not candidate membership or schema-join semantics.
- The manifest has deterministic versioned canonical JSON and hash identity over resource, baseline/effective schema references when present, resolved budget, normalizer/policy versions, and sorted candidate/probe/verdict entries.
- Candidate identity explicitly records transport, canonical location, size/mtime when present, identity value and strength, physical-schema hash, probe bytes, participation, metadata variance, and verdict.
- The sidecar is written atomically under a content-addressed `.cdf/schemas/` path; unsafe paths, missing files, tampering, and hash mismatch fail hydration.
- Snapshot/lock references gain optional manifest fields with serde defaults/omission so every existing v1 artifact and byte-stability test remains unchanged.
- Public/compiler models below the CLI remain executor/transport neutral.

## Evidence expectations

Canonical round-trip/hash fixtures, v1 compatibility fixtures, unrelated-lock preservation, atomic write/tamper/path tests, budget validation tests, semver checks, and artifact-focused adversarial review.

## Explicit exclusions

No candidate enumeration, schema joining, format probing, CLI reporting, package stamping, runtime coercion, quarantine, checkpoint, or conformance behavior.

## Progress and notes

- 2026-07-09: Opened after the user ratified `.10x/decisions/multi-file-discovery-aggregation-and-budget.md`.

## Blockers

None.
