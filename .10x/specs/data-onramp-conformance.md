Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Data onramp conformance

## Purpose and scope

This specification governs the P2 golden paths, preview/run parity law, friction regression suite, and property targets. It refines `.10x/specs/conformance-governance-roadmap.md` and `.10x/knowledge/runtime-conformance-throughput-rule.md`.

## Golden paths

The P2 program is not done until conformance owns these scenarios:

1. Public HTTPS Parquet single file, zero typed schema fields, through `cdf add` and `cdf run`.
2. Public HTTPS Parquet monthly glob with default `FileManifest` incrementality and no-change no-op rerun.
3. S3 compressed NDJSON recursive glob with transparent gzip and drift governed by contract policy.
4. Postgres table discovery with optional schema block and cursor candidates.
5. REST API in discover mode with a recorded sample page and pinned snapshot.
6. Drift quarantines with accepted stream unblocked and file/column remediation rendered.
7. Append requires no key; merge without key fails with precise remediation.
8. Preview/run parity per source archetype.

Network-dependent scenarios SHOULD have recorded deterministic fixtures for ordinary CI and a separate live tier for public/cloud smoke checks.

## Friction regression suite

Each of the eighteen P2 field-test frictions MUST map to at least one regression test before parent closure. The evidence record for closure MUST name the test or conformance scenario that catches each recurrence.

## Property targets

The widening lattice MUST have generated tests for composition and no value loss over generated Arrow arrays where supported.

Source-position serialization MUST continue to round-trip `FileManifest` across `state_version`.

Parser and decoder adversarial tests MUST include NDJSON/JSON schema inference, source-decode drift quarantine, and compressed input boundaries.

## Acceptance criteria

- Every runtime path changed by P2 has conformance ownership before its workstream closes.
- Golden package or package-evidence fixtures exist for at least S1, S2, S5, and S6.
- Preview/run parity is a named conformance law, not only unit tests.
- The final P2 evidence includes a recorded S1+S2 terminal session through the P1 rendering surface.

## Explicit exclusions

This spec does not require live cloud credentials in every push CI run; deterministic fixtures may stand in for normal CI while a live tier remains separately scheduled.
