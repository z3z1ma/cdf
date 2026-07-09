Status: open
Created: 2026-07-08
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-07-p0-structural-debt-program.md

# P2 data onramp, source experience, schema intelligence, and happy path program

## Scope

Implement the P2 data-onramp program: schema discovery as a compiler stage, schema reconciliation and full Arrow declarative vocabulary, automatic identity normalization, real file-source partitions and manifest incrementality, remote transports, disposition/key ergonomics, source-specific diagnostics, `cdf add` and ad-hoc flows, and conformance coverage for the eight P2 golden paths.

This parent is a plan and orchestration record. Workstream tickets own the major lanes below; implementation inside a broad workstream MUST be split into bounded executable child tickets before code changes when the workstream contains multiple independent outcomes.

## Governing records

- `VISION.md`, especially Chapters 7, 8, 11, 13, 18, 19, and 20.
- `.10x/decisions/cdf-system-authority.md`.
- `.10x/decisions/data-onramp-schema-discovery-reconciliation.md`.
- `.10x/decisions/data-onramp-file-source-transport-manifest.md`.
- `.10x/decisions/data-onramp-source-identity-preview-disposition.md`.
- `.10x/specs/data-onramp-schema-intelligence.md`.
- `.10x/specs/data-onramp-file-sources-transports.md`.
- `.10x/specs/data-onramp-source-experience-cli.md`.
- `.10x/specs/data-onramp-conformance.md`.
- `.10x/specs/resource-authoring-planning-batches.md`.
- `.10x/specs/types-contracts-normalization.md`.
- `.10x/specs/project-cli-observability-security.md`.
- `.10x/specs/conformance-governance-roadmap.md`.
- `.10x/knowledge/runtime-conformance-throughput-rule.md`.

## Hard guardrails

- CDF MUST NOT collapse into dlt-style perpetual inference. Discovery is pinned, package identity participates in the discovered snapshot, and drift is a governed contract event.
- The plan artifact survives every convenience. `cdf add`, discover mode, ad-hoc mode, preview, and remote file sources compile into plan/package/receipt/checkpoint evidence rather than bypassing it.
- Verdicts stay total. Widening, coercion, schema drift, normalization, and file variance produce validation-program or contract verdicts where applicable.
- Commit gate, package determinism, replay identity, redaction, secret references, egress policy, and destination receipt verification are untouched.
- P1 rendering and error-catalog paths apply to every new CLI surface. JSON output changes are additive only.
- No P2 workstream closes until conformance owns its changed runtime behavior.

## Golden paths

- S1: public HTTPS Parquet single file, zero typed schema fields, `cdf add` then `cdf run`.
- S2: monthly public HTTPS Parquet glob with `FileManifest` default incrementality and no-change no-op rerun.
- S3: S3 compressed NDJSON recursive glob, transparent gzip, bounded discovery, and contract-governed drift.
- S4: Postgres table discovery with optional schema block and cursor candidates.
- S5: REST API in discover mode with recorded sample page and pinned snapshot.
- S6: incompatible drift quarantines with accepted stream unblocked and useful remediation.
- S7: append requires no key; merge without a key emits one precise plan-time error.
- S8: preview/run parity across source archetypes.

## Workstreams

- `.10x/tickets/2026-07-08-p2-ws-a-discovery-compiler-stage.md`
- `.10x/tickets/2026-07-08-p2-ws-b-schema-reconciliation-arrow-vocabulary.md`
- `.10x/tickets/2026-07-08-p2-ws-c-source-identity-normalization.md`
- `.10x/tickets/2026-07-08-p2-ws-d-file-source-globs-manifest-compression.md`
- `.10x/tickets/2026-07-08-p2-ws-e-remote-transports.md`
- `.10x/tickets/2026-07-08-p2-ws-f-keys-dispositions.md`
- `.10x/tickets/2026-07-08-p2-ws-g-source-diagnostics-deep-validate.md`
- `.10x/tickets/2026-07-08-p2-ws-h-scaffolding-id-model-two-minute-path.md`
- `.10x/tickets/2026-07-08-p2-ws-i-conformance-parity-friction-suite.md`

## Sequencing

WS-B and WS-C may start immediately. WS-A and WS-D are core lanes and may proceed in parallel. WS-E follows the WS-D facade shape, but object-store facade scaffolding may start now if it does not lock in wrong file semantics. WS-F and WS-G ride alongside continuously. WS-H starts once WS-A, WS-B, and WS-D stabilize enough that `cdf add` will not encode throwaway shapes. WS-I accrues throughout and gates every workstream closure.

Where performance tickets intersect file partitioning, remote reads, compression, or Parquet streaming, coordinate with the benchmark/performance backlog instead of duplicating work.

## Acceptance criteria

- A stranger can load six months of public TLC yellow-taxi Parquet data into DuckDB with one `cdf add` and one `cdf run`, first try, through P1 rendering, in under two minutes.
- S1-S8 are green in CI, with network-dependent cases covered by deterministic fixtures in ordinary CI and live-tier evidence where appropriate.
- Declarative types express the full Arrow vocabulary required by `VISION.md` Chapter 7, including decimals and nested types.
- No execution path rejects discover-mode resources merely because the schema source is not declared.
- Multi-file globs plan as partitions and use `FileManifest` incrementality by default.
- `file://`, implicit local paths, `https://`, `s3://`, `gs://`, and `az://` file sources work through one facade with secret refs and egress policy.
- Preview/run parity is a conformance law.
- `namecase-v1` runs automatically and `source_name` is override-only.
- The eighteen P2 frictions each name a regression test in closure evidence.
- Coverage matrix rows for Chapter 7.5, 8.2, 8.6, 9.2, and 13.3 are updated as work closes.

## Evidence expectations

Each workstream records focused evidence and adversarial review. Parent closure requires aggregate evidence mapping every acceptance criterion, P2 golden path output, coverage-matrix updates, friction-regression mapping, property/fuzz output for the widening lattice, preview/run parity evidence, and a recorded S1+S2 terminal session.

## Explicit exclusions

P2 does not implement a GUI, a scheduler, distributed execution, resident streaming supervisor, zip archive member semantics, lakehouse destinations, or new non-source product surfaces unless later active tickets explicitly scope them.

## Progress and notes

- 2026-07-08: Opened from the P2 data-onramp directive after pivoting back from CI/P1 follow-on work. This activation is record-only; no implementation work is started by this parent ticket.
- 2026-07-08: Activation evidence recorded in `.10x/evidence/2026-07-08-p2-data-onramp-program-activation.md`; activation review recorded in `.10x/reviews/2026-07-08-p2-data-onramp-program-activation-review.md`.
- 2026-07-09: Friction regression registry recorded in `.10x/evidence/2026-07-08-p2-friction-regression-registry.md` and linked from WS-I. Initial registry status: all eighteen directive frictions were open P2 coverage obligations; partial existing primitive/negative coverage was documented but not treated as closure coverage.
- 2026-07-09: First P2 implementation child closed: `.10x/tickets/done/2026-07-08-p2-ws-b1-declarative-arrow-type-vocabulary.md`. This retires the direct "type vocabulary too small" expressibility gap for declarative schemas while leaving WS-B reconciliation/coercion and WS-I conformance ownership open.
- 2026-07-09: Second P2 implementation child closed: `.10x/tickets/done/2026-07-08-p2-ws-c1-declarative-schema-normalization.md`. This retires the direct declarative compiled-schema `VendorID` normalization and automatic `cdf:source_name` gaps while leaving broader WS-C destination-sheet and package-evidence work open.
- 2026-07-09: Third P2 implementation child closed: `.10x/tickets/done/2026-07-08-p2-ws-f1-append-default-merge-key-error.md`. This retires the direct append default, keyless append, explicit merge-key, and local scaffold fake-key gaps while leaving S7 CLI rendering and conformance coverage open.
- 2026-07-09: Fourth P2 implementation child closed: `.10x/tickets/done/2026-07-08-p2-ws-a1-schema-source-model-snapshot-foundation.md`. This establishes the schema-source model split and pinned snapshot artifact/store foundation for WS-A, with unpinned discover/hints still fail-closed until probe/auto-pin children land.
- 2026-07-09: Fifth P2 implementation child closed: `.10x/tickets/done/2026-07-09-p2-ws-c2-destination-identifier-policy-adapter.md`. This gives WS-C a destination-sheet-to-contract normalizer adapter, while live plan/run integration and package evidence remain later children.
- 2026-07-09: Sixth P2 implementation child closed: `.10x/tickets/done/2026-07-08-p2-ws-d1-file-glob-partition-planning.md`. This retires the local modest-N multi-file glob runtime rejection and establishes root-relative per-file partition identity for preview/run, while default `FileManifest` incrementality, remote/public Parquet globs, compression, schema variance, no-op reruns, and full S2/S8 conformance remain open.

## Blockers

None for parent activation. Workstream implementation tickets may carry technical dependencies or focused decisions such as large-N coalescing thresholds.
