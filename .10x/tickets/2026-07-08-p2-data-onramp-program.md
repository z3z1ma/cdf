Status: open
Created: 2026-07-08
Updated: 2026-07-10
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
- `.10x/decisions/explicit-sampled-discovery-and-residual-promotion.md`.
- `.10x/specs/data-onramp-schema-intelligence.md`.
- `.10x/specs/data-onramp-file-sources-transports.md`.
- `.10x/specs/data-onramp-source-experience-cli.md`.
- `.10x/specs/data-onramp-conformance.md`.
- `.10x/specs/sampled-schema-discovery-coverage.md`.
- `.10x/specs/residual-variant-capture.md`.
- `.10x/specs/schema-promotion-corrections.md`.
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
- `.10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md`

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
- Explicit sampled discovery is truthful and deterministic; unseen compatible data is validated, and safe nonconforming paths can survive in exact `_cdf_variant` residuals.
- `cdf schema promote` can dry-plan and, where destination capabilities permit, correct or rematerialize retained residuals through packages, receipts, checkpoints, and atomic pin publication without making append require a key.

## Evidence expectations

Each workstream records focused evidence and adversarial review. Parent closure requires aggregate evidence mapping every acceptance criterion, P2 golden path output, coverage-matrix updates, friction-regression mapping, property/fuzz output for the widening lattice, preview/run parity evidence, and a recorded S1+S2 terminal session.

## Explicit exclusions

P2 does not implement a GUI, a scheduler, distributed execution, resident streaming supervisor, zip archive member semantics, or lakehouse destinations. The residual/promotion surface is explicitly scoped by the active focused specs and `.10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md`; no broader post-load modeling surface is implied.

## Progress and notes

- 2026-07-08: Opened from the P2 data-onramp directive after pivoting back from CI/P1 follow-on work. This activation is record-only; no implementation work is started by this parent ticket.
- 2026-07-08: Activation evidence recorded in `.10x/evidence/2026-07-08-p2-data-onramp-program-activation.md`; activation review recorded in `.10x/reviews/2026-07-08-p2-data-onramp-program-activation-review.md`.
- 2026-07-09: Friction regression registry recorded in `.10x/evidence/2026-07-08-p2-friction-regression-registry.md` and linked from WS-I. Initial registry status: all eighteen directive frictions were open P2 coverage obligations; partial existing primitive/negative coverage was documented but not treated as closure coverage.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-08-p2-ws-b1-declarative-arrow-type-vocabulary.md`. This retires the direct "type vocabulary too small" expressibility gap for declarative schemas while leaving WS-B reconciliation/coercion and WS-I conformance ownership open.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-09-p2-ws-b2-schema-reconciliation-core.md`. This establishes the format-independent observed-vs-constraint reconciler, widening lattice, coercion/verdict plan, and `cdf:physical_type` provenance while leaving per-format integration and validation-program execution open.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-08-p2-ws-c1-declarative-schema-normalization.md`. This retires the direct declarative compiled-schema `VendorID` normalization and automatic `cdf:source_name` gaps while leaving broader WS-C destination-sheet and package-evidence work open.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-08-p2-ws-f1-append-default-merge-key-error.md`. This retires the direct append default, keyless append, explicit merge-key, and local scaffold fake-key gaps while leaving S7 CLI rendering and conformance coverage open.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-08-p2-ws-a1-schema-source-model-snapshot-foundation.md`. This establishes the schema-source model split and pinned snapshot artifact/store foundation for WS-A, with unpinned discover/hints still fail-closed until probe/auto-pin children land.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-09-p2-ws-c2-destination-identifier-policy-adapter.md`. This gives WS-C a destination-sheet-to-contract normalizer adapter, while live plan/run integration and package evidence remain later children.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-08-p2-ws-d1-file-glob-partition-planning.md`. This retires the local modest-N multi-file glob runtime rejection and establishes root-relative per-file partition identity for preview/run, while default `FileManifest` incrementality, remote/public Parquet globs, compression, schema variance, no-op reruns, and full S2/S8 conformance remain open.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-09-p2-ws-h1-resource-id-validation-inspection.md`. This makes compiled resource ids, source/resource names, source files, and project mapping status visible through `cdf inspect resources`, and fails zero-match resource mapping patterns before validate/plan proceed.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-08-p2-ws-e1-file-transport-facade-local-http.md`. This establishes the local/HTTP(S) file transport facade, file identity metadata, bounded HTTP ranged-read seam, explicit HTTP listing rejection, egress/auth hooks, and redacted debug output for URL-bearing transport surfaces while leaving cloud transports, production HTTP wiring, credential resolution, doctor probes, HTTP template enumeration, compression, and full run integration open.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-09-p2-ws-a2-local-parquet-discovery-probe.md`. This establishes the first concrete Parquet footer/schema discovery API and project schema snapshot handoff while leaving remote ranged discovery, schema CLI, auto-pin, lockfile writes, run/plan integration, and conformance S1/S2 closure open.
- 2026-07-09: P2 implementation child opened: `.10x/tickets/2026-07-09-p2-ws-b3-parquet-declared-schema-reconciliation.md`. This is the first WS-B per-format integration slice and targets the current Parquet declared-schema bypass.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-09-p2-ws-b3-parquet-declared-schema-reconciliation.md`. This makes local declared-schema Parquet reads use the shared reconciler, materializes supported Arrow width widenings, preserves physical provenance, and routes declarative Parquet resources through the declared-schema reader while leaving remote discovery, policy threading, validation-program serialization, and S1/S2/S8 conformance open.
- 2026-07-09: P2 implementation child opened: `.10x/tickets/done/2026-07-09-p2-ws-a3-local-parquet-discover-autopin.md`. This targets the next S1-critical gap: an operator-visible `cdf schema discover` doorway for local Parquet and using the local Parquet footer probe to auto-pin single-file discover resources before plan/run.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-09-p2-ws-a3-local-parquet-discover-autopin.md`. Local single-file Parquet discover resources now have a no-write `cdf schema discover <resource>` command and plan/run auto-pin into deterministic normalized snapshots. Broader discovery surfaces remain open per WS-A sequencing.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-09-p2-ws-a4-generic-discovery-postgres-catalog.md`. Discovery is no longer wired as a Parquet-only CLI/project path: `cdf schema discover` now dispatches by source archetype and can probe declarative Postgres table resources through catalog metadata and the project secret provider. SQL `plan`/`run` auto-pin, REST/Python/WASM/future Avro probes, and remote/multi-file discovery remain open P2 slices.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-09-p2-ws-a5-generic-discover-autopin-postgres-run.md`. Declarative Postgres table resources in discover mode now auto-pin from catalog metadata and work through `cdf plan`, `cdf preview`, and `cdf run`, including source-name-aware physical column reads. This is still not product-wide discovery completion; REST/Python/WASM/future Avro probes, schema pin/show/diff, remote/multi-file discovery, and S4/S5 conformance remain open.
- 2026-07-09: P2 implementation child closed: `.10x/tickets/done/2026-07-09-p2-ws-a6-rest-sample-discovery-autopin.md`. Declarative REST discover-mode resources now have one-page sample discovery and first-use auto-pin through `cdf schema discover`, `cdf plan`, `cdf preview`, and `cdf run`. The program still owns pagination-wide REST sampling, schema pin/show/diff, remote file discovery/execution, `cdf add`, manifest no-op reruns, cloud transports, compression, diagnostics/deep validate, and S1-S8 conformance.
- 2026-07-09: Split next execution batch: D3 manifest no-op incrementality, E2 HTTPS file runtime/discovery, A7 schema pin/show/diff, G1 diagnostics/deep validate foundation, H2 `cdf add` single-file Parquet, and I2 conformance matrix/parity foundation.
- 2026-07-09: Closed the A7/D3/I2 batch with shared evidence `.10x/evidence/2026-07-09-p2-ws-a7-d3-i2-batch.md` and review `.10x/reviews/2026-07-09-p2-ws-a7-d3-i2-batch-review.md`. Completed children: `.10x/tickets/done/2026-07-09-p2-ws-a7-schema-pin-show-diff-cli.md`, `.10x/tickets/done/2026-07-09-p2-ws-d3-file-manifest-incremental-noop.md`, and `.10x/tickets/done/2026-07-09-p2-ws-i2-preview-run-parity-and-golden-path-matrix.md`.
- 2026-07-09: Closed the E2/G1/B4 batch with shared evidence `.10x/evidence/2026-07-09-p2-e2-g1-b4-batch.md` and review `.10x/reviews/2026-07-09-p2-e2-g1-b4-batch-review.md`. Completed children: `.10x/tickets/done/2026-07-09-p2-ws-e2-http-file-runtime-and-discovery.md`, `.10x/tickets/done/2026-07-09-p2-ws-g1-source-diagnostics-and-deep-validate-foundation.md`, and `.10x/tickets/done/2026-07-09-p2-ws-b4-widening-property-conformance.md`. The parent remains open for H2 `cdf add`, remote HTTP glob/template enumeration, cloud transports, compression, final source diagnostics, full S1-S8 conformance, and the S1+S2 recorded session.
- 2026-07-09: Closed the H2/D4/B5 batch with integration evidence `.10x/evidence/2026-07-09-p2-h2-d4-b5-integration-quality.md` and review `.10x/reviews/2026-07-09-p2-h2-d4-b5-integration-review.md`. Completed children: `.10x/tickets/done/2026-07-09-p2-ws-h2-cdf-add-single-file-parquet.md`, `.10x/tickets/done/2026-07-09-p2-ws-d4-gzip-zstd-file-decode-foundation.md`, and `.10x/tickets/done/2026-07-09-p2-ws-b5-validation-program-coercion-evidence.md`. The parent remains open for public S1/S2, HTTP glob/template enumeration, cloud transports, remote compression, schema variance/drift quarantine, REST/Postgres add, ad-hoc mode, final S1-S8 conformance, and the recorded S1+S2 session.
- 2026-07-09: Reconciled the continuation audit against current source and opened the next executable batch: A8 durable auto-pin/no-pin inspection, B6 JSON-family observed-first reconciliation, and I3 conformance matrix/friction-registry repair. Mandatory dlt source/naming/schema study provenance is recorded in `.10x/research/2026-07-09-p2-dlt-source-schema-naming-study.md`. Later Hints, HTTP enumeration, cloud credentials, and keyless exact-row dedup remain at explicit confirm-or-correct checkpoints rather than being implemented from guessed semantics.
- 2026-07-09: Closed A8/B6/I3 with integration evidence `.10x/evidence/2026-07-09-p2-a8-b6-i3-integration.md` and adversarial review `.10x/reviews/2026-07-09-p2-a8-b6-i3-integration-review.md`. Completed children: `.10x/tickets/done/2026-07-09-p2-ws-a8-autopin-lockfile-no-pin.md`, `.10x/tickets/done/2026-07-09-p2-ws-b6-json-family-observed-reconciliation.md`, and `.10x/tickets/done/2026-07-09-p2-ws-i3-matrix-friction-reconciliation.md`. The review caught and repaired perpetual re-probing, coercion-evidence injection, JSON policy/localization, and lifecycle-test defects before closure.
- 2026-07-09: Opened the next record-backed executable batch: B7 REST observed-first reconciliation, F2 exact S7 append/merge experience, and G2 reconciliation-specific command diagnostics. These slices deliberately exclude the still-unratified Hints syntax, HTTP template enumeration, cloud credential object shape, and exact-row dedup option.
- 2026-07-09: Closed B7 and F2 with integration evidence `.10x/evidence/2026-07-09-p2-b7-f2-integration.md` and adversarial review `.10x/reviews/2026-07-09-p2-b7-f2-integration-review.md`. REST execution now uses the shared observed-first reconciliation path without runtime-only policy authority; append/merge CLI behavior meets its scoped S7 contract. G2 remains blocked rather than encoding unratified JSON probe bounds, row-local deep-validation rendering, or Tier-0 type-policy syntax/defaults.
- 2026-07-09: Shaped the next record-backed execution tranche: I4 standalone S5/S7 conformance and C3 live DuckDB/Postgres destination normalization. Parquet column policy is not inferred from its object-key normalizer; it remains outside C3 pending an explicit semantic record.
- 2026-07-09: Shaped H3 as the next WS-H slice: ad-hoc local/stable-HTTPS Parquet execution must persist a real `.cdf/adhoc/` resource and flow through ordinary discovery, plan, package, receipt, checkpoint, and ledger evidence. Postgres/REST add and broader ad-hoc sources remain outside this child.
- 2026-07-09: Shaped A9 for local Arrow IPC schema-block discovery, pin lifecycle, preview/run parity, and package-producing execution. This advances the product-wide Discover requirement without choosing the still-unratified CSV/JSON/NDJSON sample bound.
- 2026-07-09: Closed C3 and I4 with integration evidence `.10x/evidence/2026-07-09-p2-c3-i4-integration.md` and adversarial review `.10x/reviews/2026-07-09-p2-c3-i4-integration-review.md`. S5/S7 are green standalone scenarios; DuckDB/Postgres destination normalization is live and fail-closed against stale policy evidence. The parent remains open for S1-S4/S6/S8, Parquet column policy, H3/A9, cloud/HTTP enumeration, format/drift work, G2 ratification, docs, and the recorded TLC session.
- 2026-07-09: Shaped D5 for record-backed `.parquet`/`.arrow` extension-plus-magic auto-detection. CSV/JSON/NDJSON confirmation and noncanonical binary aliases remain outside the child pending explicit signal rules.
- 2026-07-09: A10 was expanded and blocked as `.10x/tickets/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md` after the user rejected single-file discovery as an architectural endpoint. It now owns a format-neutral discovery-set aggregator for Parquet and Arrow IPC, pinned discovery-manifest identity, and per-file contract verdicts; implementation awaits confirmation of the explicit aggregation/large-N/freeze checkpoint rather than inventing semantics.
- 2026-07-09: Opened I5 after two parallel workspace runs each reached 791/792 while standalone S5 stayed green. The recorded HTTP fixture treats nonblocking `WouldBlock` as EOF and can capture headers before authorization arrives; I5 owns a bounded deterministic harness repair without weakening S5 semantics.
- 2026-07-09: Closed H3 and A9 with integration evidence `.10x/evidence/2026-07-09-p2-h3-a9-integration.md` and review `.10x/reviews/2026-07-09-p2-h3-a9-integration-review.md`. Evidence-preserving local/HTTPS Parquet ad-hoc execution and bounded local Arrow IPC discovery/run are complete after adversarial repairs. The program remains open for A10, D5, I5, G2 ratification, S1-S4/S6/S8 completion, transports/file variance, docs, and the recorded S1+S2 session.
- 2026-07-09: Closed D5 and I5 with integration evidence `.10x/evidence/2026-07-09-p2-d5-i5-integration.md` and review `.10x/reviews/2026-07-09-p2-d5-i5-integration-review.md`. Glob/resource-level Parquet and Arrow IPC format inference now confirms every local match, bounded HTTPS Parquet remains supported, unsupported remote Arrow fails at plan time, and the S5 parallel request-capture race is closed. Final workspace nextest passed 809/809. The program remains open for A10's resource-level multi-file schema aggregation/pinning contract, G2 ratification, S1-S4/S6/S8, cloud/remote file breadth, docs, and the recorded S1+S2 session.
- 2026-07-09: Multi-file discovery research is consolidated in `.10x/research/2026-07-09-multi-file-discovery-aggregation-contract.md`. It rejects sampled binary pins and recommends one exhaustive resource-level aggregator with distinct baseline/effective/manifest identities and per-file gate-bearing verdicts. A10 was blocked at this checkpoint until the following ratification entry.
- 2026-07-09: The user ratified A10 and the production-scale portability invariant. `.10x/decisions/multi-file-discovery-aggregation-and-budget.md` makes discovery exhaustive and executor-neutral; 64 MiB/file, 128 MiB in flight, and 8 probes are configurable plan-recorded per-executor defaults rather than global ceilings. A10a/A10b are executable in parallel, followed by A10c-f.
- 2026-07-10: The user confirmed explicit `stratified-hash-v1` sampled discovery, exact field/path residual capture, and fenced plan-first schema promotion with framework row provenance and capability-dependent correction/rematerialization. Active focused specs now govern each surface. A10g owns sampled binary discovery; `.10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md` owns the separately decomposed residual/correction/lease/promotion/conformance graph. This turn is governing-record activation only; implementation begins from executable children afterward.

## Blockers

None for parent activation. Workstream implementation tickets may carry technical dependencies or focused decisions such as large-N coalescing thresholds.
