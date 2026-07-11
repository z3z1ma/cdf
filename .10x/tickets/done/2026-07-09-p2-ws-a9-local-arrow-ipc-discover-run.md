Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-a-discovery-compiler-stage.md
Depends-On: .10x/specs/data-onramp-schema-intelligence.md, .10x/specs/data-onramp-source-experience-cli.md, .10x/tickets/done/2026-07-09-p2-ws-a8-autopin-lockfile-no-pin.md, .10x/tickets/done/2026-07-07-cli-preview-resource-breadth.md

# P2 WS-A9 local Arrow IPC discover, pin, and run

## Scope

Complete `SchemaSource::Discover` for deterministic local single-file Arrow IPC resources. Read the IPC file schema block without scanning record batches, normalize and snapshot it through the shared discovery artifact path, support `cdf schema discover|pin|show|diff`, and allow plan/preview/package-producing run to execute against the pinned schema.

Remove the current preview-only Arrow IPC runtime split for this bounded local file surface. Preview and run must open the same planned partition and use the same IPC reader/schema front end; run continues through the normal package/receipt/checkpoint gate.

## Acceptance criteria

- Local single-file `format = "arrow_ipc"`, `schema_source = "discover"` resources can use `cdf schema discover`, first-use auto-pin, explicit pin/show/diff, plan, preview, and run.
- The discovery probe reads the Arrow IPC file schema block, records a named probe/format/source identity and `cdf:normalizer`, and does not decode all record batches merely to discover schema.
- Existing pins remain authoritative until explicit refresh; `--no-pin` observes without writes under the A8 lifecycle.
- Preview and run share partition resolution, IPC decode, pinned schema identity, normalization, and error behavior.
- Package schema/segments, destination receipt, and committed checkpoint use the pinned hash and preserve Arrow field metadata.
- Malformed/truncated IPC, stream-vs-file mismatch where relevant, schema drift against the pin, multi-file ambiguity, and unsupported remote IPC fail with source-specific errors before destination/checkpoint mutation.
- Existing declared Arrow IPC preview behavior remains compatible, and the production path no longer contains a preview-only internal rejection for local Arrow IPC.

## Evidence expectations

Focused declarative/project/CLI tests, schema command snapshots, bounded schema-probe instrumentation, preview/run parity, package artifact verification, malformed input/no-write checks, deterministic reruns, and applicable parser/input/identity quality profiles.

## Explicit exclusions

Remote Arrow IPC, Arrow IPC stream framing, compressed IPC, multi-file schema union, CSV/JSON/NDJSON sampling, Hints, and format auto-detection are excluded. This ticket does not change the Arrow type vocabulary or destination mapping policy.

## Progress and notes

- 2026-07-09: Opened after source inspection found discovery dispatch accepts file resources but routes only Parquet, while local Arrow IPC is explicitly enabled only for preview and otherwise reaches an internal unsupported `FileResource` error. The Arrow IPC schema-block behavior is already ratified and does not depend on the unresolved text-sampling bound.
- 2026-07-09: Added a seekable Arrow IPC file-framing schema probe that constructs the IPC `FileReader` and returns its schema without iterating record batches. The local probe records size, modification time, and physical schema hash as source identity. A counting-reader regression with a one-megabyte record-batch body proves schema discovery reads less than half the file, and a stream-framed fixture fails with an explicit file-versus-stream diagnostic.
- 2026-07-09: Extended generic discovery dispatch for local single-file `format = "arrow_ipc"`. Snapshots use probe `arrow-ipc-file-schema`, format `arrow_ipc`, source kind `files`, and `cdf:normalizer = namecase-v1`; field and schema metadata survive normalization and snapshot reconstruction. Remote, compressed, multi-file, malformed/truncated, and stream-framed IPC remain explicit source-specific failures without schema/lock/package/destination/checkpoint writes.
- 2026-07-09: Removed the preview-only IPC gate from `FileResource`. Preview and ordinary run now share the same seekable local IPC path, planned partition, file-manifest source position, pinned-schema reconciliation, and batch decoder. The shared reader reconciles the physical IPC schema against the pinned or declared schema through the existing strict lossless coercion path, so incompatible pinned drift fails in both preview and run before destination mutation or checkpoint commit.
- 2026-07-09: Added CLI lifecycle coverage for write-free `schema discover`, `plan --no-pin`, first-use plan auto-pin, explicit pin/unchanged, show, diff, ordinary pinned plan, no-write preview, package-producing DuckDB run, package Arrow metadata, receipt/checkpoint completion, and committed rows. Negative coverage proves malformed, multi-file, and remote discovery failures plus incompatible pinned type drift with zero committed checkpoints. Existing declared Arrow IPC preview remains green.
- 2026-07-09: Evidence passed: focused schema-only/file-framing probe 2/2; focused Arrow IPC CLI lifecycle/error/parity 4/4; full `cdf-formats` + `cdf-declarative` + `cdf-project` nextest 228/228; format/diff checks; and all-target warnings-denied Clippy for `cdf-formats`, `cdf-declarative`, `cdf-project`, and `cdf-cli`. Full CLI nextest reached 235/236 with every A9 test green; the sole failure was an unrelated concurrent H3 migrated-error-code expectation (`CDF-PROJECT-CONTRACT` observed versus `CDF-RUN-ARGUMENT` expected), reported to its owner and the parent for integration rerun.
- 2026-07-09: After the H3 owner restored the missing run-resource validation, the previously unrelated CLI regression passed and the complete `cargo nextest run -p cdf-cli --locked --no-fail-fast` rerun passed 236/236. A9 has no remaining test or integration blocker.
- 2026-07-09: Parent adversarial review found the bounded-probe claim is not yet supported end to end. Generic local `plan_partitions` resolution computes a full-file SHA-256 for every matching file before Arrow IPC discovery selects one partition, so the real `schema discover|pin` path reads the entire IPC payload and multi-file rejection hashes every candidate. The unit counting-reader test covers only the direct probe and misses this product-path bypass. Closure is blocked pending discovery-specific local enumeration that avoids runtime manifest hashing before single-file selection, plus end-to-end byte-bound regression evidence through generic discovery.
- 2026-07-09: Repaired the bounded discovery boundary by adding discovery-only local candidate enumeration. It reuses contained glob traversal and bounded four-byte compression detection but does not compute runtime `FileManifest` SHA-256 values; single-file selection or multi-file rejection now occurs before the seekable IPC schema probe. Runtime planning/open remains unchanged and still computes the full checksum used by committed manifest identity.
- 2026-07-09: Added measured probe-byte evidence to the local IPC discovery result and exercised it through the generic CLI `schema discover` path with a one-megabyte payload; the report proves selection plus schema reads remain below half the file size. Expanded project-path no-write failures to truncated file framing, stream framing, gzip and zstd magic detection, and an explicit gzip override, in addition to malformed, multi-file, and remote cases.
- 2026-07-09: Strengthened the successful run proof with package verification, destination receipt verification, pinned hash agreement across receipt/state/checkpoint, exact committed `FileManifest` path/bytes/SHA-256, schema/source metadata, physical type provenance, and preserved coercion evidence. Added an in-scope declared `int32 -> int64` IPC case proving shared lossless widening and serialized `Widened` evidence without adding policy vocabulary.
- 2026-07-09: Repair verification passed: focused Arrow IPC format tests 4/4 and CLI tests 5/5; full affected library tests 467/467 (`cdf-cli` 239, `cdf-declarative` 82, `cdf-formats` 31, `cdf-project` 115); dependency-inclusive and `--no-deps` warnings-denied Clippy for all four affected crates; scoped formatting; and `git diff --check`. The ticket remains open and unmoved for independent re-review.
- 2026-07-09: Independent re-review passed with no findings after tracing discovery candidate selection, actual byte accounting, runtime manifest hashing, unsupported-case failures, package/receipt/checkpoint evidence, and lossless widening. Parent integration evidence is `.10x/evidence/2026-07-09-p2-h3-a9-integration.md`; review is `.10x/reviews/2026-07-09-p2-h3-a9-integration-review.md`. This bounded A9 slice is complete.

## Blockers

None for local single-file Arrow IPC file framing.
