Status: done
Created: 2026-07-19
Updated: 2026-07-20
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md
Depends-On: .10x/tickets/done/2026-07-19-iceberg-f1-neutral-object-access.md

# Glue G1: conventional external-table source — closed

## Scope

Implement Glue catalog object classification and conventional object-store external-table compilation/execution through neutral object access and format registries, including descriptors, schema hints, partition predicate pushdown/pagination, per-partition overrides, exact routing errors, object-manifest incrementality, product hooks, and mocked protocol conformance.

## Non-goals

No Iceberg duplication, Delta/Hudi/view/federated/JDBC/stream execution, Glue jobs/crawlers/catalog mutation, or Lake Formation governed access beyond exact detection/failure.

## Acceptance Criteria

- Classification routes every supported/unsupported table family exactly before payload work.
- Supported Parquet and row-format external tables honor table/partition descriptors and registered format semantics.
- Partition planning is bounded/spill-backed, predicate-classified, cancellable, retriable, and deterministic.
- Table/partition/object identity supports correct no-op/new/changed planning without treating Glue schema as physical truth.
- Ordinary add/discovery/deep-validation/preview/run/replay/inspect/doctor and source conformance hooks pass without generic branches.

## References

- `.10x/specs/aws-glue-external-table-source.md`
- `.10x/specs/source-extension-runtime-contract.md`

## Assumptions

- User-ratified 2026-07-19: conventional Glue external tables are a separate source; Iceberg routes to the Iceberg driver.

## Journal

- 2026-07-20: Activated after neutral object access F1 and Iceberg I3 closed. The implementation lane is a dedicated `cdf-source-glue` driver: Glue protocol/classification remains source-local, selected objects use injected `cdf-object-access`, and physical decoding joins registered format drivers. No runtime delegation to `cdf-source-iceberg` or `cdf-source-files` is permitted.
- 2026-07-20: Extracted credential resolution, SigV4, endpoint partitioning, egress, cancellation, and bounded AWS JSON transport into the source-neutral `cdf-aws` crate. The existing Iceberg Glue catalog binding now consumes that authority, removing its duplicate signer/credential/HTTP implementation before the conventional-table driver joined it.
- 2026-07-20: Implemented exact Glue object-family classification, Glue/Hive-to-Arrow schema hints, table and partition descriptor reconciliation, canonical predicate expression generation, bounded paginated partition enumeration, canonical spill-backed object task planning, generation revalidation, and `FileManifest` resume/no-op selection. Provider limits remain configurable safety bounds rather than execution concurrency caps.
- 2026-07-20: Implemented file execution exclusively through injected `FileTransport`, the process-scoped `FormatRegistry`, and the process-scoped `ByteTransformRegistry`. Parquet and streamed NDJSON fixtures exercise the same driver path; partition columns materialize as typed constants after physical decoding, and batch retention stays under the shared memory authority.
- 2026-07-20: Registered Glue at the CLI composition root without a generic runtime branch. `cdf add glue://<region>/<database>/<table>` authors typed Glue configuration; classification remains pre-payload, with Iceberg, Delta, Hudi, view, federated, stream, and unknown-SerDe outcomes naming their exact owner/remediation.
- 2026-07-20: Read-only FQ12 smoke against `bronze.transactions` completed in 0.685 seconds and rejected it as Iceberg before object listing/payload work, with the required `iceberg` + Glue-catalog route. An account-wide read-only catalog search found no conventional table fixture, so conventional live coverage is explicitly left to G2's governed fixture/conformance scope rather than mutating shared AWS state under G1.

## Blockers

None. Lake Formation authorization and live governed fixtures are owned by G2.

## Evidence

- Classification and mapping: `cargo test -p cdf-source-glue --lib` passed 11 tests, including every owned/non-owned table family, unknown SerDe failure, exact Parquet/Avro/JSON/CSV mappings, partition overrides, documented Glue predicate syntax, pagination-token safety, and nested/wide schema parsing.
- Complete physical paths: the Parquet fixture discovers, plans an external canonical task, decodes, materializes a typed partition column, publishes completion evidence, and rewrites an unchanged resume to a visible no-op. The NDJSON fixture proves row formats use the registered streaming decoder rather than a Glue-private reader.
- Shared AWS and Iceberg preservation: `cargo test -p cdf-aws -p cdf-source-glue -p cdf-source-iceberg --lib` passed 50 tests (2 AWS, 11 Glue, 37 Iceberg), including request redaction and the migrated Iceberg Glue binding.
- Product composition: `cargo test -p cdf-cli source_registry::tests::builtin_registry_is_process_scoped --lib` passed and asserts one process-scoped Glue driver alongside the existing source registry.
- Static/supply-chain gate: `cargo clippy -p cdf-aws -p cdf-source-glue -p cdf-source-iceberg -p cdf-cli --all-targets -- -D warnings`, `cargo deny check`, and `git diff --check` passed.
- Live read-only boundary: exported the existing FQ12 PowerUser SSO credentials and ran `cdf add live.transactions glue://us-west-2/bronze/transactions --dry-run`; Glue classified the real Iceberg table in 0.685 seconds and failed with the exact authoring route before S3 access. No AWS resource was created or modified.

## Review

Verdict: pass.

Fresh-hat review attempted to falsify source isolation, credential leakage, table-family routing, generation coherence, bounded memory/disk, predicate syntax, row-format semantics, and resume correctness. The review found and corrected two pre-closure gaps: backtick-quoted partition identifiers were replaced with the documented simple-identifier Glue expression subset (unrepresentable identifiers remain an engine-side residual), and an actual streamed row-format end-to-end fixture was added rather than inferring row support from classification alone. AWS signing/credentials are neutral, task artifacts contain no credentials, catalog generations are revalidated after inventory, tasks are path-canonical and externalized, and no generic runtime/project/engine branch names Glue. Lake Formation is an explicit fail-closed boundary owned by G2, not a G1 residual hidden behind ambient S3 credentials.

## Retrospective

- The first implementation draft could prove Parquet composition but not row-format composition; acceptance evidence should cross each materially different codec family, not treat registry membership as execution proof.
- AWS control-plane mechanics immediately appeared twice (Iceberg Glue and conventional Glue). Extracting `cdf-aws` before adding a third protocol kept source crates focused on their request/response semantics and makes Athena composition materially smaller.
- Glue's Data Catalog does not validate type strings, so its parser must be strict about structural syntax while retaining the original `cdf:glue_type` provenance and accepting the full useful Arrow decimal/nested envelope.
- Canonical, spill-backed tasks and format/object registries made this source additive: the generic runtime required no Glue conditionals. That is the extension-boundary law future catalog sources should preserve.
