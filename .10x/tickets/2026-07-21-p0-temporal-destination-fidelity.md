Status: active
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md

# Make first-party destination type fidelity truthful and executable

## Scope

Repair the cross-destination schema-fidelity boundary so the complete canonical Arrow vocabulary is validated once before execution and every advertised first-party mapping is implemented by the destination hot path. `Unsupported` is reserved for a proven engine/format representation gap; it is not a substitute for implementing a native mapping or a governed lossy fallback.

The bounded implementation owns:

- recursive plan-time validation of the normalized output schema against the selected destination sheet, including nested child types and the resource's recorded `allow_lossy_mapping` policy;
- identical mapping validation in `plan` and the ordinary `run` preflight, before source open, package creation, state creation, or destination mutation;
- DuckDB lossless ingestion of timezone-aware Arrow timestamps at second, millisecond, and microsecond resolution through the sole canonical segment scanner and `TIMESTAMPTZ`, without requiring ICU merely to store UTC instants;
- DuckDB correction/scalar paths required by the same declared mapping, or an explicit plan-time capability exclusion if a correction operation cannot preserve it;
- Parquet schema preflight based on the native arrow-rs writer's actual schema conversion rather than CDF's hand-maintained primitive whitelist;
- Parquet round-trip preservation of timestamp integer value, unit, timezone annotation, nulls, and nested placement through its embedded Arrow schema;
- Postgres native binary-COPY coverage for the full scalar vocabulary and deterministic canonical JSONB fallback for complex Arrow values under `allow_lossy_mapping`;
- truthful destination-sheet mappings and schema-preflight version bumps for DuckDB, Parquet, and Postgres;
- conformance proving that declared lossless mappings are accepted before mutation and round-trip exactly, while unsupported/lossy mappings fail during planning with field path, Arrow type, destination type, and actionable alternatives.

## Non-goals

- No ICU installation, runtime extension loading, or calendar/timezone display functions. DuckDB ICU remains relevant to calendar arithmetic and rendering diagnostics, not lossless storage of an already-zoned Arrow instant.
- No silent nanosecond truncation. Zoned DuckDB nanosecond timestamps remain unsupported unless the compiled schema-coercion program explicitly lowers them under `allow_lossy_mapping`; this ticket does not invent an adapter-local cast.
- No destination-local projection system for types the selected engine cannot represent. Float16, Decimal256, dense union, run-end encoding, and zoned nanosecond gaps remain explicit in DuckDB until a generic compiled projection can preserve or govern them; Parquet retains only arrow-rs-native representation gaps.
- No legacy fallback writer, row appender, alternate Parquet encoder, or destination-specific generic-runtime branch.
- No change to canonical package identity or Arrow timestamp semantics.

## Acceptance Criteria

- Shared planning recursively resolves every output field and nested child against the selected destination sheet before adapter planning or package/destination mutation.
- Missing, unsupported, or unallowed-lossy mappings fail at plan time and identify the exact field path, Arrow type, selected destination, destination type when present, and the supported remediation.
- DuckDB accepts `Timestamp(Second|Millisecond|Microsecond, Some(_))`, creates `TIMESTAMPTZ`, and commits through `canonical_segment_scan` with exact instant/null preservation for at least `UTC`, `+00:00`, and `America/Phoenix` annotations.
- DuckDB continues to reject zoned nanosecond timestamps without a compiled lossy coercion; no commit path silently divides or rounds them.
- Parquet accepts arrow-rs-supported temporal and nested schemas without a duplicated CDF whitelist, and readback preserves timestamp unit/timezone/value/null semantics for the same timezone fixtures.
- DuckDB and Parquet sheets exactly match the executable mappings; conformance rejects a deliberately drifted sheet or an implementation that rejects a declared-lossless representative.
- Existing Postgres `TIMESTAMPTZ` behavior participates in the shared plan-time mapping conformance so the same canonical schema receives the same fidelity classification across all first-party destinations.
- Postgres persists complex/nested Arrow values through binary COPY as deterministic JSONB when the recorded contract allows the representation change; it does not reject an otherwise encodable value merely because no native relational type exists.
- Every first-party sheet is audited against the actual production encoder/scanner. Remaining `Unsupported` mappings name a proven engine/format gap and an operator remediation rather than a placeholder implementation omission.
- Focused tests, strict Clippy, formatting, product-level plan/run smokes for DuckDB and Parquet, and performance checks show no regression in the unchanged data path.

## References

- `.10x/specs/types-contracts-normalization.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/decisions/data-onramp-schema-discovery-reconciliation.md`
- `crates/cdf-contract/src/compiler.rs`
- `crates/cdf-project/src/runtime/planning.rs`
- `crates/cdf-dest-duckdb/src/{sheet,package,segment_scan,rows}.rs`
- `crates/cdf-dest-parquet/src/{sheet,package,runtime}.rs`
- `crates/cdf-package/src/parquet.rs`

## Assumptions

- Record-backed: Arrow timestamps with a timezone annotation carry UTC-epoch integer instants; the timezone string is schema meaning and MUST remain in CDF's canonical schema/evidence.
- Record-backed: DuckDB `TIMESTAMPTZ` stores microsecond UTC instants, and the pinned C API exposes `DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ`; the existing benchmark scanner already maps zoned Arrow timestamps to it.
- Record-backed: the native arrow-rs `ArrowWriter` is the selected Parquet authority and embeds Arrow schema metadata needed to reconstruct timezone annotations.
- User-ratified on 2026-07-21: implementation decisions needed to complete this repair may be self-ratified; the obvious lossless native paths are preferred over fail-closed placeholders.
- User-ratified standing constraint: performance may not regress; validation belongs at plan time and must not add per-row hot-path work.

## Journal

- 2026-07-21: Activated after the user confirmed systemic execution. The governing specs and decision were reread in full. Implementation will keep mapping validation at plan/schema-preflight time, preserve the sole native hot paths, and audit every registered first-party destination rather than only DuckDB and Parquet.
- 2026-07-21: Field reports showed DuckDB rejecting `Timestamp(Microsecond, Some(_))` behind a claimed ICU prerequisite and Parquet rejecting `Timestamp(Microsecond, Some("+00:00"))` despite both native storage engines having viable lossless paths.
- 2026-07-21: Source trace found that `DestinationSheet.type_mappings` is resolved for schema promotion but not for ordinary resource commit planning. `ResolvedProjectDestination::plan_resource_commit` delegates directly to adapter planning; generic recursive schema-to-sheet validation is absent.
- 2026-07-21: DuckDB's `field_plan`, scalar correction conversion, and production `LogicalType::from_arrow` reject all zoned timestamps. The benchmark table-function implementation in `cdf-benchmarks/src/references.rs` already uses `DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ`, proving the pinned stock-library mechanism is available without nanoarrow or ICU.
- 2026-07-21: `cdf-package::validate_parquet_schema` duplicates a small primitive whitelist, while the selected arrow-rs `ArrowWriter::try_new` is already the actual schema authority. The destination sheet repeats the same stale restriction and incorrectly marks all nested types unsupported.
- 2026-07-21: The DuckDB canonical segment scanner now committed negative, null, and positive microsecond instants carrying `UTC`, `+00:00`, and `America/Phoenix` annotations into three `TIMESTAMP WITH TIME ZONE` columns without loading ICU. `epoch_us` readback exactly matched every input instant. A paired test proves zoned nanoseconds still fail before mutation with compiled-coercion remediation.
- 2026-07-21: Audited DuckDB v1.5.4's pinned Arrow C importer rather than inferring support from prior CDF whitelists. The production scanner now exercises Null, view types, Date64, Decimal32/64/128, duration, YearMonth/DayTime interval, list views, sparse union, and dictionaries in addition to the existing primitive/nested path. Float16, Decimal256, dense union, run-end encoding, and zoned nanoseconds remain explicit gaps because the importer/engine cannot preserve them through the selected representation.
- 2026-07-21: Restored and completed Postgres JSONB conversion instead of deleting it. The binary COPY encoder and correction path use CDF's canonical Arrow-value JSON encoder, extended to dictionary, union, and run-end arrays. Complex types are allowance-gated because the relational type changes, while Arrow Null is exact because its entire value domain is SQL NULL.
- 2026-07-21: A lifecycle regression test exposed that ordinary `run` still bypassed the new generic mapping validator even though `plan` used it. Moved the validation behind `ResolvedProjectDestination::validate_output_schema_mappings` and invoked it from both entry points. The repaired run rejects an unallowed lossy Postgres mapping before source open and before package, state, or destination creation.
- 2026-07-21: Release-mode local TLC control over 3,514,289 April rows completed in `1.98s`, `1.91s`, and `1.76s` wall (median `1.91s`; median recorded run elapsed `1.828s`) with 18 canonical segments. This matches the retained `1.93s` same fixture/path observation and shows no hot-path regression from schema-time validation and additional type match arms.

## Blockers

None.

## Evidence

- Shared mapping authority: `cdf-contract` destination-mapping tests cover deterministic specificity, ambiguous-sheet rejection, nested field paths, constrained decimals, union modes, and allowance-gated loss. `cdf-project` proves `plan`/`run` parity, capability-sheet drift rejection, and no writes/source open on preflight failure.
- DuckDB executable fidelity: `canonical_segment_scan_preserves_zoned_timestamp_instants_without_icu` and `canonical_segment_scan_preserves_extended_native_arrow_types` commit through the sole production scanner and verify destination values/types. Sheet representative tests bind every declared lossless category through the executable type planner.
- Parquet executable fidelity: native writer preflight plus zoned/nested round trips prove schema and integer-value preservation; representative sheet coverage is checked against arrow-rs `ArrowWriter` construction.
- Postgres executable fidelity: `live_binary_copy_persists_complex_arrow_values_as_jsonb` starts a real local PostgreSQL server, writes nested Arrow through binary COPY, and verifies JSONB values and type. Scalar, timezone, allowance, and sheet/encoder parity tests cover the remaining categories.
- Full focused suites: `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-contract -p cdf-package -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres -p cdf-project --locked -j 12` passed 493 active tests; 9 benchmark/scheduled tests remained explicitly ignored.
- Static quality: strict all-target Clippy for the six affected crates passed with `-D warnings`; `cargo fmt --all -- --check` and `git diff --check` passed.
- Product barrier: `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 tools/product-smoke-matrix.sh` passed all 11 product scenarios across CLI, project, preview/run conformance, and Iceberg projection authority.
- Performance: optimized CLI build completed with the downloaded DuckDB runtime. Three fresh-state local TLC-to-DuckDB runs completed exact 3,514,289-row receipts in median `1.91s` wall, matching the retained `1.93s` path observation. Validation is schema-time only; unchanged existing types add no per-row operation.

## Review

Fresh-hat adversarial source review traced each sheet claim through planning, production encoding/scanning, correction handling, and readback. One significant lifecycle defect was found and repaired: `run` initially did not invoke the shared validator. One fidelity overstatement was corrected: Postgres Null is exact rather than allowance-gated. No destination identity leaked into generic orchestration, no legacy writer or fallback path was introduced, and the three destinations retain one production ingress path each. Verdict: pass after repairs. Residual risk is limited to Arrow combinations not instantiated by the representative matrices; native writer construction, recursive validation, and fail-before-mutation behavior contain that risk.

## Retrospective

The original failures persisted because destination sheets were descriptive data disconnected from ordinary execution, while adapters maintained separate handwritten type whitelists. Fixing only the two timestamp errors would have repeated that failure. The durable repair is one generic recursive sheet interpreter at the project boundary plus executable parity tests owned by each adapter. The main process lesson is that plan-only tests can conceal a run lifecycle bypass; every compiler-front-end validation needs an explicit pre-source-open run assertion. The performance lesson is to keep capability resolution at schema time and prove the unchanged data path with a retained product workload rather than treating more match arms as self-evidently free.
