Status: open
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md

# Make destination temporal fidelity truthful and executable

## Scope

Repair the cross-destination schema-fidelity boundary so canonical Arrow temporal types are validated once during plan compilation and every advertised first-party mapping is implemented by the destination hot path.

The bounded implementation owns:

- recursive plan-time validation of the normalized output schema against the selected destination sheet, including nested child types and the resource's recorded `allow_lossy_mapping` policy;
- DuckDB lossless ingestion of timezone-aware Arrow timestamps at second, millisecond, and microsecond resolution through the sole canonical segment scanner and `TIMESTAMPTZ`, without requiring ICU merely to store UTC instants;
- DuckDB correction/scalar paths required by the same declared mapping, or an explicit plan-time capability exclusion if a correction operation cannot preserve it;
- Parquet schema preflight based on the native arrow-rs writer's actual schema conversion rather than CDF's hand-maintained primitive whitelist;
- Parquet round-trip preservation of timestamp integer value, unit, timezone annotation, nulls, and nested placement through its embedded Arrow schema;
- truthful destination-sheet mappings and schema-preflight version bumps for both destinations;
- conformance proving that declared lossless mappings are accepted before mutation and round-trip exactly, while unsupported/lossy mappings fail during planning with field path, Arrow type, destination type, and actionable alternatives.

## Non-goals

- No ICU installation, runtime extension loading, or calendar/timezone display functions. DuckDB ICU remains relevant to calendar arithmetic and rendering diagnostics, not lossless storage of an already-zoned Arrow instant.
- No silent nanosecond truncation. Zoned DuckDB nanosecond timestamps remain unsupported unless the compiled schema-coercion program explicitly lowers them under `allow_lossy_mapping`; this ticket does not invent an adapter-local cast.
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

- 2026-07-21: Field reports showed DuckDB rejecting `Timestamp(Microsecond, Some(_))` behind a claimed ICU prerequisite and Parquet rejecting `Timestamp(Microsecond, Some("+00:00"))` despite both native storage engines having viable lossless paths.
- 2026-07-21: Source trace found that `DestinationSheet.type_mappings` is resolved for schema promotion but not for ordinary resource commit planning. `ResolvedProjectDestination::plan_resource_commit` delegates directly to adapter planning; generic recursive schema-to-sheet validation is absent.
- 2026-07-21: DuckDB's `field_plan`, scalar correction conversion, and production `LogicalType::from_arrow` reject all zoned timestamps. The benchmark table-function implementation in `cdf-benchmarks/src/references.rs` already uses `DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ`, proving the pinned stock-library mechanism is available without nanoarrow or ICU.
- 2026-07-21: `cdf-package::validate_parquet_schema` duplicates a small primitive whitelist, while the selected arrow-rs `ArrowWriter::try_new` is already the actual schema authority. The destination sheet repeats the same stale restriction and incorrectly marks all nested types unsupported.

## Blockers

None.

## Evidence

Pending implementation.

## Review

Pending implementation.

## Retrospective

Pending implementation.
