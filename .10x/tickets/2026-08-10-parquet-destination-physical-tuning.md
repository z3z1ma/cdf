Status: done
Created: 2026-08-10
Updated: 2026-08-10

# Parquet destination physical tuning authority

## Scope

Replace universal Parquet object-layout constants with validated destination URI options for target
package bytes and maximum segments per object, retaining the measured 256 MiB/eight-segment
defaults. Bind resolved values into the prepared physical plan, staging metadata, receipt/replay
authority, and deterministic object layout. Make writer declarations mean safe maxima rather than
an unproven universal performance plateau.

## Non-goals

- Resource-level destination overrides before a shared destination-option surface exists.
- Changing the measured default compression, row-group settings, receipt semantics, or package identity.
- Compatibility parsing for old pre-release metadata.

## Acceptance Criteria

- [x] `parquet://` accepts exactly validated `compression`, `object_target_bytes`, and
      `max_segments_per_object` options with current measured defaults when omitted.
- [x] Layout settings are recorded before mutation and replay never consults ambient defaults.
- [x] Deterministic grouping honors both configured bounds, including oversized singleton objects.
- [x] Writer concurrency is host/jobs/memory admitted and no field claims an unmeasured useful plateau.
- [x] Focused behavioral tests, formatting, check, and strict affected-package Clippy pass.

## References

- `.10x/decisions/schema-planned-destination-bulk-paths.md`
- `.10x/decisions/injected-execution-host-runtime-ownership.md`
- `.10x/tickets/done/2026-07-26-parquet-parallel-object-encoding.md`

## Assumptions

- The user explicitly ratified both destination-level layout controls identified by the audit.
- Defaults remain 256 MiB and eight segments because those are the current measured production path.

## Journal

- 2026-08-10: Opened from the user-authorized destination audit repair.
- 2026-08-10: Replaced the ambient `ParquetObjectLayoutPolicy::current()` path with one validated
  destination-owned policy. `parquet://` now resolves compression, target object bytes, and the
  maximum segment count independently and order-independently; diagnostics reject malformed,
  duplicate, zero, overflow, fragment, unknown, and authority-shaped credential input without
  echoing supplied values.
- 2026-08-10: Bumped the current-only physical plan to version 7, staging/publication metadata to
  version 2, and immutable object manifests to version 5. Exact layout policy now flows through
  runtime construction, bulk preparation, staged grouping, pre-mutation metadata with exact
  readback, publication recovery metadata, immutable manifests, receipt transaction/verification
  parameters, and receipt verification. Old pre-release shapes are not read.
- 2026-08-10: Duplicate replay now verifies and returns the immutable manifest's recorded physical
  authority before preparing a new reachability root. A changed current runtime default can
  therefore replay the same logical package without requiring its newly encoded physical objects
  to match the prior nonidentity tuning choice; unused new claims are structurally cleaned.
- 2026-08-10: Error-ownership audit classified URI option failures as caller-repairable Contract
  errors and malformed persisted layout authority as durable-destination failures. No credential
  or option value is interpolated into the new URI diagnostics.
- 2026-08-10: Focused verification passed: all 50 runnable Parquet library tests (one explicit
  release roofline ignored), the three URI tests, the recorded-policy duplicate-replay test,
  affected library check, strict all-target no-deps Clippy, the explicit cognitive-complexity
  diagnostic, package formatting, and diff whitespace validation. Strict Clippy including
  dependencies was attempted but stopped on a concurrent PostgreSQL CDC `too_many_arguments`
  finding outside this ticket; the Parquet-only strict target passed.
- 2026-08-10: Added the current-model Parquet destination operator guide and linked it from both
  documentation indexes. It documents the exact URI surface and defaults, deterministic grouping
  including oversized singleton objects, the distinction between package-byte object grouping and
  encoding batch/row-group controls, host/jobs/memory writer admission, and recorded replay
  authority without inventing resource overrides or new flags.

## Blockers

None.

## Evidence

- URI/default/option authority: `parquet_destination_uri_compiles_compression_into_the_physical_path`,
  `parquet_destination_uri_compiles_layout_options_independently_and_in_any_order`, and
  `parquet_destination_uri_rejects_every_invalid_option_without_echoing_credentials` passed.
- Layout and replay authority: `layout_is_deterministic_and_bounds_non_oversized_groups`,
  `oversized_segment_is_a_singleton_without_weakening_later_groups`,
  `default_layout_groups_seventeen_canonical_segments_as_eight_eight_one`,
  `staged_attempt_records_the_exact_prepared_physical_plan`, and
  `nondefault_layout_round_trips_through_metadata_receipt_and_duplicate_replay` passed.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-parquet --lib`: passed 50, failed 0, ignored the one
  explicit release-only roofline benchmark.
- `DUCKDB_DOWNLOAD_LIB=1 cargo check -p cdf-dest-parquet --lib`: passed.
- `DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-dest-parquet --all-targets --no-deps -- -D warnings`:
  passed. `DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-dest-parquet --lib --no-deps -- -W
  clippy::cognitive_complexity`: passed without a finding.
- `cargo fmt -p cdf-dest-parquet` and scoped `git diff --check`: passed.
- `docs/operators/parquet-destination.md` is linked from `docs/operators/README.md` and
  `docs/README.md`; all relative links in the new page were manually resolved to existing files,
  and scoped documentation/ticket `git diff --check` passed.

## Review

The independent tranche-level red team re-read the integrated destination diff and returned
`pass` with no actionable findings. The release-only Parquet roofline was not rerun because this
ticket preserves the measured defaults; a future default change still requires new measurement.

## Retrospective

The old staging record already serialized two layout numbers, but production reconstructed them
from constants and treated the record as cleanup-only data. Making the policy a first-class value
exposed the more important nonidentity replay law: immutable package/token authority must win over
whatever equivalent physical tuning happens to be selected for a later attempt. Checking the
existing verified manifest before creating the new reachability root both preserves that law and
removes an unnecessary equality requirement between equivalent Parquet encodings. Keeping the
defaults unchanged avoided speculative performance work; future default changes still require a
new measured physical-plan version.
