Status: done
Created: 2026-07-21
Updated: 2026-07-22
Parent: .10x/tickets/done/2026-07-21-p3-d18-duckdb-reference-adapter-closeout.md
Depends-On: .10x/tickets/done/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md

# P3 D18B: DuckDB sparse-wide projection

## Scope

Use complete, verified package statistics to omit provably all-null nullable user fields from
canonical IPC decode, Arrow-to-DuckDB conversion, and the `INSERT` projection while retaining the
complete destination table schema and exact null values. Implement this within the sole DuckDB
scanner/preparation boundary and use Arrow IPC projection rather than decoding discarded columns.

## Non-goals

No sampled/incomplete-statistics pruning, constant-value synthesis, schema removal, generic-runtime
DuckDB branch, package mutation, or second scanner.

## Acceptance Criteria

- Only complete package-wide `null_count == row_count` evidence bound to the verified package and
  schema may omit a nullable field; missing, incomplete, stale, overflowed, or inconsistent evidence
  retains the field.
- Target tables always contain the complete compiled destination schema and omitted fields read back
  as null for append, replace, and merge.
- `_cdf_package_row_ord`, `_cdf_row_key`, merge keys, correction/provenance fields, and nonnullable
  fields are never omitted.
- Projected IPC readers decode only retained field buffers; scanner schema/type authority remains
  exact and one-path.
- Controlled wide evidence materially improves wall time, CPU, memory, or spill with no more than
  3% TLC regression. Otherwise the implementation is deleted and the ticket closes with the
  measured no-action result.

## References

- `.10x/specs/typed-statistics-evidence.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/decisions/canonical-package-row-ord.md`
- `.10x/tickets/done/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md`

## Assumptions

- Record-backed: complete statistics may prove absence; sampled or incomplete statistics may not.
- User-ratified: optimize sparse-wide ingestion without weakening visible schema or values.

## Journal

- 2026-07-22: Began execution after D18A closure. The optimization must enter through verified
  package statistics, not a DuckDB-specific package reader or runtime branch. The intended seam is
  a conservative optional `BatchStats` fact on `VerifiedPackageAccess`, captured into final-binding
  authority only after the package is complete. DuckDB then owns projection planning and null
  synthesis inside its sole canonical-segment scanner. Missing statistics remain the current full
  scan; no default profile-generation tax is introduced without separate evidence.
- 2026-07-22: Implemented the candidate as one scanner path. A fully verified optional package
  aggregate is captured by generic final-binding authority; DuckDB validates exact field paths,
  Arrow types, total row counts, completeness, null counts, and canonical segment schema before
  constructing an Arrow IPC projection. Only complete all-null nullable user fields may disappear
  from decode/conversion; merge keys, nonnullable fields, every reserved `_cdf_*` field, and the
  package ordinal remain physical. Target DDL remains complete, while omitted fields are absent
  from the INSERT column list so DuckDB applies their nullable default without materializing typed
  NULL vectors. Missing profile evidence retains the preexisting full scan.
- 2026-07-22: The affected suite passes: 50 DuckDB tests, 82 package tests with four deliberate
  performance ignores, 10 package-contract tests, 148 runtime tests with two deliberate ignores,
  seven build-graph tests, and doc tests. New tests prove manifest-bound aggregate reconstruction,
  conservative absence, exact IPC projection, protected framework/merge fields, stale/incomplete
  handling, and complete append/replace/merge target semantics. Created a benchmark-only profiled
  variant of the exact D18A package from verified canonical bytes without source re-extraction:
  package hash `sha256:5ca00b991ce2e5a5a8dd32a69880be458d2465a25684e9f29c588cb7c35ecde7`,
  3,513,266 rows, 231 unchanged canonical segments, and a 1.5 MiB typed profile. The temporary
  generator source was deleted after the artifact was verified; it is not a product or legacy path.
- 2026-07-22: Falsified the first SQL shape on the controlled EC2 host before spending the full
  median-of-three cell. IPC projection correctly removed 2,012 of 2,053 complete all-null package
  fields from Arrow decode, but the first draft reconstructed those fields as explicit typed NULL
  expressions in the insert. Its first sample remained in the same order of magnitude as the
  approximately 203-second baseline and still produced multi-gigabyte DuckDB spill. Stopped the
  remaining samples and removed the sink-side work as well: omitted nullable fields are now absent
  from the INSERT column list, preserving the same visible NULL values through ordinary nullable
  defaults.
- 2026-07-22: The retained shape completed the controlled wide median-of-three at
  `180.180250924s` (`166.041655ms` MAD), versus D18A's `203.280955s`: an 11.364% wall-time
  improvement with exact 3,513,266 rows and package/schema/byte authority. TLC completed at
  `10.151752277s` versus `10.145533317s`, a 0.061% delta inside noise and far below the 3% gate.
  The wide cell's maximum child RSS was 5,111,205,888 bytes versus D18A's 4,546,355,200 bytes and
  the cgroup peak was 13,040,189,440 bytes under a 16 GiB ceiling with no pressure or OOM event.
  The wall win is retained; the wide physical-table sink remains DuckDB's dominant memory floor.
- 2026-07-22: Closed the final semantic edge before review. A pre-existing append target may define
  a non-null column default even when its column is nullable. Omitted proven-null fields now stay
  absent from ordinary CDF-owned INSERTs, but fields with an existing destination default receive
  an explicit typed NULL so the default cannot leak into persisted values. A focused integration
  test proves this against a real DuckDB target.
- 2026-07-22: Adversarial review found that structural profile validation did not yet prove the
  package aggregate agreed with every segment aggregate. Repaired the generic package-statistics
  authority: package statistics are exposed only after recomputing the aggregate from every
  manifest-bound segment statistic and requiring exact equality. A forged profile that claims an
  all-null package column over a non-null segment now fails before final binding or destination
  mutation.

## Blockers

None.

## Evidence

- `.10x/evidence/.storage/2026-07-22-p3-d18b/d18b-wide-profiled-v2-median3.json` and adjacent
  request, run-cell, and systemd log: revision `a363027e`, controlled c7i.4xlarge median-of-three,
  exact package/schema/row/physical-byte authority, 11.364% faster than D18A, no cgroup event.
- `.10x/evidence/.storage/2026-07-22-p3-d18b/d18b-tlc-control-median3.json` and adjacent request,
  run-cell, and systemd log: exact 41,169,720-row TLC control at 10.151752277 seconds, 0.061% above
  the D18A baseline and therefore within the 3% no-regression gate.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-package-contract -p cdf-package
  -p cdf-runtime -p cdf-dest-duckdb --locked -j 12`: 10 package-contract, 83 package, 148
  runtime, 51 DuckDB, eight build-graph/doc tests passed; six deliberate performance tests ignored.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo clippy -p cdf-dest-duckdb --all-targets
  --locked -j 12 -- -D warnings`, formatting, and `git diff --check` passed. `graphify update .`
  refreshed the architecture graph after the implementation.

## Review

Fresh adversarial review initially failed closure on one critical issue: independently supplied
package-grain statistics could disagree with segment-grain evidence, allowing unsafe omission. The
generic package reader now derives and exactly reconciles the segment aggregate before exposing the
package aggregate; `verified_package_statistics_reject_aggregate_drift_from_segments` proves the
failure mode. The reviewer reported no other blocker and judged the existing-destination default
handling sound. Closure verdict after the focused repair and complete affected suite: pass.

Accepted residuals: the final binding retains schema-width `BatchStats` without a separate memory
lease, and the pathological wide cell's maximum child RSS rose 12.424% while wall time improved
11.364%. Both are bounded and disclosed; the 16 GiB cgroup recorded no pressure or OOM. The user
explicitly chose to retain statistics-driven pruning and to stop further DuckDB wide-table tuning.

## Retrospective

The optimization became useful only when omission crossed both boundaries: Arrow IPC decode and the
DuckDB INSERT column list. Synthesizing explicit NULL vectors preserved values but left the sink
cost largely intact. More importantly, manifest membership and profile shape were not sufficient
semantic authority for an aggregate optimization; derived evidence must reconcile with its source
grains before it can authorize data elimination. Future destinations can reuse the verified package
aggregate, but must keep omission policy destination-owned and preserve exact defaults, keys,
provenance, schema, and null semantics.
