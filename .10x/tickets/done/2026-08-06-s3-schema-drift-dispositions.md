Status: done
Created: 2026-08-06
Updated: 2026-08-06
Parent: `.10x/tickets/done/2026-08-06-state-backed-schema-authority-program.md`
Depends-On: `.10x/tickets/done/2026-08-06-s1-state-schema-authority-foundation.md`

# S3 total schema drift dispositions

## Scope

Replace coupled evolution/quarantine policy with the ratified total admission model across contract,
declarative/project lowering, engine/runtime, packages, destinations, reports, and supported-source
conformance:

- delete `SchemaEvolutionMode::Evolve|Freeze`, automatic output widening, duplicate allow-new/
  unknown flags, and `quarantine.enabled`;
- introduce typed field/row/record/partition dispositions and shared evidence redaction policy;
- encode the exact Experimental/Governed/Financial/Serving presets;
- replace `control_critical` as decision authority with compiler-derived ordinary/required/
  identity/progress/CDC-operation/transaction roles and allowed action sets;
- preserve the exact `_cdf_variant` codec and make variant presence plan/schema-visible;
- keep accepted-with-residual distinct from quarantine in packages, receipts, checkpoints,
  telemetry, human output, and JSON;
- prove quarantine-only durable settlement before advancement;
- make plan show observed drift, disposition, wider-fetch cost, and no source-driven migration;
- retain source-specific isolation/advancement proofs behind adapter contracts.

## Non-goals

- changing active schema heads or implementing promotion;
- inventing a new variant encoding or storing raw residual values in state;
- destination-specific admission semantics in generic runtime;
- nested promotion, automatic DDL, or business-key correction;
- weakening source-position/CDC/transaction correctness to keep a run alive.

## Acceptance criteria

1. Current policy serialization contains no evolution mode, global quarantine switch, or impossible
   quarantine-verdict/disabled-mechanism combination.
2. Every relevant observation compiles to one typed total disposition at enforceable grain.
3. Four trust presets exactly match the ratified matrix, including Financial fail strictness and
   Serving's separately governed sampled fast path.
4. Field roles prevent variant/quarantine when identity/progress/operation/transaction correctness
   cannot be preserved.
5. Exact variant bytes, non-reconstructable redaction metadata, multi-path envelopes, and nulling
   behavior remain canonical.
6. Main accepted rows, variant rows, quarantined evidence, failed resources, receipt/checkpoint
   outcomes, and schema-head immutability are asserted for each supported source family.
7. Quarantine-only input cannot advance until canonical evidence and an empty/metadata settlement
   proof are durable.
8. Plan/run typed reports show exact dispositions/counts without double-counting or secret leakage.
9. Affected fmt/check/focused tests/strict Clippy and behavioral conformance pass.

## References

- `.10x/decisions/state-backed-schema-authority.md`
- `.10x/specs/schema-drift-dispositions.md`
- `.10x/specs/residual-variant-capture.md`
- `.10x/specs/schema-discovery-and-stream-admission.md`
- `.10x/specs/types-contracts-normalization.md` (only clauses not superseded by the drift spec)
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/knowledge/cli-report-authority.md`

## Assumptions

- User-ratified: exact four preset matrix in the governing spec.
- User-ratified: there is no unlocked/evolving established state and no global quarantine toggle.
- Record-backed: missing nullable remains typed null and compiled lossless coercion is not drift.
- Record-backed: control/source-advancement safety outranks row availability.

## Journal

- 2026-08-06: Opened dependency-gated behind S1 and independent of S2 after the shared types land.
- 2026-08-06: Activated after S1 and S2 closed. Read the complete ticket and governing decision,
  drift/admission/residual/checkpoint/report records, plus the error-ownership and CLI-report audit
  skills. The immutable-head oracle is now present; implementation starts from current policy and
  validation-program authority rather than adding a parallel runtime policy.
- 2026-08-06: Replaced evolution/verdict/quarantine switches with total typed admission at field,
  row, record, and partition grain. The compiler now records ordinary, required-output,
  destination-identity, source-progress, CDC-operation, and transaction-boundary roles plus each
  field's allowed and selected disposition. Identity/progress facts fail closed.
- 2026-08-06: Kept active schema heads immutable during planning and execution. Active state is an
  explicit runtime schema source; compatible observations reconcile without changing the head,
  unknown values remain residuals when allowed, and incompatible partitions quarantine or fail
  according to the compiled policy.
- 2026-08-06: Renamed package evidence to `schema/admission-evidence.json`, preserved the canonical
  residual codec, made `_cdf_variant` plan/schema-visible, and added exact accepted-clean,
  accepted-with-residual, quarantined-row, and quarantined-partition report counts. Isolated-worker
  evidence carries and validates the same typed summary.
- 2026-08-06: Final accounting review found that watermark recapture/quarantine happened after
  contract admission and could leave withheld rows in the new main-row count. The phase boundary
  now subtracts withheld and residual-withheld rows before publication and rejects an invalid
  residual-subset invariant.
- 2026-08-06: Updated active REST/Postgres/SQLite source execution authority and diagnostics,
  retained durable zero-segment/quarantine settlement, and removed the obsolete kernel contract
  policy module and current product vocabulary.

## Blockers

None.

## Evidence

1. Policy serialization and presets: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-contract --lib`
   passed 101 tests with two release-performance tests ignored. Assertions cover the exact four
   preset shapes, absence of removed switches, total disposition serialization, and field-role
   fail-closed behavior.
2. Compiled/runtime admission: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-engine
   tests::schema_admission::` passed 23/23. It covers lossless admission, unknown/incompatible
   observations, missing identity fields, terminal quarantine attestation, replay verification,
   and unchanged fixed authority.
3. Residual, quarantine, and package evidence: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-engine
   tests::package_evidence::` passed 26/26; `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-package --lib`
   passed 94 tests with four performance tests ignored. Exact variant bytes, redaction,
   quarantine artifacts, admission-evidence versions, and package verification are covered.
4. Active project behavior: the focused CLI tests
   `active_multi_file_parquet_keeps_fixed_schema_and_admits_new_physical_schemas_in_stream` and
   `run_rest_progress_drift_fails_closed_without_parse_coercion` passed. The former proves a fixed
   active schema, plan-visible `_cdf_variant`, disjoint 2 clean/2 residual/0 quarantine counts, and
   no source-driven migration; the latter proves progress drift cannot be coerced or hidden.
5. Settlement/checkpoint behavior: the focused project test
   `zero_segment_processed_package_recovers_after_receipt_without_source_or_data_mutation` passed,
   proving quarantine-only/empty settlement does not advance before a durable receipt and recovery
   commits without rereading input. The DuckDB drift-quarantine conformance test passed with the
   current typed disposition evidence.
6. Cross-phase counts: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-engine
   tests::retry_drain::late_rows_are_quarantined_or_admitted_with_identity_evidence` passed for
   quarantine, next-epoch recapture, and admitted annotation; accepted telemetry exactly equals
   rows retained in the main package.
7. Affected source adapters: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-source-postgres -p
   cdf-source-sqlite -p cdf-source-rest --lib` passed 59 tests with two live/performance tests
   ignored. A focused Postgres authority test additionally proves active and discovered schema
   sources are admitted while unprepared modes fail with the current diagnostic.
8. Build and quality: `DUCKDB_DOWNLOAD_LIB=1 cargo check --workspace --all-targets` passed. Strict
   affected-package Clippy passed with `-D warnings`; a separate cognitive-complexity diagnostic
   reported only existing hotspots and no new admission/reporting blocker. `cargo fmt --all` and
   `git diff --check` passed.

Limits: external live Postgres is intentionally not configured; its one environment-backed test
remained ignored. The program-wide release build, sandbox matrix, and independent red-team review
remain owned by S6 under the user's explicit tranche-level review policy.

## Review

Focused self-review passed. It traced policy serialization through compiler roles, engine
admission, worker merge, package verification, project reporting, and CLI JSON/human rendering;
searched current product code for removed evolution/verdict/quarantine authority; and corrected the
watermark post-admission accounting gap found during that audit. No unresolved finding remains.
Independent red-team review is explicitly deferred to S6, where the user requested the single
tranche-wide review.

## Retrospective

- The old Boolean/control-verdict model duplicated decisions at too many layers. Carrying one typed
  admission policy plus compiler-derived field roles removed conditionals and made fail-closed
  behavior inspectable in plans, packages, and worker evidence.
- An active state head is neither a declaration nor a filesystem snapshot. Naming it explicitly in
  `SchemaSource` avoided a fake path, another discovery branch, and misleading adapter errors.
- Outcome counts must be adjusted at the last phase that can still remove main rows. Computing
  residual counts only during contract evaluation was insufficient because watermark policy runs
  later; the publication invariant now catches this class of phase-ordering mistake.
- Exact artifact/version tests and one real multi-file CLI journey gave faster, stronger feedback
  than a broad suite at this child boundary. Preserve the full matrix for S6.
