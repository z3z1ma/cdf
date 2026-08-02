Status: active
Created: 2026-08-02
Updated: 2026-08-02
Parent: .10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md

# SQLite destination connector

## Scope

Implement and ship `cdf-dest-sqlite` with append, atomic replace, merge, package-token
idempotency, typed SQL mirrors, independently verifiable receipts, live crash coverage, operator
documentation, and a release-mode direct-`rusqlite` destination roofline cell. Reconcile any exact
lower SQLite protocol duplication with the source without creating a universal database wrapper.

## Non-goals

State-store reuse, parallel writers, implicit WAL changes, network filesystems, cross-attached-file
atomicity, CDC application, and arbitrary SQL.

## Acceptance Criteria

- The sheet, planner, runtime, mapping, transaction, provenance, mirror, and verification behavior
  implement `.10x/specs/sqlite-destination.md`.
- Append/replace/merge, duplicate package, zero rows, merge-key conflicts, schema incompatibility,
  journal/durability preservation, and crash-before/after-commit have unit and live tests.
- One transaction covers target and mirror mutations; a fresh connection independently verifies
  receipts before checkpoint commit.
- Built-in catalog integrity, generic destination/product/chaos/jobs laws, and
  `tools/certify-connector.py --kind destination --id sqlite --core-impact` pass.
- The destination macro benchmark records raw samples and reaches the 0.90 direct-`rusqlite`
  roofline under the same durability and journal mode.
- Independent review passes after any closure repair.

## References

- `.10x/specs/sqlite-destination.md`
- `.10x/specs/database-connector-roofline.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/destination-common-services.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `docs/connector-authoring.md`

## Assumptions

- Append/atomic replace/merge and the 90% roofline are user-ratified.
- SQLite's single-writer boundary is protocol authority, not a performance defect.

## Journal

- 2026-08-02: Ticket opened; execution waits for SQLite source closure.
- 2026-08-02: Source implementation dependency is satisfied: focused correctness, affected-package
  checks, exact error audit, and independent repair re-review pass. The source's fresh roofline and
  workspace certificate are parent-owned final integration gates under the user-ratified reduced
  validation cadence and do not block this paired destination implementation.
- 2026-08-02: Destination execution started. Read this complete ticket, its parent, every direct
  active authority, the transitive destination ingress/runtime/performance/concurrency authorities,
  the error-ownership taxonomy, and the mandatory rusqlite audit procedure. The executable boundary
  is fixed: one finalized-package, run-owned, non-parallel SQLite writer; one native transaction for
  payload, compact provenance, and typed SQL mirrors; generic receipt assembly and checkpoint
  ordering remain shared; verification opens a fresh connection; no generic database wrapper or
  destination-identity branch is permitted. Validation is limited to the user-ratified focused
  gates and one bounded roofline; the parent owns the final workspace/core-impact certificate.
- 2026-08-02: Implemented `cdf-dest-sqlite` as a dependency-isolated finalized-package adapter.
  The crate is split by authority (`runtime`, `sheet`, `plan`, `package`, `mapping`, `transaction`,
  `mirrors`, `receipts`, `error`, `identifier`, and models) rather than by generic database layers.
  It owns one `BEGIN IMMEDIATE` connection on the injected pinned blocking lane; append, replace,
  and fail-on-ambiguity merge write target rows, compact row provenance, quarantine, load, state,
  and segment mirrors in that transaction. It preserves the file's observed journal/durability
  settings and has no implicit WAL, pool, private runtime, or destination-identity branch.
- 2026-08-02: Closed adversarial verification gaps found during execution. Fresh verification now
  opens read-only and re-reads the exact receipt, mapped target schema, exactly one state row bound
  to receipt/package/schema/timestamp, order-independent quarantine evidence, segment ranges, and
  target row coverage. Existing targets require exact type/nullability and always receive a
  deterministic unique partial provenance index. Empty packages intentionally commit only mirrors
  and therefore skip target-schema verification because no target is created.
- 2026-08-02: Enrolled SQLite through the built-in data-driven destination catalog and the
  conformance destination/runtime-chaos catalogs. Added exact inspection fixture hash, local
  payload/footprint/replay verification, runtime-chaos shard coverage, operator documentation, and
  the release-only direct-rusqlite destination roofline harness. Shared identifier validation was
  extended only at the existing SQL identifier authority; no universal database wrapper was added.
- 2026-08-02: Completed the mandatory error-ownership audit using the exact 13-file Rust manifest.
  The durable ledger, subsequently refreshed to 119 matches (118 actual CDF kind sites plus one
  `io::ErrorKind` false positive),
  is `.10x/evidence/.storage/2026-08-02-sqlite-destination-error-ownership-ledger.md`. Embedded
  typed errors retain kind/retry metadata; host open/resource failures are Environment, durable
  SQLite corruption/shape failures are Destination, finalized-package contradictions are Data,
  invalid configuration is Contract, contention is Transient, and only CDF-owned invariants are
  Internal.
- 2026-08-02: Reconciled `Cargo.lock` to exactly 23 connector-related added lines after an offline
  regeneration tried to upgrade/unify unrelated compatible transitive dependencies. The final diff
  contains only three `cdf-dest-sqlite` dependency edges and the new workspace package stanza.
- 2026-08-02: Final focused validation: affected-package all-target/all-feature check passed after
  repairing the provenance-index hex formatting and importing/asserting `DestinationProtocol` in
  the benchmark; strict all-target/all-feature Clippy passed with `-D warnings`; the final leaf run
  was 12/13 because the new verifier incorrectly required a zero-row target, then the explicitly
  authorized exact repair test passed 1/1; built-in catalog fixture passed 1/1; conformance
  destination-catalog slice passed 9/9; runtime-chaos catalog coverage passed 1/1; formatting and
  `git diff --check` passed. The 13-test suite was not rerun again under the ratified cadence.
- 2026-08-02: The first release roofline command stopped at link time because the benchmark crate's
  unrelated DuckDB dependency required `DUCKDB_DOWNLOAD_LIB=1`. The one authorized corrected
  invocation observed all five direct-rusqlite 1,000,000-row samples (median 83,769,158 useful
  bytes/s, 286,501,625 ns, MAD 1,813,917 ns) but admitted zero CDF samples: the parent host
  fingerprint was taken while the storage target was absent, then changed after the direct child
  created it. The raw report at
  `.10x/evidence/.storage/2026-08-02-sqlite-destination-roofline.json` is honestly
  `inconclusive`, ratio 0, not performance closure evidence. The harness now creates/configures the
  storage target before fingerprinting so a future explicitly authorized run has a stable host
  class; no retry or additional gate was run after that repair.
- 2026-08-02: Closure repair diagnosed the exact `file/sqlite/append` matrix abort before
  destination mutation. `cdf-project` correctly derived the live column policy from the SQLite
  destination sheet, but `cdf-contract`'s existing exact destination-rule adapter recognized only
  DuckDB namecase and Postgres quoted rules. Added the explicit
  `namecase-v1/sqlite-quoted-v1`/allowed-pattern arm at that normalization authority, preserving
  namecase-v1 live normalization, the SQLite 255-byte cap, collision behavior, and strict rejection
  of unknown patterns. No generic destination identity branch and no preflight relaxation was
  introduced. The exact focused assertion passed 1/1, and strict all-target/all-feature Clippy for
  `cdf-contract`, `cdf-project`, `cdf-conformance`, and `cdf-dest-sqlite` passed. Per orchestration
  direction, the matrix and roofline were not rerun. The error ledger records this outside-root
  supporting boundary without changing its frozen counts. Roofline content identity is unaffected:
  the destination roofline starts from an already finalized package and does not execute project
  live-column normalization.
- 2026-08-02: A second exact matrix repair found that the SQLite sheet used human documentation
  groups such as `Int8|Int16|Int32|Int64` and `Date|Time|Duration|Timestamp`; the shared compiler
  intentionally recognizes only its canonical mapping-pattern vocabulary, so it truthfully
  reported no declared `Int64` mapping before destination contact. Replaced those labels with
  executable declarations for every supported scalar, decimal, binary, temporal, duration, and
  interval family. Each declared supported mapping now resolves losslessly to the exact physical
  SQLite storage class returned by the mapper. `Null` is explicitly unsupported because the sheet
  cannot condition support on field nullability and a non-nullable Arrow Null field would bind SQL
  NULL; container/union/dictionary/run-end families remain explicitly unsupported. Added one
  sheet-to-mapper parity assertion covering every physical match arm, both nullable states, all
  unsupported families, the exact pattern inventory, and the sheet artifact hash. It passed 1/1;
  affected strict all-target/all-feature Clippy for `cdf-dest-sqlite`, built-ins, and conformance
  passed. Updated the built-in inspection fixture to
  `sha256:3cfa8b0f27d36820d82573163b855ea827693712d88ba081b78c520080728ed9` and the
  exact error-ledger line locations; counts are unchanged. The existing roofline content manifest
  already includes `mapping.rs`, the crate manifest, and `Cargo.lock`, so a future run will bind
  this repair without changing the manifest code. The prior inconclusive JSON remains correctly
  bound to pre-repair content. Matrix and roofline were not rerun.
- 2026-08-02: A third exact matrix repair preserved the reserved `_cdf_*` user-identifier fence
  while admitting only CDF's governed `_cdf_variant` package field. SQLite schema mapping now
  delegates ownership to `cdf_contract::is_framework_variant_field`, whose exact contract includes
  the name, nullable UTF-8 type, residual semantic, and `residual-json-v1` encoding metadata. Only
  that field enters the crate-private system-identifier path; schema/package revalidation persists
  and recomputes the ownership bit, and planning rejects any impossible internally flagged name as
  Internal. The focused assertion exercised the positive plan path plus seven missing/wrong
  metadata, type, nullability, encoding, and reserved-name impostors and passed 1/1. Strict
  all-target/all-feature Clippy passed for `cdf-dest-sqlite`, built-ins, conformance, and benchmarks
  with `-D warnings`. The error ledger now reproduces 119 syntactic/118 actual sites, and the
  roofline content manifest includes the contract policy/residual sources defining the exact
  classifier. The sheet artifact hash remains unchanged. Matrix and roofline were not rerun.
- 2026-08-02: A fourth exact matrix repair moved the generic target generator from
  `{source}_events_{disposition}` to the stable cross-destination-safe
  `cdf_{source}_events_{disposition}` namespace. This removes SQLite source identities from the
  engine-reserved `sqlite_` object prefix without weakening SQLite validation or branching on a
  destination identity. The exact assertion enumerates every registered source/destination/
  disposition cell, verifies the stable spelling and non-reserved namespace, constructs a
  `TargetName`, and round-trips the result through each destination sheet's published identifier
  rules. The first focused invocation compiled but could not link because it omitted the required
  `DUCKDB_DOWNLOAD_LIB=1`; the corrected invocation passed 1/1. Strict all-target/all-feature
  Clippy for `cdf-conformance` passed with `-D warnings`. No static dynamic-evidence fixture needed
  an update, and the matrix and roofline were not rerun.
- 2026-08-02: Root orchestration ran the exact post-repair destination matrix and observed a clean
  pass: 18/18 executed cells across file, Python, REST, Postgres, SQLite, and Nebula sources, each
  under append, replace, and merge; there were zero exclusions. The enclosing test passed 1/1 in
  19.99 seconds. This closes the SQLite matrix blocker on current source without weakening any
  destination rule or excluding a cell.

- 2026-08-02 closure repair: separated immutable per-receipt commit evidence and typed state
  history from mutable current target/state authority. Zero-row replace now empties an existing
  target transactionally; historical append/replace/overlapping-merge receipts remain verifiable
  and idempotent without requiring their old rows to remain current.
- 2026-08-02 closure repair: fresh verification now decodes and compares the exact stored receipt,
  commit-evidence hash, complete segment JSON plus scalar row ranges and acknowledgements, typed
  state identity/position lineage, target schema, exact unique partial provenance index, and a
  sorted multiplicity-preserving SHA-256 quarantine multiset commitment. Focused corruption tests
  falsify valid same-count quarantine substitution, full segment JSON changes, typed state-history
  changes, and provenance-index removal.
- 2026-08-02 closure repair: invocation-local `RunCancellation` now flows through
  `ExecutionServices`, session phases, row writing, merge, mirrors, and fresh verification. The
  production connection installs a SQLite VM progress handler; focused tests prove pre-consumption
  cancellation and VM interruption.
- 2026-08-02 closure repair: Float16/32/64 now persist canonical big-endian IEEE-754 bit-pattern
  `BLOB`s and publish the canonical SQLite `BLOB` storage class with lossless fidelity. Tests cover
  NaN payloads and signed zero. Error ownership now distinguishes payload NOT NULL/datatype
  contradictions (`Data`), durable target constraints and missing receipt databases
  (`Destination`), host open/metadata failures (`Environment`), and injected cancellation
  (`Internal`) without flattening embedded typed errors.
- 2026-08-02 closure repair: subprocess failpoints now terminate during payload and mirror mutation
  before COMMIT and prove rollback/replay recovery. The roofline harness now places statement
  preparation inside both timed delivery regions, serializes full receipt/state/segment/evidence
  mirrors on the direct path, and invokes the production verifier; the roofline itself was not
  rerun. The former 1,399-line transaction authority is now an 18-line facade over acyclic
  `session.rs` (540 lines), `verifier.rs` (347), and `writer.rs` (559).
- 2026-08-02 verification: the SQLite leaf suite ran once and reported 20/22 passing. Its two
  focused assertion defects were repaired without rerunning the other 20: the direct error-owner
  assertion then passed 1/1, and sheet/physical mapping parity passed 1/1. The exact built-in
  catalog assertion passed 1/1 after deterministic fixture alignment. Final sheet artifact hash is
  `sha256:752ca09c24e025ce0382d99760be03336b914b1df50cbd7230b203fd8e930fb7`;
  final SQLite inspection hash is
  `sha256:1c61a59ab6759c477623db5cbddcb2ee3bdeda19855601b39106db771a93bcd8`.
- 2026-08-02 verification: affected all-target/all-feature `cargo check` passed for runtime,
  contract, dest-sql, dest-sqlite, builtin-drivers, conformance, and benchmarks (with transitive CLI
  and DuckDB). Strict all-target/all-feature Clippy passed with `-D warnings`.
- 2026-08-02 root-observed verification: the exact SQLite runtime-chaos test passed 1/1 in 6.43s.
  All four crash windows passed: package replay verified before destination write; checkpoint
  proposed before destination write; destination receipt recorded and verified before checkpoint
  commit; checkpoint committed before package status became checkpointed. In every case recovery
  required no source contact, checkpoint state never advanced ahead of durable destination data,
  receipt recovery avoided a second destination write, and duplicate retry performed no second
  write.
- 2026-08-02 benchmark-boundary repair: independent re-review found that the direct cell alone
  timed CDF planner/receipt/mirror-artifact preparation and that only direct included the final
  target-count check. `direct_commit_artifacts`, receipt JSON serialization, and SQLite row-count
  conversion now complete before the direct transaction/timer boundary, matching CDF's untimed
  planning and prepared-session setup. Both workers now enter one shared `finish_timed_commit`
  boundary, which runs the production fresh verifier and identical target-count verification
  before capturing elapsed time. The comparability key's timed-region version is bumped from 1 to
  2, and the report describes the revised boundary without claiming a new measurement.
- 2026-08-02 benchmark-boundary verification: `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo
  check -p cdf-benchmarks --all-targets --all-features --locked -j 12` passed, followed by the
  identical package/target/feature selection under `cargo clippy ... -- -D warnings`; both gates
  finished cleanly. Per orchestration direction, no roofline, matrix, chaos, leaf, workspace, or
  certificate command ran. The workspace-content manifest already includes the changed benchmark
  source, so its input list needs no edit and a future run will derive a new hash. The error ledger
  is unchanged because its frozen scope is `crates/cdf-dest-sqlite/**/*.rs`.

## Blockers

Closure-repair handoff blockers (2026-08-02):

- Independent re-review must attempt to falsify the repaired implementation and record a new
  verdict. The prior independent verdict remains historical evidence for the repair scope.
- The SQLite roofline rerun and full destination certificate are intentionally parent-deferred;
  they are not execution blockers for this bounded closure-repair handoff.

This dated blocker statement supersedes any earlier `None` entry in this section.

- Independent red-team review remains pending.
- Parent-deferred final gates remain: a current-source 5-by-1,000,000 production/direct roofline
  run and the full workspace/core-impact certificate. The existing roofline JSON remains
  inconclusive and MUST NOT be presented as a pass or fail ratio.

## Evidence

- Implementation and exact receipt behavior: `crates/cdf-dest-sqlite/src/`; the combined final leaf
  observations cover every one of the 13 named tests, with 12 passing in the single suite run and
  the zero-segment repair passing in its one authorized exact rerun. This proves only those
  assertions; it is not a post-repair aggregate-suite run.
- Affected build: `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo check -p cdf-dest-sql
  -p cdf-dest-sqlite -p cdf-builtin-drivers -p cdf-conformance -p cdf-benchmarks --all-targets
  --all-features --locked -j 12` passed.
- Strict lint: the same affected package set under `cargo clippy --all-targets --all-features
  --locked -j 12 -- -D warnings` passed. Its limit is that the later empty-package conditional and
  benchmark pre-fingerprint setup were not re-linted under the no-additional-gate instruction; the
  former compiled and passed its exact repair test.
- Built-in catalog: `cargo test -p cdf-builtin-drivers
  catalog_matches_the_data_driven_first_party_fixture --lib --locked -j 12` passed 1/1.
- Catalog/boundary/error slice: `cargo test -p cdf-conformance destination_catalog:: --lib
  --locked -j 12` passed 9/9.
- Runtime-chaos enrollment: `cargo test -p cdf-conformance
  registered_runtime_chaos_shards_cover_destination_catalog --lib --locked -j 12` passed 1/1.
- Exact live-column normalization repair: `cargo test -p cdf-contract
  destination_identifier_policy_adapts_sqlite_quoted_live_columns --lib --locked -j 12` passed
  1/1. The affected strict Clippy command for `cdf-contract`, `cdf-project`, `cdf-conformance`, and
  `cdf-dest-sqlite` with all targets/features and `-D warnings` passed.
- Exact sheet/mapper parity repair: `cargo test -p cdf-dest-sqlite
  sheet_type_mappings_exactly_match_the_physical_scalar_mapper --lib --locked -j 12 --
  --nocapture` passed 1/1 and emitted the inspection identity now pinned in the assertion and
  built-in fixture. Strict all-target/all-feature Clippy for `cdf-dest-sqlite`,
  `cdf-builtin-drivers`, and `cdf-conformance` with `-D warnings` passed after replacing the one
  deprecated test constructor; no matrix or roofline command was run.
- Exact governed-variant repair: `cargo test -p cdf-dest-sqlite
  only_exact_governed_variant_field_enters_the_sqlite_system_namespace --lib --locked -j 12`
  passed 1/1. The affected strict Clippy command for `cdf-dest-sqlite`, built-ins, conformance, and
  benchmarks with all targets/features and `-D warnings` passed. The exact error inventory is 119
  syntactic matches/118 actual CDF-kind sites; no matrix or roofline command was run.
- Exact universal target-name repair: `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p
  cdf-conformance generated_target_names_are_stable_and_valid_for_every_catalog_destination --lib
  --locked -j 12` passed 1/1. `cargo clippy -p cdf-conformance --all-targets --all-features --locked
  -j 12 -- -D warnings` with the same native-link environment passed. The preceding invocation
  without `DUCKDB_DOWNLOAD_LIB=1` failed only while linking the conformance test binary with
  `library not found for -lduckdb`; it did not run the assertion. No matrix or roofline command was
  run.
- Root-observed exact destination matrix: 18/18 cells executed and passed for
  `file|python|rest|postgres|sqlite|nebula` × `append|replace|merge`, with zero exclusions; the
  enclosing test passed 1/1 in 19.99 seconds. This proves the complete registered matrix on the
  post-repair source, within that run's environment and assertions.
- Error ledger: `.10x/evidence/.storage/2026-08-02-sqlite-destination-error-ownership-ledger.md`.
- Operator contract: `docs/operators/sqlite-destination.md` plus root/docs/operator/architecture
  indexes.
- Roofline raw evidence: `.10x/evidence/.storage/2026-08-02-sqlite-destination-roofline.json` is
  inconclusive for the stated reason; it contains five direct samples and no CDF samples.
- `cargo fmt --all -- --check` and `git diff --check` passed before the final journal/harness
  one-line edits. `graphify update .` could not run because `graphify` is not installed. The parent
  owns the explicitly deferred network-backed run matrix, workspace suite, and connector
  certificate.

### Closure-repair evidence mapping (2026-08-02)

- Zero-row Replace and immutable historical authority: focused tests
  `zero_segment_replace_atomically_empties_an_existing_target` and
  `historical_receipts_remain_verifiable_after_replace_and_overlapping_merge` passed in the leaf
  run. They support atomic empty replacement, independent old-receipt verification, and duplicate
  replay after later target/state mutation.
- Exact verifier evidence: `quarantine_mirror_has_exact_transactional_readback_and_fresh_verification`
  and `verifier_falsifies_full_segment_state_evidence_and_provenance_corruption` passed. They
  support collision-resistant same-count substitution detection and exact full JSON, state, range,
  and index validation; their limit is deliberate focused corruption rather than arbitrary disk
  fault injection.
- Cancellation: `injected_run_cancellation_stops_at_one_observation_and_rolls_back` and
  `sqlite_vm_progress_handler_interrupts_injected_cancellation` passed. They support injected
  authority propagation, zero iterator consumption after cancellation, transactional rollback,
  and SQLite VM interruption.
- Bit-exact floats: `floats_preserve_exact_ieee_bits_in_canonical_big_endian_blobs` passed, and the
  repaired sheet/physical parity assertion passed 1/1. Together they support BLOB storage and
  lossless bit-pattern fidelity for Float16/32/64, including NaN payloads and signed zero.
- Error ownership: the repaired
  `error_ownership_distinguishes_missing_durable_host_payload_and_target_failures` passed 1/1.
  It directly exercises stable NOT NULL extended-code ownership and live missing-durable,
  host-open, and target-unique boundaries. The regenerated ledger freezes 16 Rust files, 145
  syntactic matches, and 144 actual CDF-kind sites.
- Crash recovery: `subprocess_crashes_during_payload_and_mirror_mutation_roll_back_atomically` and
  `subprocess_crash_after_commit_recovers_as_stable_duplicate_without_rewrite` passed in the leaf
  run. They support real process termination on both sides of COMMIT and exact replay behavior.
- Layout and harness: the affected all-target/all-feature check and strict Clippy `-D warnings`
  both passed after the transaction split and roofline harness repair. No roofline performance
  claim is made because the raw cell was intentionally not rerun.
- Benchmark timing symmetry: source structure now places the complete direct-only planner/artifact
  bundle before `started`, and both CDF and direct workers delegate all post-commit verification
  through the same `finish_timed_commit` helper before elapsed capture. Benchmark-package
  all-target/all-feature check and strict Clippy passed after this repair. This supports boundary
  structure and compilation only; the parent-deferred roofline remains unmeasured on current
  source.
- Runtime lifecycle: root observed the exact SQLite runtime-chaos enclosing test pass 1/1 in 6.43s
  and all four configured crash cases pass with the recovery/no-double-write invariants recorded in
  the Journal above.

## Review

Independent red-team review performed 2026-08-02 without rerunning tests or changing
implementation. The review read the ticket and every referenced active authority, the complete
current tranche diff and assertions, all thirteen destination-crate Rust files, the 119-match /
118-actual-site error ledger, the inconclusive raw roofline artifact and repaired harness, the
operator documentation, and the root-observed 18-cell destination-matrix evidence.

### Findings

1. **Critical — a zero-row `replace` advances mirrors/state without replacing the target.**
   `apply_migrations` prepares the target and enters payload handling only when
   `expected_segments` is nonempty; otherwise it immediately writes a zero-count receipt and state
   mirror (`crates/cdf-dest-sqlite/src/transaction.rs:251-267`). The target `DELETE` exists only in
   `write_segments` (`transaction.rs:297-304`), which the zero-segment path never calls. A
   zero-row replacement of an existing table therefore commits a new load receipt/checkpoint-state
   mirror while leaving every old payload row visible. The test at `tests.rs:665` encodes the
   unratified behavior only against a database with no existing target and never exercises the
   required “nonempty target becomes atomically empty” case. This directly violates replace
   semantics and can publish stale data as successfully replaced.

2. **Significant — later replace/merge commits destroy verification of older package-token
   receipts.** Replace deletes prior target rows. Merge assigns every matched row the new package's
   `_cdf_row_key` (`transaction.rs:770-797`). The state mirror is also an upsert keyed by
   pipeline/resource/scope, so a later package replaces the older receipt's only matching state
   row. Fresh verification nevertheless requires exactly one state row with the receipt id and
   requires every old segment range still to cover its original current-target row count
   (`transaction.rs:918-953`, `:995-1037`). Thus replaying an already committed older package after
   a later replace or overlapping merge finds its `_cdf_loads` receipt and returns a no-op, but
   trait-level verification rejects it. The duplicate test covers only an immediate duplicate,
   before any intervening package. This breaks durable package-token idempotency/recovery and the
   documented promise that replay of the same finalized package returns its verified receipt
   without rewrite.

3. **Significant — “independent exact verification” trusts or omits several durable facts.** The
   verifier compares the caller receipt with the same serialized receipt stored by the commit,
   then reads only `row_key_start/end` from `_cdf_segments`; it never decodes or compares
   `segment_json`, including scope, output position, row/byte counts, and commit time
   (`transaction.rs:867-953`). State verification parses untyped JSON and checks only receipt id,
   package hash, schema hash, and timestamp, leaving checkpoint/parent, pipeline/resource/scope,
   position, and state version unverified (`:995-1037`). Target provenance is only a range count;
   it does not establish exact distinct/contiguous row keys or the required unique-index authority.
   Quarantine evidence is only `(count, XOR(SHA-256(record)))`, which is order-independent but not
   a collision-resistant multiset commitment. No corruption assertion falsifies segment JSON,
   state semantics, provenance uniqueness/index loss, or equal-count quarantine substitution.

4. **Significant — cooperative cancellation is not implemented inside destination work.** The
   declared pinned lane is `CooperativeOnly` (`runtime.rs:274-308`), but the transaction/session
   never obtains or checks run cancellation while converting and inserting rows
   (`transaction.rs:636-666`), executing merge SQL, applying mirrors, or verifying potentially
   large state/target data. The cancellation test supplies an iterator that returns an already
   cancelled error before the adapter receives a segment (`tests.rs:882-923`); it proves iterator
   propagation, not cancellation of the production writer. A large segment or SQLite statement
   can therefore continue to commit-bound completion after run cancellation. The one-writer lane
   and one-segment/64 MiB declarations are otherwise structurally present, but the suite does not
   falsify their live memory/slow-consumer ceiling.

5. **Significant — the sheet overstates floating-point fidelity.** Float16/32/64 are declared
   lossless SQLite `REAL` mappings (`mapping.rs:103`, `:137-139`) and are bound directly as
   `Value::Real` without a finite/non-finite policy (`mapping.rs:208-214`). SQLite's numeric binding
   does not preserve Arrow's complete IEEE value domain (notably NaN is normalized to SQL NULL),
   so a nullable field silently changes value and a non-null field can fail a constraint. The
   parity assertion proves only that the sheet and mapper chose the same label; it never
   round-trips edge values. The mapping must either provide a genuinely canonical exact encoding,
   narrow the advertised domain/fidelity with compiled policy, or fail preflight for values that
   cannot be represented losslessly.

6. **Significant — the exact error ledger reproduces, but two primary owners are wrong at real
   boundaries.** A missing database during read-only receipt verification reaches
   `SQLITE_CANTOPEN`; when its parent exists, the shared open classifier returns Environment
   (`error.rs:37-43`, `:80-89`) even though disappearance of an externally durable destination
   artifact is Destination by the active taxonomy. Conversely, all SQLite constraint violations
   are Destination (`error.rs:44-50`); a package array containing NULL under its non-null Arrow
   field reaches the generated target's NOT NULL constraint and is a finalized-package Data
   contradiction, not damaged destination state. The shared classifier lacks the explicit
   commit-versus-durable-verification and payload-versus-existing-target provenance needed to
   distinguish those cases. Separately, `health` calls `Path::exists()` twice
   (`runtime.rs:67-83`), collapsing permission/device metadata failures into “file will be
   created.” Typed-wrapper preservation, invalid-path redaction, and the current 13-file /
   119-syntactic / 118-actual inventory otherwise hold.

7. **Significant — the repaired roofline harness is still not a semantically comparable closure
   cell.** The current JSON is honestly inconclusive with five direct samples and zero CDF samples,
   so it supplies no ratio. More importantly for the required fresh rerun, the direct cell prepares
   its insert statement before starting the clock (`crates/cdf-benchmarks/src/sqlite_destination_roofline.rs:449-455`),
   while CDF prepares its statement after the segment is offered. The direct verifier checks only
   four row counts after writing abbreviated JSON (`:485-565`); CDF serializes full typed mirrors
   and performs receipt, schema, state, quarantine, segment, and provenance verification
   (`:420-438`). Labeling that work as favorable bias does not satisfy the roofline authority's
   MUST that the native baseline perform the same verification work it can express. The host
   fingerprint setup fix, per-process RSS/CPU measurement, explicit unobserved physical bytes,
   executable/content hashes, durability mode, and ratio arithmetic are otherwise coherent.

8. **Significant evidence gap — crash/chaos closure is not yet demonstrated.** The post-COMMIT
   subprocess failpoint is a real crash/recovery assertion, but the “before commit” case merely
   drops a connection in-process (`tests.rs:616-649`) and does not exercise process loss with a
   live rollback journal at any payload/mirror mutation boundary. Runtime-chaos evidence is only
   catalog-shard enrollment, not execution of the SQLite chaos laws. The post-repair 18/18 matrix
   is useful evidence for the registered two-column, nonempty append/replace/merge cells and has no
   exclusions, but those assertions do not cover zero-row replacement, historical duplicate
   replay, crash-before-commit, cancellation, or mapping edge values. The parent-deferred fresh
   roofline and full core-impact certificate remain explicit final gates, not inferred passes.

9. **Minor — the public facade is tight, but the main transaction boundary remains too broad.**
   `lib.rs` exports only the destination, runtime driver, and stable id, and the generic/catalog
   diff adds no destination-name branch to orchestration. However the 1,051-line
   `transaction.rs` owns session state, host dispatch, target migration, row conversion/write,
   merge SQL, row-key allocation, receipt verification, state verification, and crash hooks. The
   surrounding files are authority-shaped, but this composition file should be split into
   explicit session, target/merge writer, and verifier modules so those dependency directions are
   compiler-visible and the independent verifier cannot silently reuse commit assumptions.

### Checks that held

- Append, nonempty atomic replace, fail-on-duplicate/ambiguous merge, immediate duplicate package,
  rollback-on-drop, post-COMMIT process recovery, exact governed `_cdf_variant` admission, reserved
  identifiers, existing-column type/nullability rejection, and one quarantine corruption case
  have meaningful focused assertions within their stated limits.
- One `BEGIN IMMEDIATE` transaction owns target and mirror mutations; no private runtime, pool,
  semaphore, retry loop, WAL activation, or parallel writer was introduced. Journal-mode evidence
  is read without changing the mode, and the injected host clock owns receipt time.
- Built-in enrollment, catalog fixture hashing, conformance fixture construction, target-name
  normalization, and chaos-shard enrollment are data-driven. The root-observed destination matrix
  executed all 18 declared SQLite-destination cells with zero exclusions.
- Cargo.lock contains only the expected path-crate/dependent edges and new package stanza; no new
  third-party tuple was introduced. The exact framework-variant classifier remains owned by the
  neutral contract layer, and the adapter facade does not expose raw SQL or internal models.

### Verdict

**Fail.** The zero-row replace bug is a direct data-semantics failure. Historical receipt
verification, incomplete independent verification, cancellation, type fidelity, error ownership,
and benchmark comparability add independent significant blockers. Keep the ticket active and
non-terminal; repair these findings, obtain focused evidence that actually falsifies them, then
commission a fresh independent review before the parent-final roofline/certificate gates.

### Residual risk

No tests, benchmark, matrix, chaos suite, or certificate were rerun by this review, as required.
The existing 13-test evidence is not one post-repair aggregate run. No current assertion covers
multi-segment packages, historical receipt verification after later target mutation, zero-row
replace over an existing target, abrupt pre-commit process loss, cancellation during row/SQL work,
non-finite floats or full scalar round trips, large variable-width values under the live memory
ceiling, every SQLite extended error, or hostile-but-well-formed mirror/provenance corruption. The
current roofline artifact remains pre-harness-repair and inconclusive, and no full current-source
connector certificate exists yet.

### Closure-repair re-review (2026-08-02)

Independent, read-only re-review inspected the repaired production paths, focused assertions,
operator/spec changes, exact 16-file error ledger, repaired roofline harness, and the journaled
root observations. It did not rerun any test, benchmark, matrix, chaos case, or certificate.

#### Dispositions

- **Resolved:** zero-row Replace now prepares and deletes an existing target inside the same
  transaction, records the exact deleted count, and leaves a nonexistent zero-row target absent.
  The focused assertion covers an existing two-row target and independently verifies both the old
  and replacement receipts.
- **Resolved:** immutable `_cdf_state_history` and `_cdf_commit_evidence` separate historical proof
  from mutable current target/state. The historical assertion crosses append, Replace, overlapping
  Merge, old-token replay, receipt equality, and fresh verification after the intervening commits.
- **Resolved:** the verifier decodes and compares full typed segment/state/evidence, reconciles
  scalar ranges with JSON, binds acknowledgements and receipt identities, verifies the exact unique
  partial provenance index, and uses a domain-separated sorted per-record SHA-256 multiset
  commitment. Same-count quarantine substitution, segment JSON, state-history, and index-removal
  corruption assertions exercise the repaired boundaries.
- **Resolved with evidence limits:** cancellation is carried through session, row, merge, mirror,
  and verifier code; per-row checks and the installed SQLite progress handler bound native VM work.
  The focused assertions cover cancellation before iterator consumption with rollback and a real VM
  interrupt, but not cancellation arriving mid-write through the complete managed session.
- **Resolved:** Float16/32/64 are declared and written as canonical big-endian bit-pattern BLOBs.
  Sheet/mapper parity and signed-zero/NaN-payload assertions agree with the operator contract.
- **Resolved within the audited contexts:** `try_exists` distinguishes a missing durable verifier
  database from fallible host metadata, payload NOT NULL/datatype errors use Data context, existing
  target constraints remain Destination, and embedded typed errors retain provenance. The frozen
  manifest reproduces 16 Rust files and 145 syntactic matches: 144 actual CDF kind sites plus the
  one `std::io::ErrorKind` false positive.
- **Resolved:** subprocess failpoints terminate during live payload and mirror mutation before
  COMMIT and after COMMIT, with rollback/replay assertions. Root also observed the exact four-window
  SQLite runtime-chaos case pass and the 18/18 source-by-disposition matrix with zero exclusions.
- **Resolved:** the 18-line transaction facade has an acyclic authority graph:
  `session -> writer`, `session -> verifier`, and `verifier -> writer`; none of the leaf modules
  depends back on `session` or the facade.
- **Significant — benchmark timing remains asymmetric.** The CDF cell stops its timer immediately
  after the production fresh verifier and performs `verify_target_count` outside the measured
  region (`crates/cdf-benchmarks/src/sqlite_destination_roofline.rs:487-508`). The direct cell runs
  the same production verifier, then includes `verify_target_count` before stopping its timer
  (`:519-615`). In addition, direct `direct_commit_artifacts` calls the CDF destination planner
  inside its timed region (`:553`, `:737-738`), while CDF planning and session setup finish before
  its timer (`:468-487`). Both differences burden only the native baseline and can inflate the
  reported CDF/direct ratio. Move direct planning/artifact inputs before `started` and place the
  identical target-count check on the same side of both timers before the parent-owned rerun.

#### Verdict

**Concerns.** All prior connector-semantics, verification, cancellation, fidelity, error, crash,
chaos, and layout findings are repaired within the stated evidence limits. The roofline harness
timing asymmetry is the sole remaining significant review finding; it prevents a valid future
roofline ratio but does not turn the intentionally deferred measurement or full certificate into a
current pass/fail claim. Keep the ticket active until the harness is repaired, independently
checked, and the parent-final gates run.

#### Residual risk

The repaired leaf evidence is fragmented (20/22 in one run, followed by the two repaired focused
assertions) rather than one aggregate post-repair run. No current evidence exhausts mid-session
cancellation timing, large variable-width memory behavior, every SQLite extended/kernel error, or
arbitrary coordinated durable corruption. The stored roofline JSON remains pre-repair and
inconclusive; the current-source 5-by-1,000,000 roofline and full core-impact certificate remain
parent-owned final gates.

### Benchmark-boundary repair response (2026-08-02)

The sole significant re-review finding above is repaired in source: direct planner and mirror-
artifact preparation is untimed, and a shared helper puts production receipt verification plus
target-count verification inside both timed regions. `TIMED_REGION_VERSION = 2` prevents these
samples from comparing as the earlier boundary. The benchmark package check and strict Clippy
passed; no performance run was made. This execution response does not overwrite the independent
verdict: a fresh independent re-review remains required before the parent-owned roofline and
certificate gates.

### Final benchmark-boundary re-review addendum (2026-08-02)

This narrow, read-only pass re-reviewed only the remaining timing-asymmetry finding. In the direct
worker, `direct_commit_artifacts` now completes before both `BEGIN IMMEDIATE` and `started`
(`crates/cdf-benchmarks/src/sqlite_destination_roofline.rs:502-506`), matching the CDF worker's
untimed planning/prepared-session boundary (`:433-487`). Both workers then delegate to the same
`finish_timed_commit` function, which runs the production fresh verifier, performs the identical
target-count check, and only then captures elapsed time (`:494`, `:587-617`). The comparability key
uses `TIMED_REGION_VERSION = 2`, so future samples cannot be confused with the earlier boundary.

**Verdict: Pass for the bounded benchmark-boundary finding.** No remaining timing asymmetry from
the reviewed planner/artifact or verifier/target-count ordering was found. This supersedes the
`Concerns` verdict only for that sole outstanding finding; the historical reviews remain evidence
of the repairs they drove.

**Residual risk:** this review proves current source structure and the recorded compile/lint
evidence only. It did not run the harness or validate a performance ratio. The current-source
5-by-1,000,000 roofline and full core-impact certificate remain parent-owned final gates, and the
stored earlier roofline JSON remains inconclusive.

## Retrospective

### Closure-repair execution retrospective (2026-08-02)

- What broke: the first implementation conflated mutable current target/state with immutable
  receipt evidence, skipped the Replace delete for empty packages, advertised lossy SQLite REAL as
  lossless floats, and treated cooperative iterator cancellation as if it interrupted native SQL.
  The direct roofline path also omitted material mirror/verification work, while the monolithic
  transaction file concealed those ownership seams.
- What surprised: Arrow rejects nulls against a non-nullable field while constructing the batch,
  so that fixture could never reach SQLite's NOT NULL classifier. A direct stable extended-code
  assertion is the truthful unit boundary; existing-target uniqueness remains the live integration
  boundary. The generic runtime-chaos harness also requires its Postgres prerequisite even when
  selecting SQLite; the root-provided environment completed that external prerequisite and
  observed the final pass.
- Dead ends and cost: attempting to derive artifact hashes through an ad hoc temporary helper was
  unnecessary and was abandoned without a confirmed artifact. The exact parity and catalog
  assertions were the better deterministic authorities. A first catalog invocation also matched
  zero tests because `--exact` requires the module-qualified name; subsequent exact invocations
  used the full test path.
- What worked: immutable typed evidence beside mutable current authority made historical
  verification straightforward; domain-separated sorted per-record digests preserved quarantine
  multiplicity without order dependence; SQLite's progress hook supplied native cancellation; and
  cutting the module graph only after semantics stabilized kept the split mechanical and acyclic.
- Five-whys conclusion: the recurring defects came from asking one transaction module and one
  mutable mirror set to serve execution, current-state, historical-proof, and verification roles.
  Giving each role explicit data and module ownership removed the leaky abstraction rather than
  adding conditionals around it.
- Benchmark lesson: prose claiming equivalent timing was weaker than making both workers consume
  one post-commit timing helper. Shared code now owns the verifier/count/elapsed ordering, while the
  timed-region version makes the semantic change explicit to comparison authority.
- Durable follow-up ownership: independent re-review owns falsification of this repair. The parent
  intentionally retains the roofline rerun and full destination certificate as deferred work; no
  new performance claim was introduced.

The hardest correctness bugs were at negative-space boundaries: a zero-segment package must still
commit receipt/state evidence but must not cause a target table to exist, and a fresh verifier must
therefore distinguish “no payload authority” from “missing payload.” Encoding the target schema in
the exact receipt and gating its physical verification on segment acknowledgements preserves both
invariants.

The benchmark exposed a second boundary lesson: a storage-sensitive host fingerprint must observe
the same target existence state before every child cell. Fingerprinting a nonexistent path and
letting the reference cell create it made otherwise comparable samples inadmissible. Future
database rooflines should create/configure the empty target before deriving comparability, while
keeping schema/data setup outside the timed region.

Offline lock regeneration is too broad for adding a path dependency because it can legally
re-resolve unrelated compatible transitive versions. The effective technique was to compare
against the preexisting lock graph and retain only the three dependency edges and new package
stanza. The initial direct-release link failure also showed that a monolithic benchmark crate makes
an unrelated native dependency part of every binary's link boundary; a future bounded benchmark
packaging ticket should consider feature-gating native benchmark families if this recurs.

What worked: authority-shaped modules, a single explicit transaction, the common transactional
mirror manager, fresh read-only verification, exact error-site accounting, and live subprocess
post-COMMIT recovery made failure behavior observable rather than inferred. No additional generic
database abstraction was needed.

The matrix repairs reinforced that generic harness-generated identifiers are part of the product
contract: composing a valid source identity can still create a destination-reserved object name.
A stable `cdf_` harness namespace plus a catalog-wide assertion against every published identifier
rule catches that cross-engine boundary without destination branches. Reusing the exact governed
variant classifier similarly kept the `_cdf_*` fence closed to user fields while admitting only
the one framework-owned schema field.
