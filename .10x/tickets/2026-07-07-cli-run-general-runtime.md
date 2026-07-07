Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md
Depends-On: .10x/tickets/done/2026-07-07-general-run-orchestrator.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md, .10x/decisions/destination-introspection-package-and-cli-policy.md

# Wire `cdf run` to the general runtime

## Scope

Replace the DuckDB/local-file-only CLI `run` implementation with routing through `cdf_project::run_project`.

Owns:

- `crates/cdf-cli/src/commands.rs`
- `crates/cdf-cli/src/context.rs`
- `crates/cdf-cli/src/args.rs` only if run-id or destination options need parser support.
- Focused CLI tests for supported and unsupported run combinations.

## Acceptance criteria

- `cdf run` routes local file, exact zero-lag REST, and table-backed Postgres SQL resources through `ProjectRunRequest`/`run_project` where the lower-layer resource runtime dependencies are available.
- `cdf run` supports DuckDB, Postgres, and filesystem Parquet environment destinations. Filesystem Parquet uses `parquet://<root>` as a destination root/prefix. Postgres uses explicit `--target` plus `[environments.<name>.destination_policy.postgres] merge_dedup = "fail"` for merge duplicate policy.
- Unsupported resource/destination/disposition combinations fail before source, package, destination, or checkpoint mutation.
- JSON output includes run id, package, checkpoint, receipt, destination, receipt source, row/segment counts, and ledger event summary.
- Existing DuckDB/local-file CLI run behavior remains compatible except for gaining run id/ledger fields.

## Evidence expectations

Run focused CLI run tests, relevant `cdf-project` tests, `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`, `cargo check --workspace --all-targets --locked`, and `git diff --check`.

## Explicit exclusions

No `resume`, no `replay package`, no `inspect run`, no `run --loop`, no arbitrary SQL query execution, no new destination semantics beyond CLI destination URI/config parsing needed for already-supported destinations.

## Design notes

- Current CLI `run` uses `run_local_file_to_duckdb_checkpoint` and rejects REST/SQL/Postgres/Parquet because the runtime used to be specialized.
- `.10x/decisions/destination-introspection-package-and-cli-policy.md` ratifies `parquet://<root>` as a filesystem Parquet destination root/prefix, not a single file.
- `.10x/decisions/destination-introspection-package-and-cli-policy.md` ratifies standard destination introspection wherever applicable while forbidding introspection from supplying missing write semantics.
- Postgres destination credentials must resolve through the existing secret provider without serializing resolved values into reports.
- Postgres run target remains explicit through the existing `--target` argument. Merge duplicate policy comes from `[environments.<name>.destination_policy.postgres] merge_dedup = "fail"`.

## Blockers

None from user.

REST CLI run routing still requires implementation of a production `HttpTransport` adapter. `cdf-project` supports dependency-bearing REST resources, but `cdf-cli` has no production transport registered yet.

Postgres CLI destination routing still requires implementation of the ratified project-config policy shape.

## Progress and notes

- 2026-07-07: Split from the broad CLI spine ticket after general orchestrator closure.
- 2026-07-07: Wired `cdf run` through `cdf_project::run_project(ProjectRunRequest)` for local file resources, table-backed SQL resource adapters, and DuckDB destinations. JSON run reports now include the minted run id, destination summary, receipt object, row/segment counts, and run-ledger event summary while preserving existing DuckDB/local-file fields.
- 2026-07-07: Kept REST fail-closed before run mutation because no production `HttpTransport` exists in the current CLI/lower-layer surface. At implementation time, Postgres and filesystem Parquet were kept fail-closed because `.10x/decisions/superseded/project-run-postgres-destination-inputs.md` and the then-missing Parquet URI spelling left CLI policy inputs unresolved.
- 2026-07-07: Focused evidence recorded in `.10x/evidence/2026-07-07-cli-run-general-runtime.md`.
- 2026-07-07: User ratified `.10x/decisions/destination-introspection-package-and-cli-policy.md`: `parquet://<root>` is the filesystem Parquet destination root/prefix; destination introspection is standard wherever applicable but cannot infer missing write semantics; package scope is one resource transition; and Postgres project policy is `[environments.<name>.destination_policy.postgres] merge_dedup = "fail"`. Decision-level blockers for Parquet and Postgres are cleared; implementation wiring remains.
- 2026-07-07: Implemented the filesystem Parquet CLI destination slice for `cdf run`: `parquet://<root>` is parsed as a filesystem root/prefix, relative roots resolve under the selected project root, absolute roots are allowed, and empty/nested URI roots fail closed before package, destination, or checkpoint writes. The CLI now constructs `ProjectRunDestination::ParquetFilesystem { root, target }` and reports Parquet destination root/destination id/receipt/checkpoint/ledger fields in JSON. Focused success and malformed-URI CLI tests were updated. Evidence: `.10x/evidence/2026-07-07-cli-parquet-run-replay.md`. Review: `.10x/reviews/2026-07-07-cli-parquet-run-replay-review.md`.
