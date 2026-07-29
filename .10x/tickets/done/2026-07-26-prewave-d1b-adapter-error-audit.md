Status: done
Created: 2026-07-26
Updated: 2026-07-27
Parent: `.10x/tickets/done/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/done/2026-07-26-prewave-d1-environment-error-taxonomy.md`

# Audit adapter environment and internal errors

## Scope

Semantically audit every internal-error construction site in source, format, transform,
transport, destination, Python, subprocess, and foreign-stream adapter crates. Reclassify host,
filesystem, process, SDK, resource-limit, and local I/O failures as Environment while preserving
real CDF invariants as Internal.

## Non-goals

- No adapter behavior, retry policy, type mapping, or transport semantic change.
- No keyword-based mass replacement or generic remediation.
- No error masking at FFI boundaries.

## Acceptance criteria

- Every internal-error site in the named adapter families is classified and reviewable.
- OS/file-descriptor/temp-path/executable/native-library failures become Environment with concrete
  context and remediation.
- Decoder, identity, ownership, and impossible-state invariants remain Internal.
- Source/destination/format/foreign conformance preserves retry/auth/data/destination kinds and
  redaction.
- TTY/headless/JSON representative snapshots and focused adapter tests pass.

## References

- `.10x/specs/cli-report-authority-and-environment-errors.md`
- `.10x/specs/cli-error-experience-catalog.md`
- `.10x/tickets/done/2026-07-26-prewave-d1-environment-error-taxonomy.md`

## Assumptions

- Record-backed: the shared Environment kind and stable exit mapping land in D1 first.

## Journal

- 2026-07-26: Split from the 1,094-site audit so adapter failure semantics receive focused review.
- 2026-07-27: Enumerated 358 textual Internal construction lines across 135 adapter Rust files and
  inspected the construction context rather than replacing by keyword. Reclassified subprocess
  OS/process-group operations, relative-root current-directory access, HTTP-client construction,
  DuckDB local sidecar I/O, and Parquet local filesystem operations as Environment. Preserved
  decoder, identity, ownership, lifecycle, serialization, poisoned-state, and impossible-state
  invariants as Internal.
- 2026-07-27: Split local Parquet ownership at the durable-artifact boundary: host filesystem
  inability is Environment, externally durable artifact absence/truncation/shape is Destination,
  remote provider semantics stay Destination, and asynchronous object-store task failure is
  Internal. Preserved the existing conditional-delete failure outcome while classifying a raced
  missing durable artifact as Destination and host deletion inability as Environment.
- 2026-07-27: Made the Iceberg error adapter preserve every embedded typed `CdfError` before its
  upstream retry/kind fallback. The round-trip fixture covers Transient, RateLimited (including
  delay), Auth, Contract, Data, Destination, Environment, and Internal.
- 2026-07-27: Focused adapter suites initially passed: file source 48, Iceberg 42, subprocess 30
  with one release-only ignored test, HTTP transport 16, DuckDB 58, and Parquet 44 with one
  release-only ignored test. Three representative CLI taxonomy/rendering tests pass. An initial CLI listing
  attempt omitted the required DuckDB dynamic-library environment and failed only at link; the
  corrected invocation succeeded. Strict Clippy then found one needless borrow in the new Parquet
  classifier; it was corrected before the final lint gate.
- 2026-07-27: `graphify update .` could not run because the repository's documented `graphify`
  executable is unavailable in this environment (`command not found`); no graph freshness claim is
  made.
- 2026-07-27: First delegated review failed closure with one high, four medium, and one low
  finding. The review exposed a third Internal-construction idiom omitted by the initial
  inventory, fallible metadata hidden behind `Path::exists`, private scratch flattened to
  Environment, durable destination artifact ownership drift, and object-store wrappers that could
  erase an embedded typed error. Repaired all six: the complete baseline is 359 and retained
  inventory 333; unusual HTTP status is Data; conditional deletion uses fallible metadata;
  durable absence is Destination and host inability Environment; private scratch
  disappearance/shape is Internal; and full source-chain CDF errors preserve kind, retry delay,
  and message with safe action context before I/O/provider fallback.
- 2026-07-27: Second review exposed remote provider errors that source through `std::io::Error` and
  durable-artifact shape failures hidden in direct metadata/read/delete branches. Added explicit
  local-artifact versus remote-provider context to every object-store conversion: embedded typed
  CDF errors always win, local raw I/O follows the artifact classifier, and remote raw I/O remains
  Destination. Routed conditional-delete, CAS, digest, and delete failures through the durable
  artifact classifier while preserving each operation's explicit NotFound outcome. The settled
  Parquet suite now passes 46 tests with one release-only ignore; HTTP transport passes 16; strict
  Clippy remains clean.
- 2026-07-27: The second reviewer then found Iceberg's own raw-I/O conversion hidden beneath its
  coarse non-retryable `Unexpected` kind. Extended the wrapper precedence to embedded typed CDF,
  then external-source raw I/O (missing/truncated/invalid/wrong-shape/loop as Data; host
  permission/device/resource failure as Environment), then the SDK retry/kind fallback. The
  settled 43-test Iceberg suite and strict all-feature/all-target Clippy pass; source-less
  `Unexpected` remains Internal.
- 2026-07-27: Distilled the repeated discovery/provenance failures into the canonical
  `.10x/skills/audit-error-ownership/SKILL.md` and exact `.agents/skills` mirror. The skill-creator
  validator could not start because its Python environment lacks `yaml`; Ruby parsed both
  frontmatters, checked required project metadata/no TODOs, and `cmp` proved the skill and UI
  metadata mirrors exact.
- 2026-07-27: Independent runbook review initially found the illustrative `<scoped-files>` token
  was shell redirection rather than an executable manifest. Replaced it in both mirrors with a
  concrete null-delimited Rust-file manifest reused by `xargs -0 rg`; the exact example commands
  pass and the final skill review verdict is pass.

## Blockers

None.

## Evidence

- `.10x/evidence/2026-07-27-adapter-error-ownership-audit.md` records the reproducible 135-file,
  333-retained-site classification, direct-enum supplement, and ownership rationale for every
  family.
- `cargo test -p cdf-subprocess -p cdf-source-iceberg -p cdf-source-files
  -p cdf-transport-http -- --test-threads=1` — the pre-review run passed 136 tests with one
  release-only test ignored. After adding raw-I/O classification coverage,
  `cargo test -p cdf-source-iceberg -- --test-threads=1` passes all 43 tests.
- `DUCKDB_LIB_DIR="$PWD/target/debug/deps"
  DYLD_LIBRARY_PATH="$PWD/target/debug/deps" cargo test -p cdf-dest-duckdb
  -p cdf-dest-parquet -- --test-threads=1` — the pre-review run passed 102 tests with one
  release-only test ignored. After adding two classification regressions,
  `cargo test -p cdf-dest-parquet -p cdf-transport-http -- --test-threads=1` passes 62 tests with
  one release-only test ignored (Parquet 46, HTTP 16); the unchanged DuckDB suite's 58 tests had
  already passed.
- With the same DuckDB library environment,
  `migrated_command_family_errors_include_code_and_remediation`,
  `missing_current_directory_maps_through_the_cli_environment_boundary`, and
  `not_supported_error_preserves_exit_code_and_json_compatibility` each pass and cover
  human/headless/JSON compatibility, Environment mapping, code/remediation, and the preserved
  non-Environment path.
- `cargo clippy -p cdf-subprocess -p cdf-source-iceberg -p cdf-source-files
  -p cdf-transport-http -p cdf-dest-duckdb -p cdf-dest-parquet --all-targets --all-features --
  -D warnings` passes in the final settled state.
- `cargo fmt --all -- --check` and `git diff --check` pass.
- Final inventory rerun: 135 scoped Rust files, 333 convenience-constructor sites across 68 files,
  and three direct `ErrorKind::Internal` matches, all verified as one conversion and two test
  assertions rather than hidden constructors. Therefore 67 scoped files contain no Internal
  construction.
- Skill validation: the prescribed `quick_validate.py` is unavailable because its interpreter
  lacks PyYAML (`ModuleNotFoundError: yaml`). A Ruby YAML parse verifies both mirrored
  frontmatters, project metadata, and absence of template TODOs; `cmp` verifies exact canonical
  SKILL/UI-metadata mirrors. Limit: this is equivalent structural validation, not a successful run
  of the prescribed validator.
- Skill command validation: the exact null-delimited example manifest and both scans execute
  successfully (64 convenience matches and one direct-enum match in the illustrative three-crate
  scope); canonical/mirror `cmp` and `git diff --check` pass.

## Review

- OCR deterministic scope/rules covered all changed Rust files and 10x records. First delegated
  review verdict: fail (one high, four medium, one low). It found swallowed metadata failure,
  private/durable ownership drift, typed wrapper flattening, an omitted direct Internal
  constructor, and false inventory arithmetic. All were reproduced and repaired.
- A second independent review found remote provider I/O provenance could still be mistaken for
  local host ownership and then found raw Iceberg I/O hidden by its coarse SDK kind. Both were
  repaired with explicit origin context and source-chain precedence.
- Final reviewer A: pass, no findings. It independently reproduced 135 files, the 359-construction
  baseline, 333 retained sites, 68 site-bearing files, and 67 without sites; it verified the
  Parquet/HTTP/Iceberg repairs. Residual risk: platform-specific subprocess/filesystem fault
  injection is representative rather than exhaustive.
- Final reviewer B: pass, no findings. It verified typed kind/retry preservation, local artifact
  versus remote provider ownership, durable shape handling, subprocess OS versus lifecycle
  invariants, and the reproducible inventory. Residual risk: real provider chains and
  uncooperative external filesystem races are not exhaustively induced.
- Independent skill review: initial concerns over a non-executable placeholder were repaired;
  final verdict pass with no findings. Residual risk: future users must replace the illustrative
  crate roots with the ratified scope.

## Retrospective

- What broke: the first inventory matched constructor spellings instead of the semantic enum and
  missed a direct `CdfError::new(ErrorKind::Internal, ...)`. Wrapper helpers then proved capable of
  erasing both typed error metadata and the origin needed to distinguish local host from remote
  provider I/O. `Path::exists` also hid actionable metadata failure as absence.
- What surprised: `std::io::Error` is not ownership provenance. It can mean local host I/O, an
  external source artifact, a durable destination artifact, private scratch, or a remote SDK
  implementation detail. Likewise, a lower retained-Internal count can be less correct: two
  additional Internal sites were necessary to represent private-scratch corruption honestly.
- Dead ends and friction: the initial keyword inventory and constructor-only filesystem tests
  looked complete but did not falsify nested wrappers or shape/resource splits. One CLI command
  omitted the repository's dynamic DuckDB library environment; one strict lint exposed a needless
  borrow. The prescribed skill validator also lacks its PyYAML dependency in this environment.
- What worked: explicit boundary provenance, a typed-error-first full source-chain walk, paired
  shape/resource fixtures, full affected suites, and two independent falsification reviews found
  defects that the raw count and happy-path tests could not.
- Five whys: ownership drift recurred because wrappers accepted only an error, because helper
  signatures omitted origin provenance, because tests exercised one local constructor rather than
  a boundary matrix, because discovery searched spelling rather than kind, because no repeatable
  ownership-audit runbook existed. The knowledge taxonomy and new `audit-error-ownership` skill now
  make those invariants executable for D1c and future adapter work.
