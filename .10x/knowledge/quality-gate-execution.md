Status: active
Created: 2026-07-06
Updated: 2026-07-31

# Quality gate execution

Use `QUALITY.md` to identify the change set, touched quality vectors, and smallest sufficient verification profile before choosing tools. Quality checks SHOULD be parallelized whenever tools do not contend on the same exclusive output. Run independent Cargo checks, scanners, and report readers in parallel batches within the selected profile instead of treating `QUALITY.md` as a universal serial loop.

When the selected `QUALITY.md` profile calls for CodeQL, database creation is expensive. Keep a reusable Rust database under ignored build output, such as `target/quality/codeql-db-rust`, and run `codeql database analyze` against the existing database when it is still valid for the current source, CodeQL version, and extractor inputs. Recreate the database only when source or dependency changes make the existing database stale, when CodeQL/extractor version changes, or when analysis indicates the database is invalid.

For CDF, prefer `tools/codeql-rust-quality.sh` for local Rust CodeQL runs. It keeps the reusable database at `target/quality/codeql-db-rust`, regenerates the generated CodeQL config with `target/**` and `reports/**` ignored, stores a content fingerprint beside the database, skips database creation when the existing database is fresh for the current CodeQL version and Rust source/manifests/lockfile content, analyzes with `--rerun`, and prints the extraction summary.

Before creating a CodeQL database from the repository root, remove or avoid generated analysis directories such as `target/semver-checks` and `target/llvm-cov-target`; otherwise CodeQL may index generated Rust files and report extraction-warning noise even when the source checks pass. If the command must run from the root so Cargo can see the workspace, keep the database under `target/quality/codeql-db-rust` and record extraction-warning limits in evidence.

With CodeQL CLI 2.25.6 and local Cargo/Rust 1.96.1, CDF's Rust extractor diagnostics are dominated by ordinary macro expansion failures for `format!`, `assert!`, `assert_eq!`, `vec!`, `matches!`, `serde_json::json!`, and `rusqlite::params!`, plus an extractor-side `cargo metadata` warning for unsupported `--lockfile-path`. Treat this as a local CodeQL Rust extractor limit, not a reason to rewrite normal product code, as long as analysis has 0 extraction errors, 0 SARIF findings, and the selected `QUALITY.md` profile plus any explicitly risk-triggered checks pass.

`cargo geiger` is supporting evidence for the `QUALITY.md` Unsafe/Soundness profile, not a default closure step. It can clean normal Cargo build output and may fail on dependency scan warnings even when cdf-owned code has no `unsafe`. A 2026-07-06 package-manifest run against normal target output removed a large Cargo build cache before it was interrupted. Do not run Geiger against the normal workspace target during closure. Use an isolated `CARGO_TARGET_DIR` only when the Geiger signal is worth the rebuild cost, and always pair it with a direct source search over `crates/` for `unsafe`, FFI, raw-pointer, and `Send`/`Sync` surfaces. For a usable first-party summary, prefer an absolute package manifest and JSON output, for example `CARGO_TARGET_DIR=target/quality/geiger-<slug>-target cargo geiger --manifest-path "$PWD/crates/<crate>/Cargo.toml" --all-targets --all-features --include-tests --locked --output-format Json > target/quality/reports/geiger-<slug>.json 2> target/quality/reports/geiger-<slug>.stderr`.

When the `QUALITY.md` Test-Quality or Deep profile applies, protocol parsers and validation-heavy Rust code often benefit from a bounded package-local `cargo mutants` pass after the normal focused tests pass. Treat surviving mutants as closure evidence gaps when they map to the ticket acceptance contract; add focused assertions for field shape validation, deterministic serialization, line numbers, and identity formatting instead of accepting broad coverage percentages as proof.

For reusable conformance harnesses, include negative self-tests with deliberately faulty implementations so mutation testing can prove the harness itself fails on contract violations. When the harness is consumed by downstream crates, run `cargo mutants` with both the harness crate and at least one downstream consumer in `--test-package`; otherwise harness-local tests or downstream integration may silently be omitted from the mutation oracle.

This local `cargo-mutants` version does not accept Cargo flags such as `--locked` directly. Pass them through `--cargo-arg`, for example `cargo mutants --file crates/cdf-engine/src/execution.rs --all-features --cargo-arg --locked --jobs 2 --test-tool cargo --output reports/ai-quality/mutants-engine-tracing -- -p cdf-engine`.

Do not place generated quality reports or CodeQL databases in tracked source. Prefer ignored build output or `/tmp` for transient reports, and record summarized results in `.10x/evidence/`.

## Hosted deep-gate triage

Scheduled deep quality MUST keep independent compile, test/conformance, generated/API,
metrics, supply-chain, and static-security jobs so one failure exposes a complete attributable
frontier instead of hiding every later gate. On the first failing dispatch, collect every completed
job and shard result before repairing. Classify the frontier once into product correctness,
test-fixture isolation, generated-artifact drift, tool invocation/configuration, accepted policy
exceptions, and unrelated inventory. Apply one bounded corrective tranche and dispatch one clean
run at its exact commit; do not turn each surfaced line into a separate serial review cycle.

Scanner exit status is not itself policy. Preserve the full Semgrep registry inventory as an
artifact, but block on its warning/error severities rather than treating informational uses of
`unsafe`, process arguments, temporary directories, and current-executable discovery as 100+
indistinguishable failures. OSV output MUST be parsed and may admit only exact active advisory
exceptions; a scanner runtime/configuration failure or any additional advisory remains blocking.
Existing dependency-hygiene inventories without a ratified zero baseline MAY be nonblocking only
when the step remains visibly yellow and its complete output is uploaded for a separately owned
cleanup decision.

GitHub's CodeQL Rust extractor uses `build-mode: none`; it rejects manual build mode. In hosted
Actions, use the official init/analyze pair so the action-owned CLI and database lifecycle stay
coherent. `tools/codeql-rust-quality.sh` remains the local cached-CLI path and assumes `codeql` is
on `PATH`; do not compose that local script with an Actions init step that keeps its CLI private.
Validate workflow/tool contracts on the hosted runner because local macOS checks cannot prove
Linux-only compilation or runner-owned action behavior.

Rust build caches are incomplete when a build script downloads an untracked native link input.
For `DUCKDB_DOWNLOAD_LIB=1`, cache `target/duckdb-download` explicitly and validate that a
platform `libduckdb` file exists after restoring Cargo artifacts. If it is absent, invalidate only
`libduckdb-sys` so Cargo reruns the authoritative build script; a restored Rust fingerprint is not
proof that its external native library still exists.

For staged destination replay, a prepared writer/input memory lease is authority to consume a
durable canonical segment directly; it is not spare capacity for decoding the same segment through
a second live-batch reservation. A finalized package with verified durable-file authority SHOULD
transfer that authority into the destination worker. Selecting a live reader after reserving the
worker budget can turn a tiny duplicate replay into an unbounded `reserve_blocking` wait. When a
deep shard stalls but the corresponding live-repeat law passes, capture one process stack and
compare live versus finalized-package input ownership before changing timeouts or concurrency.

Historical Gitleaks scans have two known false-positive `generic-api-key` findings in removed Python-era Harness SDK-key field declarations. See `.10x/knowledge/historical-gitleaks-findings.md` for exact fingerprints and limits. Treat only those exact fingerprints as documented historical scanner noise; any new history finding, and any finding from a current-tree or staged-diff scan selected by `QUALITY.md`, remains a hard failure until triaged.

## Product lifecycle gate

Focused tests are not product-lifecycle proof when a change crosses discovery, compilation,
external task authority, package creation, destination ingress, receipt verification, or
checkpoint commit. Use the mandatory bounded smoke matrix and representative-fixture guidance in
`.10x/knowledge/product-integration-and-closure-gate.md`. In particular, lifecycle fixtures must
exercise real identity transitions, more than one partition/segment, and both first-run and no-op
incremental behavior rather than manufacturing one hash for every phase.

When multiple workers share the repository, one coordinator SHOULD own workspace-wide checks and
route failures to the authoring lane. Do not run several contending copies of the full Cargo suite.
Each lane remains responsible for focused tests, formatting its owned paths, and journaled
evidence.

## Connector admission

Use `tools/certify-connector.py` for a source or destination admission claim. It is orchestration,
not a second semantic harness: it selects connector-leaf laws, exact built-in catalog integrity,
ordinary conformance, the registered matrix slice, and the applicable graph/product/chaos laws.
Child output goes to stderr; stdout and `--report` contain one machine-readable result with the
fixed integration base, merge base, exact HEAD, changed-file content digest, selected profile,
exact commands, timeouts, durations, and statuses. Synthetic Nebula/Quasar gate checks require
`--fixture` and emit `admissible: false`.

The classifier reads committed, staged, unstaged, deleted, and untracked files directly from Git.
Do not add a baseline or changed-file override: either would let a report omit a committed core
edit. Root manifests, the lockfile, dependency policy, workflows, and tools are shared ownership,
not connector-only files. Generic ownership without acknowledgement fails before expensive
checks. With `--core-impact`, run every connector law first and then workspace nextest and strict
workspace all-feature Clippy.

## Build and performance runbooks

Routine local and EC2 builds that include DuckDB SHOULD follow
`.10x/skills/build-and-install-cdf/SKILL.md`; published release builds intentionally use the full
static `bundled-duckdb` path. Performance regressions and promotable defaults follow:

- `.10x/skills/investigate-cdf-performance-regressions/SKILL.md`
- `.10x/skills/run-cdf-ec2-benchmarks/SKILL.md`
- `.10x/knowledge/performance-evidence-and-regression-triage.md`

Do not use a one-TiB stress run or a new EC2 host as routine reassurance. Select them only when the
changed quality vector and ticket acceptance need that evidence.
