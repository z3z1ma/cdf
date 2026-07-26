Status: done
Created: 2026-07-08
Updated: 2026-07-26
Parent: .10x/tickets/2026-07-08-p1-product-experience-program.md
Depends-On: QUALITY.md, .10x/specs/conformance-governance-roadmap.md, .10x/knowledge/dependency-tuple-migration-guard.md

# P1 product WS8: Release engineering and distribution

## Scope

Add the production pipeline: CI workflows, release workflow, changelog, versioning/LTS policy, reproducible checksummed binaries, install channel, and generated completions/man pages in artifacts.

## Required outcomes

- CI has fast gates per push and scheduled/manual slow gates that follow the relevant `QUALITY.md` Deep Loop.
- Release workflow produces reproducible checksummed binaries for mainstream targets.
- `CHANGELOG.md` follows a ratified convention.
- Versioning/LTS policy covers artifact-spec versions, migration fixtures, dependency tuple cadence, support windows, and graph-derived crates.io publication eligibility.
- At least one install channel is scripted and smoke-tested; other channels are ticketed.
- Completions and man pages ship as release artifacts.

## Acceptance criteria

- Green pipeline runs are recorded as evidence.
- A versioned pre-release is cut end to end.
- Installer smoke test passes on a clean target or documented local equivalent.
- The LTS/versioning spec is active and referenced by release jobs.
- Supply-chain gates from `QUALITY.md` are wired into the appropriate fast/slow phases without recreating reusable CodeQL databases unnecessarily.

## Evidence expectations

Record CI run URLs or local workflow output, release artifact checksums, installer smoke output, generated artifact proof, LTS spec review, and supply-chain gate output.

## Explicit exclusions

No claim of crates.io publication while the distributable graph contains disallowed git/path dependencies. No unsupported target promises. No manual-only release steps unless explicitly recorded as temporary blockers.

## Progress and notes

- 2026-07-08: Opened from P1 product directive. This lane may begin immediately and must respect the existing reusable CodeQL database policy.
- 2026-07-08: Ratified `.10x/specs/versioning-lts-release-policy.md` from P1 plus existing governance records. Split execution into `.10x/tickets/done/2026-07-08-p1-product-ws8a-ci-quality-workflows.md`, `.10x/tickets/done/2026-07-08-p1-product-ws8b-release-artifact-workflow.md`, and `.10x/tickets/done/2026-07-08-p1-product-ws8c-changelog-installer-channel.md`.
- 2026-07-08: WS8B release artifact workflow closed at `.10x/tickets/done/2026-07-08-p1-product-ws8b-release-artifact-workflow.md`. It added the GitHub Actions release workflow, fail-closed metadata/artifact packaging scripts, local host artifact smoke evidence, and checksum verification. Parent remains open for actual hosted release-run evidence and generated completions/man pages once WS2D closes.
- 2026-07-08: WS8B reproducibility blocker repaired. Packaging now uses deterministic tar/gzip writing through `tools/write-reproducible-targz.py`, and WS8B evidence includes a two-package identical-input SHA-256 and byte-identity smoke proof.
- 2026-07-11: Fast CI was reduced to the ratified smoke/deep boundary in `.10x/decisions/fast-ci-budget-and-deep-gate-separation.md`. Evidence is `.10x/evidence/2026-07-11-fast-ci-lean-boundary.md`; review is `.10x/reviews/2026-07-11-fast-ci-lean-boundary-review.md`. The gitleaks false-positive failure was fixed structurally by scanning `git archive HEAD`, not by suppressing findings.
- 2026-07-17: That fast-CI authority was superseded by `.10x/decisions/fast-ci-leaf-owner-gates.md` after CG1 extracted `cdf-cli-core`; the smoke/deep boundary remains, but CLI grammar/render/artifact owner checks now run against the lean core package rather than the full product graph.
- 2026-07-25: Activated the final hosted release gate after CX4 closed the holistic CLI lane. The existing public `v0.1.0` prerelease is not a valid current install channel. A direct smoke with the then-current dynamic-runtime installer failed with `artifact does not contain libduckdb.dylib`; all five public archives were inspected and lack that runtime. The release path will cut `0.2.0-alpha.1` from current `main` rather than preserving the obsolete archive layout through a compatibility shim.
- 2026-07-25: Direct inspection of the old public macOS binary found a second independent portability defect: it names `/Library/Frameworks/Python.framework/Versions/3.14/Python` absolutely and aborts on a clean host without that runner-specific framework. The release contract now stages every required non-system runtime library beside `cdf`, rewrites macOS load commands to `@rpath`, installs the complete verified runtime set, and includes third-party licenses. Per user direction, published artifacts do **not** use `DUCKDB_DOWNLOAD_LIB`: the workflow enables `duckdb/bundled` and pays the one-time source-build cost for static DuckDB, while developer and benchmark builds retain the prebuilt dynamic optimization.
- 2026-07-25: Made the release/CI boundary mechanically explicit after user correction. The
  release matrix does not set `DUCKDB_DOWNLOAD_LIB`; it activates the first-class
  `cdf-cli/bundled-duckdb` feature, which propagates through `cdf-dest-duckdb` to
  `duckdb/bundled` and selects `libduckdb-sys`'s static source-build backend. The matrix also
  inspects each platform binary's dependency table and fails if any dynamic DuckDB library is
  present. Developer and benchmark builds retain the prebuilt dynamic optimization without
  weakening the published static-binary contract.
- 2026-07-25: The first hosted static matrix falsified two portability assumptions before publication. Windows/MSVC cannot compile `sha2-asm`'s GNU assembly, so the two feature-enabling owners now retain hardware assembly on non-Windows targets and use RustCrypto's portable implementation only on Windows. ARM Linux completed the full 13-minute bundled DuckDB build but exposed literal backslashes in three shell-single-quoted Python packaging snippets; those snippets now pass valid Python on every shell. Neither repair changes the non-Windows hashing hot path or the static DuckDB release contract.
- 2026-07-25: A subsequent hosted matrix exposed that Cargo's dependency feature syntax on the
  `cdf-cli` command line did not activate DuckDB's bundled backend through the package boundary;
  Unix jobs therefore attempted `-lduckdb`, while Windows separately found Unix-only payload-cache
  test helpers compiled without definitions. Added explicit feature forwarding at both owning
  package boundaries and fail-closed non-Unix payload-cache helpers. The release command now has
  one public static-link feature rather than reaching into a transitive dependency.
- 2026-07-26: Closed from hosted run
  <https://github.com/z3z1ma/cdf/actions/runs/30196650532>. All five native targets passed static
  DuckDB dependency inspection, relocatable Python packaging, archive verification, and upload;
  the aggregate bundle verified and published
  <https://github.com/z3z1ma/cdf/releases/tag/v0.2.0-alpha.1>. The actual published aarch64 macOS
  archive then passed checksum verification, clean-prefix installation, environment-independent
  version execution, strict signature verification, and generated completion/man-page inspection.

## Blockers

None for shaping. Actual crates.io publication remains blocked while the current Arrow-rs and Iceberg git pins are reachable from the distributable crate graph; binary release work is not blocked.

## Evidence

- Public `v0.1.0` audit: all five published archives lack an adjacent DuckDB runtime, and the
  aarch64 macOS executable names
  `/Library/Frameworks/Python.framework/Versions/3.14/Python` absolutely. Running that exact
  executable on the current host aborts in `dyld` before `cdf version`; this proves the old
  prerelease is not a portable install channel.
- `tools/verify-release-metadata.sh 0.2.0-alpha.1`,
  `tools/test-release-artifacts.sh`, and `tools/test-install-cdf.sh`: pass. The matrix covers
  deterministic byte-identical archives, generated artifacts, required runtime/license presence,
  checksum mismatch, missing checksum/artifact, unsupported target, dry-run, clean-prefix install,
  and requested/artifact version mismatch.
- `cargo tree -p cdf-cli --locked --features bundled-duckdb -e features -i libduckdb-sys`: the
  exact release feature resolves
  `cdf-cli/bundled-duckdb → cdf-dest-duckdb/bundled-duckdb → duckdb/bundled →
  libduckdb-sys/bundled`, including `libduckdb-sys/cc`; this proves the release command selects
  the static source-build backend rather than the developer download backend.
- `cargo test -p cdf-object-access --locked`: 39 passed and the one million-entry slow test was
  intentionally ignored. Strict all-target Clippy over `cdf-object-access`, `cdf-dest-duckdb`,
  and `cdf-cli`, formatting, and `git diff --check` passed. A local Windows cross-check reached
  the native AWS-LC build and stopped because this macOS host has no Windows SDK; the hosted
  Windows matrix remains the target authority.
- Local artifact-mechanism smoke with the optimized aarch64 macOS binary: the packager rewrote
  absolute Python and dynamic developer DuckDB references to `@rpath`, verification passed, the
  installer populated a clean prefix, and `env -u DYLD_LIBRARY_PATH -u
  DYLD_FALLBACK_LIBRARY_PATH <prefix>/bin/cdf version` printed `cdf 0.2.0-alpha.1`. This validates
  relocatable runtime staging and installation, but the hosted workflow remains authority for the
  release-only static DuckDB build.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: pass.
- Fast-core local equivalent: 399 kernel/contract/package/runtime tests passed with six intentional
  performance ignores; 44 CLI-core all-feature tests passed; formatting, generated CLI/docs
  freshness, shell parsing, workflow lint where installed, and `git diff --check` passed.
- `.10x/evidence/2026-07-26-p1-ws8-hosted-release.md`: hosted run `30196650532` passed all nine
  jobs at head `2928ee09`; `v0.2.0-alpha.1` publishes five checksummed target archives plus
  `SHA256SUMS`; the actual published aarch64 macOS artifact passed the shell installer and ran
  without local dynamic-library environment overrides.

## Review

Fresh-hat review rejected the first apparent fix because it merely packaged dynamic DuckDB and
would have preserved an avoidable release dependency. User direction ratified static DuckDB for
published artifacts. The second pass also found runner-native CPU flags and runner-absolute Python
linkage that ordinary tests did not falsify. Release builds now override local `target-cpu=native`,
link DuckDB statically, stage the genuine embedded-Python runtime, rewrite macOS linkage, verify
the staged executable, and include the Python license.

The final adversarial pass checked the failure classes that had escaped local fixtures: native
Windows dependency-tool discovery, macOS framework-signature invalidation after standalone
staging, absolute runtime paths, dynamic DuckDB leakage, checksum aggregation, generated artifact
omission, and installer dependence on the developer environment. The hosted run and subsequent
published-artifact install falsified none of those boundaries. Verdict: pass.

## Retrospective

The original artifact fixture proved archive mechanics but could not prove native linkage or
runtime portability. The effective release gate must execute on every promised target and then
test one published artifact through the public installer; merely uploading workflow artifacts is
not a release. Static DuckDB makes release builds intentionally expensive, so the static feature
belongs at the release composition boundary while ordinary development and benchmark builds keep
the downloaded prebuilt path. macOS framework executables also carry bundle signatures that become
invalid when staged alone; relocation must end with deterministic ad-hoc re-signing and strict
verification.
