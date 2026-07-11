# Change-Set Driven Verification

## Operating Frame

The Rust coding agent improves critical Rust repositories through objective self-verification loops.

The unit of work is a **change set**.

A change set may be:

* the active working tree diff
* the staged diff
* the latest commit
* a commit range
* a branch relative to its integration base
* a direct change on the integration branch
* a release candidate

The agent evaluates the change set, identifies the quality vectors it touches, selects the smallest sufficient verification profile, runs the relevant tools, and reports the evidence.

Use Git state to understand scope:

```bash
git status --short
git branch --show-current
git rev-parse --show-toplevel
git diff --name-only --diff-filter=ACMR
git diff --cached --name-only --diff-filter=ACMR
git diff --stat
git diff --cached --stat
```

For a branch-relative change set:

```bash
BASE="$(git merge-base HEAD origin/main 2>/dev/null || true)"
if [ -n "$BASE" ]; then
  git diff --name-only --diff-filter=ACMR "$BASE"...HEAD
  git diff --stat "$BASE"...HEAD
fi
```

For the latest committed change set:

```bash
git show --name-only --diff-filter=ACMR --format= HEAD
git show --stat --format=short HEAD
```

For an explicit commit range:

```bash
git diff --name-only --diff-filter=ACMR <base>...<head>
git diff --stat <base>...<head>
```

The verification question is:

```text
What quality vectors did this change set touch, and what is the smallest sufficient tool profile that covers those vectors?
```

---

## Profile Selection

Use fast checks for every meaningful change set.

Escalate when the change set touches higher-risk vectors.

| Changed vector                                                              | Verification profile     |
| --------------------------------------------------------------------------- | ------------------------ |
| Local implementation change                                                 | Micro Loop               |
| Ordinary multi-file change                                                  | Standard Change-Set Loop |
| `Cargo.toml`, features, optional deps, target `cfg`                         | Feature/Manifest Loop    |
| `Cargo.lock`, dependency versions, dependency policy                        | Dependency Loop          |
| Public API, exported types, proc macros, documented behavior                | Public API Loop          |
| `unsafe`, FFI, aliasing, pinning, atomics, concurrency primitives           | Unsafe/Soundness Loop    |
| Auth, secrets, filesystem, subprocesses, network input, SQL, untrusted data | Security Loop            |
| Parsers, codecs, protocols, binary formats, deserialization                 | Input-Boundary Loop      |
| Tests added, deleted, weakened, or moved                                    | Test-Quality Loop        |
| Hot paths, allocation pressure, binary size, benchmarks                     | Performance/Size Loop    |
| Release candidate, large integration, scheduled validation                  | Deep Loop                |

---

## Micro Loop

Use after small local edits.

```bash
cargo fmt --all
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

Then run the smallest relevant test target:

```bash
cargo nextest run -p <package> --locked
```

Fallback:

```bash
cargo test -p <package> --locked
```

For a known test:

```bash
cargo nextest run -p <package> <test_name> --locked
# or
cargo test -p <package> <test_name> --locked
```

When CLI definitions or error mappings change, verify the committed generated references:

```bash
cargo run -p cdf-cli --locked --features cli-artifacts --bin cdf-generate-cli-artifacts -- --docs-dir docs --docs-only --check
```

---

## Standard Change-Set Loop

Use for normal feature work, bug fixes, refactors, and behavior-preserving cleanup.

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo check --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo nextest run --workspace --locked
cargo test --workspace --doc --all-features --locked --no-fail-fast
cargo doc --workspace --all-features --no-deps --locked
```

Fallback when nextest is unavailable:

```bash
cargo test --workspace --all-targets --locked --no-fail-fast
```

`nextest` is the preferred unit/integration test runner when available. `cargo test --doc` remains separate because doctests are a distinct executable documentation surface.

---

## Feature/Manifest Loop

Use when the change set touches features, optional dependencies, target-specific configuration, workspace structure, crate membership, or `Cargo.toml`.

```bash
cargo check --workspace --all-targets --locked
cargo check --workspace --all-targets --all-features --locked
cargo check --workspace --all-targets --no-default-features --locked
```

When the feature graph warrants matrix validation and `cargo-hack` is available:

```bash
cargo hack check --workspace --all-targets --each-feature --locked
```

Use powerset validation for compact feature graphs, release candidates, or feature interaction work:

```bash
cargo hack check --workspace --all-targets --feature-powerset --locked
```

`cargo hack` expands feature coverage. It is not a substitute for behavioral tests.

---

## Dependency Loop

Use when dependency versions, dependency features, lockfiles, dependency policy, or supply-chain metadata changed.

```bash
cargo check --workspace --all-targets --locked
cargo nextest run --workspace --locked
cargo deny check
cargo audit
```

When the repository uses cargo-vet:

```bash
cargo vet
```

For scheduled or release validation:

```bash
osv-scanner scan source -r .
```

Keep dependency movement intentional, minimal, and auditable.

---

## Public API Loop

Use for exported types, traits, trait impls, macros, proc macros, feature-gated APIs, error types, documented behavior, or semver-relevant behavior.

```bash
cargo check --workspace --all-targets --all-features --locked
cargo test --workspace --doc --all-features --locked --no-fail-fast
cargo doc --workspace --all-features --no-deps --locked
cargo semver-checks --baseline-rev origin/main
```

For proc macros and type-level APIs, run the repository’s compile-fail or UI tests through the normal test command.

---

## Unsafe/Soundness Loop

Use when the change set touches `unsafe`, FFI, raw pointers, `MaybeUninit`, `transmute`, custom allocation, pinning, atomics, interior mutability, `Send`, `Sync`, or concurrency primitives.

Inventory the soundness surface:

```bash
rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|Unpin|Send|Sync" .
```

When available:

```bash
cargo geiger --all-features
```

Run targeted behavioral tests:

```bash
cargo nextest run -p <package> --locked
```

Run Miri where feasible:

```bash
cargo +nightly miri test -p <package> --all-features
```

Use `cargo-careful` or sanitizers when they fit the codebase and platform better:

```bash
cargo +nightly careful test -p <package> --all-features
```

Every changed unsafe boundary needs a clear safety invariant and verification appropriate to the executed paths.

---

## Security Loop

Use for authentication, authorization, secrets, filesystem boundaries, subprocess execution, SQL/query construction, network input, untrusted input, dependency changes, unsafe code, or externally reachable behavior.

```bash
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo deny check
cargo audit
gitleaks dir --no-banner --redact .
```

When repository Semgrep rules exist:

```bash
semgrep scan --config .semgrep.yml --error .
```

Use CodeQL for security-sensitive change sets, release candidates, and scheduled deep validation.

---

## Input-Boundary Loop

Use for parsers, codecs, protocols, binary formats, compression/decompression, serialization, deserialization, and untrusted inputs.

```bash
cargo nextest run -p <package> --locked
```

When fuzzing is already configured:

```bash
cargo +nightly fuzz list
cargo +nightly fuzz run <target> -- -max_total_time=60
```

Crashes, panics, hangs, timeouts, and sanitizer findings become regression-test candidates.

---

## Test-Quality Loop

Use when tests changed or when the change set fixes a subtle behavioral bug.

```bash
cargo nextest run --workspace --locked
cargo test --workspace --doc --all-features --locked --no-fail-fast
cargo llvm-cov --workspace --all-features --locked --summary-only
```

For critical touched logic:

```bash
cargo mutants --workspace
```

Coverage and mutation testing are test-quality signals. They guide attention toward weak behavioral evidence.

---

## Performance/Size Loop

Use when the change set touches hot paths, allocation behavior, binary size, build footprint, benchmarked code, CLIs, WASM, embedded targets, serverless artifacts, or performance-sensitive runtime behavior.

```bash
cargo bench --workspace --locked
```

For binary size:

```bash
cargo bloat --release -n 20
```

Use profilers after benchmark or production evidence identifies a hotspot.

---

## Scheduled / Release / Integration Deep Loop

Use for release candidates, large integrations, broad refactors, scheduled validation, or change sets that touch multiple high-risk vectors.

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo check --workspace --all-targets --all-features --locked
cargo check --workspace --all-targets --no-default-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo nextest run --workspace --locked
cargo test --workspace --doc --all-features --locked --no-fail-fast
cargo doc --workspace --all-features --no-deps --locked
cargo deny check
cargo audit
cargo semver-checks --baseline-rev origin/main
cargo llvm-cov --workspace --all-features --locked --summary-only
gitleaks git --no-banner --redact .
```

Add configured or risk-triggered deep tools:

```bash
cargo hack check --workspace --all-targets --feature-powerset --locked
cargo vet
osv-scanner scan source -r .
semgrep scan --config .semgrep.yml --error .
codeql database create reports/ai-quality/codeql-db --language=rust --source-root . --overwrite
codeql database analyze reports/ai-quality/codeql-db codeql/rust-queries --format=sarif-latest --output=reports/ai-quality/codeql-rust.sarif
cargo +nightly miri test --workspace --all-features
cargo +nightly careful test --workspace --all-features
cargo mutants --workspace
cargo bench --workspace --locked
cargo bloat --release -n 50
```

---

## Final Report

Each agent run ends with a compact evidence report.

```text
Rust Quality Report

Decision:
- ACCEPT / REVISE / REJECT

Change Set:
- Scope:
- Files changed:
- Commits inspected:
- Base/reference point:
- Branch:
- Working tree state:
- Staged changes:
- Public API affected:
- Unsafe affected:
- Dependency graph affected:
- Feature graph affected:

Profile Used:
- Micro / Standard Change-Set / Feature-Manifest / Dependency / Public API / Unsafe-Soundness / Security / Input-Boundary / Test-Quality / Performance-Size / Deep

Changed Risk Vectors:
- <compile | tests | features | dependencies | API | unsafe | security | performance | docs>

Commands Run:
- <command>
  - exit code:
  - result:

Commands Skipped:
- <tool>
  - reason:
  - remaining risk:
  - recommended trigger:

Findings:
- <tool> | <severity> | <path:line> | <finding> | <status>

Hard Blockers:
- None
or
- <finding>

Decision Rationale:
- <why the selected profile covered this change set>
- <what remains unverified, if anything>
```

The agent should be strict, practical, and evidence-driven.

The goal is changed code plus a concise proof trail that the relevant quality vectors were preserved or improved.
