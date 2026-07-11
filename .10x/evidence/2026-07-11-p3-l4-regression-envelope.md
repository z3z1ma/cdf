Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-10-p3-ws-l4-ci-envelope-generation.md

# P3 L4 regression and envelope evidence

## What was observed

The lab now compares only equal dataset/workload/timed-region/dependency/host/toolchain/I/O/reference authority while allowing CDF revision to be the measured delta. It refuses row/logical-byte/physical-byte drift, requires summaries to reproduce exactly from raw samples, marks MAD strictly above 10% inconclusive, and fails wall-time movement strictly above 10%. Missing or non-observed cells never become passes.

Baseline installation stores canonical reports by SHA-256, requires an existing `.10x/evidence/*.md` record that resolves inside the evidence directory, appends history, verifies every prior report/evidence/digest on each replacement, and refuses tampering. The generated envelope includes sanitized host/effective-resource authority, all ten P3 targets, absolute observations, same-work roofline ratios, overhead, RSS, bias, unavailable cells, and profile links. The committed document is explicitly marked pre-baseline and authorizes no claim.

## Procedure and results

```text
CARGO_INCREMENTAL=0 cargo test -j1 -p cdf-benchmarks --locked
CARGO_INCREMENTAL=0 cargo test -j1 -p cdf-benchmarks --test lab_policy --locked
CARGO_INCREMENTAL=0 cargo clippy -j1 -p cdf-benchmarks --all-targets --locked -- -D warnings
cargo fmt --all -- --check
git diff --check
ruby YAML.safe_load over fast-quality.yml slow-quality.yml performance-lab.yml
gitleaks detect --no-git --source <changed-source-tree> --no-banner --redact
```

- Twenty benchmark tests passed across provider, fixtures, policy, and runner targets. The final policy rerun passed all four comparator/baseline/envelope tests after baseline-history tamper hardening.
- Boundary tests prove exactly 10% regression passes, above 10% fails, exactly 10% MAD remains comparable, above 10% becomes inconclusive, and host/mode/fixture/schema/reference/work-byte drift refuses comparison.
- Baseline tests prove missing evidence rejection, two-version preservation, current digest authority, and tampered-history rejection.
- Generated envelope output exactly matches `docs/performance-envelope.md`; existing L1 report/catalog canonical hashes remain unchanged.
- All benchmark targets passed Clippy with warnings denied; formatting/diff checks passed. Ruby parsed all three workflow YAML files.
- Source-only Gitleaks scans passed independently for `crates/cdf-benchmarks`, `.10x`, `docs`, and `.github`. A whole-worktree scan was deliberately stopped because it descended into ignored build/database artifacts and did not match CI's tracked-source boundary.

## CI shape

Fast CI was not expanded. Broad slow-quality no longer repeats benchmark execution and excludes `cdf-benchmarks` from its nextest workspace pass. A dedicated weekly/manual performance workflow owns benchmark policy tests, legacy compatibility timing, and artifact upload. No workflow was polled to establish this evidence.

## Limits

The generated document is a pre-baseline fixture, not a green envelope or marketing claim. L5 must replace it from recorded distributions. The scheduled `full` suite is the current long tier; the 100 GB/1 TB constant-memory runtime stress gate cannot truthfully execute until P3 WS-A/F builds the bounded pipeline and ledger, and remains their acceptance law rather than a fake L4 pass.
