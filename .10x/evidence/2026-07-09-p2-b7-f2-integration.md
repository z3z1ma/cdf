Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-b7-rest-observed-reconciliation.md, .10x/tickets/done/2026-07-09-p2-ws-f2-s7-key-disposition-experience.md, .10x/tickets/2026-07-09-p2-ws-g2-type-mismatch-diagnostics.md

# P2 B7 and F2 integration evidence

## What was observed

- Declarative REST pages use the shared observed-first JSON reconciliation path, preserve physical/source-name provenance, localize row drift, require one stable reconciliation plan across pages, and carry exact coercion evidence through the existing dual-channel validation gate.
- Runtime dependencies cannot grant parse or lossy authority. Strict REST defaults quarantine parse-like row drift, reject unsupported signed-to-unsigned mappings with allowance guidance, and cannot emit `CoercedByPolicy` or `LossyAllowed` package evidence.
- Append resources validate, plan, preview, and run without keys or key suggestions. Merge without `merge_key` fails once before source contact or project mutation and names both valid fixes.
- Current append scaffolds and documentation do not nudge operators toward invented keys; intentional merge examples retain their keys.

## Procedure

- `cargo fmt --all -- --check` — passed.
- `git diff --check` — passed.
- `cargo check --workspace --all-targets --all-features --locked` — passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` — passed.
- `cargo nextest run --workspace --all-features --locked --status-level fail --final-status-level fail` — passed 773/773 tests, zero skipped.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast` — passed.
- `cargo doc --workspace --all-features --no-deps --locked` — passed.
- `cargo deny check` — passed with the repository's existing policy-allowed duplicate-version warnings.
- `cargo audit --quiet` — passed with only the repository-allowed `RUSTSEC-2024-0436` unmaintained `paste` warning.
- Every changed tracked/untracked file in this batch was scanned individually with `gitleaks dir --no-banner --redact`; no leaks were found.

## Acceptance mapping

- B7 observed schema, width/decimal widening, row-local quarantine, source-name and physical provenance, extras, stable multi-page plans, cursor advancement, and exact package evidence are covered by `cdf-declarative`, CLI, and conformance tests in the 773-test run.
- B7 strict authority is covered by temporal parse failure, signed-to-unsigned rejection, and the production CLI regression proving runtime defaults cannot authorize parse/lossy verdicts.
- F2 keyless append and merge-key remediation are covered for file and REST resources across validate/plan/preview/run, including JSON/human error shape, zero HTTP requests, and unchanged project-tree assertions.
- F2 scaffold/document claims are supported by the repository audit and scaffold regression.

## What this supports or challenges

This supports closure of B7 and F2. It challenges any future design that passes semantic type allowances through runtime-only dependencies; `.10x/knowledge/type-policy-authority.md` records the durable invariant.

## Limits

G2 diagnostics are not implemented: their probe bounds, row-local validation rendering, and explicit Tier-0 type-policy surface remain blocked. The standalone S7 golden-path conformance scenario is still pending, and this evidence does not promote S7 or close WS-F, WS-G, WS-I, or P2. Public-network TLC behavior was not exercised by this batch.
