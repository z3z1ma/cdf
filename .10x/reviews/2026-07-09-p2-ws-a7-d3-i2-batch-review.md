Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-a7-schema-pin-show-diff-cli.md, .10x/tickets/done/2026-07-09-p2-ws-d3-file-manifest-incremental-noop.md, .10x/tickets/done/2026-07-09-p2-ws-i2-preview-run-parity-and-golden-path-matrix.md
Verdict: pass

# P2 A7/D3/I2 batch review

## Target

The review covers the A7 schema pin/show/diff CLI slice, the D3 local append `FileManifest` incrementality/no-op slice, and the I2 P2 conformance matrix/parity foundation.

## Findings

- Pass: A7 uses the generic discovery dispatcher instead of reintroducing a Parquet-only schema path. The tests cover local Parquet pin/show/diff, REST no-write diff and redaction, and Postgres pin/redaction. The explicit no-new-lockfile behavior is acceptable for this slice because the ticket only promised lockfile updates where the current project model supports them.
- Pass: D3 keeps the managed incrementality rule scoped to append file resources. Replace disposition still plans all files, unchanged append runs avoid package/destination/checkpoint writes, and changed/new files update the committed manifest without dropping unchanged prior entries.
- Pass: I2 is honest scaffolding. It names S1-S8 and the eighteen frictions without pretending the final P2 golden paths are complete, and it makes unsupported or pending cells explicit.
- Concern accepted: `validate_partition` now requires file identity metadata for planned file partitions. Current planner/conformance paths populate it, but legacy hand-authored pre-D3 plan JSON without `sha256` or `etag` will fail closed rather than being treated as incremental evidence. This is consistent with the file-manifest decision because replay/incrementality must not guess identity.
- Residual: public HTTPS file ingestion, cloud stores, compression, `cdf add`, deep validation diagnostics, final golden-path conformance, and the S1+S2 recording remain open under P2. This batch must not be described as P2 completion.
- Residual: CodeQL still reports the pre-existing P1 backfill fake-secret fixture findings in `crates/cdf-cli/src/tests.rs`; the owner is `.10x/tickets/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.
- Residual: OSV still reports the already-ratified `paste` advisory. The advisory is not introduced by this batch.

## Verdict

Pass for the scoped A7, D3, and I2 tickets. The evidence is sufficient for child closure, while the P2 parent remains open with major exit criteria still active.

## Residual risk

The highest residual risk is semantic drift between partial local-file incrementality and the upcoming remote/public file runtime: E2 must reuse the same file identity model rather than inventing a parallel one. The second risk is CLI/schema snapshot ergonomics around projects without an existing lockfile; H2 `cdf add` should make that path natural instead of expanding A7 retroactively.
