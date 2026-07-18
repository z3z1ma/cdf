Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-transform-zstd, .10x/tickets/done/2026-07-11-p3-b1-streaming-byte-transforms.md
Verdict: pass

# Native zstd driver adversarial review

## Findings

- No critical or significant correctness finding in the leaf-driver slice.
- Window memory is not hidden: the decoder rejects frames above a 64 MiB window and reserves 68 MiB before constructing/using the native context. This is safe and constant-memory, but conservative for high partition concurrency.
- Concatenated-frame handling explicitly reinitializes the decoder only when unread input follows a completed frame. EOF before frame completion is a data error; EOF after completion is success.
- Expansion accounting is shared with gzip and is exact at every frame boundary.
- The crate imports no source, transport, project, CLI, destination, or sibling codec implementation.

## Verdict

Pass for the native zstd milestone. The implementation is correct and ledger-honest at its declared window ceiling.

## Residual risk

Exact frame-header-driven admission is required to avoid reserving 68 MiB for small-window frames; reference throughput and malformed-frame fuzzing are also unmeasured. These remain B1 acceptance obligations, so B1 must stay open.
