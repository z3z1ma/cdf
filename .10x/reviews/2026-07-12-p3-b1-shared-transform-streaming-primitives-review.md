Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-runtime/src/format.rs, crates/cdf-transform-gzip/src/lib.rs
Verdict: pass

# Shared transform primitive review

## Findings

- No critical or significant finding.
- The cursor abstraction has two consumers immediately planned (gzip and zstd) and owns a cross-codec invariant, so it is not a speculative one-implementation interface.
- The expansion guard does not guess a semantic policy: all ceilings and chunk grace come from the already-validated transform request.
- Exact ratio enforcement remains the codec's responsibility at its real frame/member/terminal boundaries; the helper does not invent framing semantics.

## Verdict

Pass. The refactor reduces codec-local code and prevents memory/expansion behavior drift without moving parser concerns into the runtime.

## Residual risk

The next independent transform is the practical proof that these APIs generalize; B1 remains open until that and product composition land.
