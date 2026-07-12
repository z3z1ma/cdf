Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-source-files/src/runtime.rs
Verdict: concerns

# Accounted async full-scan spool review

## Findings

- No critical/high finding: the spool is generation-bound by its provider, pre-reserves network chunks and disk, verifies exact bytes/hash, preserves cancellation checks, and retains its reservation through decode.
- Significant: decode does not tail the growing spool. This removes blocking transport work but not download/decode latency overlap.
- Significant: weak HTTP still uses the blocking compatibility transfer because it requires end re-attestation; an async weak-generation spool provider remains G1 work.
- Significant: transformed remote inputs currently stage compressed input and transformed output separately. B1/G3 must replace this with byte-source → transform → codec/spool composition under one disk plan.
- Significant: no chaos fixture kills the transfer at each write/flush boundary or proves partial spool cleanup and reservation release.
- Minor: temporary-file durability is intentionally not fsynced because the spool is a disposable optimization, not package evidence. Cache promotion would require a separate durability contract.

## Verdict

Concerns raised. This is the correct bounded intermediate architecture for full scans, but G2/G3 remain open for early decode, chaos, transforms, telemetry, and performance evidence.

## Residual risk

Real CDN/object-store streaming chunk behavior and disk contention are not represented by the in-memory provider fixture.
