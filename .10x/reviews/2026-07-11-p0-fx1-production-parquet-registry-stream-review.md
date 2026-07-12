Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/evidence/2026-07-11-p0-fx1-production-parquet-registry-stream.md
Verdict: concerns

# Review: production native Parquet registry stream

## Assumptions tested

- The CLI composition root, not generic orchestration, owns first-party codec enrollment.
- A registered codec can execute local and remote-spooled input without receiving transport types.
- The injected I/O runtime can stream accounted batches into the engine without collection.
- Compiler artifacts are not themselves executable resources.
- The architecture change must be measured against the immediately preceding TLC control.

## Findings

No critical correctness or layering finding remains in the registered path. `FileSourceDriver` now receives a fully composed runtime rather than constructing transports or codecs. The generic file runtime selects any registered format by id and the native driver sees only `ByteSource`, decode requests, memory, and cancellation. The old dependency-free execution constructors and direct compile-layer file open functions were removed, and the CLI local/HTTP conformance paths pass.

One significant performance concern remains: median CPU rose from roughly 1.62 to 1.80 seconds and median wall from 1.53 to 1.63 seconds in the small three-sample TLC comparison. The architecture is required, but B2 must eliminate neutral local range-copy/open overhead rather than normalize this regression. No fallback to parser-specific paths is acceptable.

One expected migration concern remains: unregistered formats still fall through to `cdf-formats`. This is not accepted compatibility policy; it is unfinished FX1 scope with concrete B3/B4/B5 children. The fallback and closed enum must be deleted when those codecs enroll.

## Verdict

Concerns raised. The production Parquet boundary is correct and should remain, but FX1/B2 cannot close until the local roofline is recovered and the monolithic fallback is gone.

## Residual risk

The current remote spool download is still synchronous at the old transport boundary, so download/decode overlap is not yet achieved. G1/G2 own replacement with the async byte-source transport while preserving sequential full-scan policy.
