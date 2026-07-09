Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-e2-http-file-runtime-and-discovery.md, .10x/tickets/done/2026-07-09-p2-ws-g1-source-diagnostics-and-deep-validate-foundation.md, .10x/tickets/done/2026-07-09-p2-ws-b4-widening-property-conformance.md
Verdict: pass

# P2 E2/G1/B4 batch review

## Target

Adversarial closure review for the E2 HTTPS file runtime/discovery slice, the G1 source diagnostics/deep-validate foundation, and the B4 widening property conformance slice.

## Findings

- Significant residual, not a blocker for these children: E2 only proves deterministic single-file HTTPS Parquet. It intentionally does not implement HTTP template/glob enumeration, S3/GCS/Azure object-store transports, compression, remote multi-file manifest incrementality, or the live TLC S1/S2 session. Those remain on the WS-D/E/H/I parent graph.
- Significant residual, not a blocker for G1: `cdf validate --deep` now runs useful current compiler-front-end checks, but the full P2 deep-validate promise still depends on future cloud transports, compression, Python/WASM/future source probes, and broader type-mismatch remediation wording.
- Minor residual: `cargo geiger` was not usable as a completed gate in this batch because the current invocation was impractically slow. The changed code introduces no `unsafe` and the direct unsafe/FFI scan plus CodeQL reduced the soundness risk enough for this scoped closure.
- No determinism concern found: package identity fixture changes are explained by the newly recorded file `sha256` metadata in live-run plan evidence, and conformance/golden reruns passed under `cargo test`, `nextest`, and `llvm-cov`.
- No redaction concern found in the reviewed surfaces: HTTP file transport debug tests, CLI renderer tests, gitleaks current-tree scan, and CodeQL results did not expose new secret leakage.

## Verdict

Pass for the scoped E2, G1, and B4 child tickets. The P2 parent remains open and must not treat this batch as S1/S2/S8 closure.

## Residual risk

The next highest-risk P2 path is `cdf add` plus public HTTPS Parquet ergonomics: without it, users still cannot complete S1 with the normative two commands. Remote glob enumeration and compression are the next file-source correctness risks after that.
