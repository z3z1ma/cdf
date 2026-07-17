Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: crates/cdf-http/src/egress.rs, crates/cdf-source-rest/src/runtime.rs, crates/cdf-source-rest/src/driver.rs
Verdict: pass

# Shared REST transport review

## Findings

- Significant, resolved: neutral HTTP required mutable sends and REST serialized every request behind an adapter-wide mutex. The trait and dependency are now shared/concurrent; no poison/error compatibility branch remains.
- Significant, preserved: auth refresh is genuinely mutable and remains independently locked; removing it would permit duplicate/unsynchronized credential transitions.
- Minor, resolved: discovery, CLI, benchmark, conformance, and test call sites still advertised mutable transport authority after the trait changed. Strict Clippy found and removed every unnecessary mutable pass.
- Significant, subsequently closed: response bodies remained collected by blocking Reqwest. `.10x/tickets/done/2026-07-11-p3-g1-streaming-transport-byte-sources.md` closed the native async provider/body stream, and `.10x/tickets/done/2026-07-11-p3-b5-json-codecs.md` closed streamed decode.

## Verdict

Pass for the shared transport boundary. The change deletes false serialization at its neutral owner and preserves real state synchronization.

## Residual risk

Concurrent sends are now permitted, so concrete transports must honor the `Send + Sync` contract. All registered/test implementations compile under the supertrait and the permanent peak-concurrency law; provider-specific throttle/pool evidence remains G1/C4 work.
