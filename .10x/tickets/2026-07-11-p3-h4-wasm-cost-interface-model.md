Status: blocked
Created: 2026-07-11
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-h-interop-boundaries.md
Depends-On: .10x/tickets/done/2026-07-11-p3-h1-interop-measurement-copy-proof.md

# P3 H4: prospective WASM stream cost and interface model

## Scope

Review/validate the WIT foundation against the neutral foreign-stream contract and record a versioned prospective cost model for arbitrary-chunk IPC, startup/compile/instantiate, host calls, memory copies, interruption/fuel, sandbox memory, and capability mediation without implementing a host.

## Acceptance criteria

- WIT can express descriptor/plan/errors, incremental IPC, cancellation, typed control/state, and host-mediated capabilities or names exact required revisions.
- Every cost cell cites named evidence/prototype or remains explicitly unknown.
- No executable/native-equivalent WASM claim appears.
- Later Wasmtime work has concrete benchmark/conformance criteria and no second runtime API.

## Evidence expectations

WIT validation/review, cost worksheet with provenance/limits, reference/prototype bias labels, and adversarial sandbox/performance review.

## Explicit exclusions

No Wasmtime dependency/host, guest SDK, registry, signing, or throughput acceptance claim.

## Blockers

H1 is done and the WIT foundation is an input rather than an execution dependency. Closure is semantically blocked: D-26's `open(partition) -> stream<u8>` shape does not express the neutral boundary's ordered typed control and independently observable terminal status; the performance-oriented multi-handle alternative would supersede that shape and needs a ratified ordering/completion/cancellation state machine. Recursive value limits/errors and concrete HTTP/secrets/log broker calls remain unratified. Current Wasmtime also requires explicit close/abort and may require Store teardown; drop is not cancellation authority.

## References

- `.10x/specs/foreign-stream-interop.md`
- `.10x/tickets/2026-07-05-wasm-components-registry-signing.md`
- `.10x/tickets/2026-07-08-wasm-wit-interface-foundation.md`
- `.10x/research/2026-07-18-wasi03-stream-cost-interface-model.md`

## Journal

- 2026-07-18: Corrected the dependency inversion that made this cost/interface review wait for the artifact it is meant to validate. The failed WIT foundation and completed recursive-value research are sufficient inputs for the cost review and for locating the unresolved interface decisions; H4 remains strictly prospective and adds no Wasmtime dependency or product implementation.
- 2026-07-18: Inspected WASI 0.3, Wasmtime 46/current API documentation, the Component Model WIT/Canonical ABI, H1-H3 evidence, and the neutral foreign-stream contract. Recorded the versioned worksheet in `.10x/research/2026-07-18-wasi03-stream-cost-interface-model.md`. Every numeric cell without an executable prototype remains explicitly unknown.
- 2026-07-18: Fresh adversarial review rejected closure in one pass. It proved the initial drop-cancels claim false for current Wasmtime, identified the unratified D-26 supersession and incomplete cross-stream state machine, and found missing AOT trust, handle/task/control/broker quotas, host-call deadlines, and tenant-remanence evidence. The research now distinguishes established requirements from recommended candidates, corrects the cancellation and version facts, expands the security/performance matrix, and leaves this ticket blocked rather than turning recommendations into authority.

## Evidence

- Primary-source URLs, inspection date, supported qualitative statements, and explicit limits are recorded in `.10x/research/2026-07-18-wasi03-stream-cost-interface-model.md`.
- The model distinguishes established WIT requirements from unratified candidates and names the exact remaining decisions for recursive values, IPC/control/terminal structure, ordering, cancellation, brokered capabilities, and shared schema reconciliation. This is blocker evidence, not acceptance evidence for the first criterion.
- The worksheet separates cold compile, AOT load, instantiation, host calls, byte crossing, IPC encode/decode, interruption, memory, capability mediation, and cancellation. No number is inferred from marketing prose or a different workload.
- The later executable matrix specifies comparative modes, sizes, concurrency, memory categories, failure boundaries, and identity laws without inventing a throughput threshold before measurement.
- `git diff --check -- .10x/research/2026-07-18-wasi03-stream-cost-interface-model.md .10x/tickets/2026-07-11-p3-h4-wasm-cost-interface-model.md` is the local source-quality gate; no Rust or dependency graph is changed by H4.

## Review

Fresh adversarial sandbox/performance review verdict: **fail** for closure. Critical findings were technically false cancellation semantics and an unratified replacement of D-26's bare-stream contract. Significant findings covered incomplete recursive/broker/control revisions; incomplete cross-stream ordering/fairness/completion; stable-version overclaiming; unsafe AOT-cache provenance; incomplete task/handle/control/broker memory and deadline quotas; and pooled-instance tenant isolation. Minor findings covered unproved `Bytes` stream lowering, floating API citations, and missing falsification cells. The record repair accepts every finding and does not claim closure. No product or dependency change was made.

## Retrospective

The original graph put interface implementation ahead of lifecycle and cost validation even though H4 explicitly allowed required revisions as its outcome. Reviewing the failed artifact first was both faster and safer: it avoided freezing a versioned WIT around a bare byte stream that cannot independently report terminal status or order typed state, and it kept all unmeasured performance claims visibly unknown. The deeper lesson is that WASI async handles are mechanics, not CDF lifecycle authority; explicit close/abort/Store teardown and a complete state machine must be proven against the pinned implementation.
