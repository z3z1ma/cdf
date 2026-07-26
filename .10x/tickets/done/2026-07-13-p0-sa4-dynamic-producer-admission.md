Status: done
Created: 2026-07-13
Updated: 2026-07-25
Parent: .10x/tickets/done/2026-07-13-p0-fixed-schema-discovery-stream-admission.md
Depends-On: .10x/tickets/done/2026-07-13-p0-sa0-cold-discovery-final-plan-lifecycle.md, .10x/tickets/done/2026-07-13-p0-sa1-compiled-stream-admission-plan.md, .10x/tickets/done/2026-07-11-p3-h1-interop-measurement-copy-proof.md, .10x/tickets/done/2026-07-11-p3-h2-python-incremental-arrow-boundary.md

# P0 SA4: single-invocation dynamic producer admission

## Scope

Apply the cold bootstrap barrier and compiled stream admission to the currently registered dynamic-producer boundary, beginning with the existing neutral Python producer, so arbitrary user code executes once per partition absent retry/replay. Future Lua and WASM producers inherit this law when separately activated; they are not prerequisites for proving it.

## Non-goals

No new language runtime, Lua/WASM implementation, or connector-specific schema semantics.

## Acceptance criteria

- Optional cheap schema handshakes are explicit capabilities.
- Producers without handshakes start once; retained first batches flow downstream.
- Cancellation, retry, quarantine, and replay preserve invocation and package evidence semantics.
- Process/component counters prove no hidden discovery invocation.

## References

- `.10x/specs/schema-discovery-and-stream-admission.md`
- `.10x/tickets/done/2026-07-11-p3-h2-python-incremental-arrow-boundary.md`

## Assumptions

The same SA1 plan operation applies across native and dynamic sources.

## Journal

- 2026-07-18: Closure graph audit found SA0-SA3 complete but SA4 not executable: upstream dynamic-producer owners had not yet supplied the full incremental boundary needed to count one invocation, retain bootstrap batches, and continue the same producer under the compiled admission program. Marked this ticket blocked on H1/H2/H4/WIT rather than implementing a source-specific workaround.
- 2026-07-18: IX1 closed the neutral foreign-stream contract. SA4 remains blocked because measurement/copy proof, concrete Python migration, WASM cost/interface validation, and WIT recursive composite projection are still upstream of a source-agnostic dynamic producer admission implementation.
- 2026-07-19: H2 closed the concrete Python neutral producer migration. SA4 remains blocked only on H4 and the WIT recursive composite projection; it must reuse H2's neutral lifecycle rather than add Python-specific bootstrap code.
- 2026-07-25: Backlog grooming removed speculative WASM implementation from P3 closure. The completed neutral producer contract and Python implementation are sufficient to prove the bootstrap-barrier law without waiting for an unimplemented runtime. Future Lua/WASM producers must conform to the same law when activated.
- 2026-07-25: Added `ForeignSchemaAcquisition::{DeclaredHandshake, StreamBootstrap}` to the neutral producer descriptor. Python callable metadata now compiles an explicit declared handshake when a schema is present and `Discover` plus stream bootstrap when it is absent; the Python driver artifact version advanced to 2.0.0 with no compatibility decoder.
- 2026-07-25: Implemented the bootstrap barrier through the existing invocation-local `PreparedSourcePayloads` authority. Discovery starts the real Python producer, reads only the first Arrow schema, retains the first outcome and its live stream under the batch's existing memory lease, freezes the plan, and final resolution consumes that exact invocation once. Dropping an unused prepared invocation cancels its producer; executable partitions remain non-reopenable and retry-forbidden.
- 2026-07-25: The product test exposed that first lock generation and lock hydration omitted configured source references. Generalized locked-snapshot hydration to any compiled resource and admitted an exact configured reference into first lock generation. The cold Python run now pins once; later planning is metadata-only.
- 2026-07-25: Fresh adversarial control-flow review found two early bootstrap error paths that could drop a live stream without invoking its termination authority. Both now terminate and join before returning the primary error.

## Blockers

None. H1 and H2 established the neutral producer boundary and its concrete Python implementation.

## Evidence

- 2026-07-18 ticket/source audit:
  - `.10x/tickets/done/2026-07-11-p0-ix1-neutral-foreign-stream-contract.md` now supplies the neutral producer descriptor/outcome/control/terminal vocabulary.
  - `.10x/tickets/done/2026-07-11-p3-h2-python-incremental-arrow-boundary.md` now supplies the concrete incremental Python producer, cancellation/memory boundary, and runtime-resolved lane behavior.
  - `crates/cdf-python` implements the neutral producer lifecycle and supplies the concrete proof surface for the source-neutral bootstrap barrier.
  - Future language runtimes are consumers of this invariant, not prerequisites for defining or testing it.
- 2026-07-25 product lifecycle:
  - `cargo nextest run -p cdf-cli -E 'test(python_resource_without_schema_bootstraps_and_executes_one_invocation) | test(python_resource_plan_preview_run_and_replay_use_the_product_spine)' --no-fail-fast` passed 2/2. The no-handshake cold run records exactly one callable invocation, commits both retained bootstrap rows, writes the pin, and a subsequent plan does not invoke the callable. The declared-handshake plan remains invocation-free and its preview/run/replay path remains green.
  - `cargo nextest run -p cdf-python -p cdf-foreign-stream -p cdf-subprocess --no-fail-fast` passed 66/66. Existing cancellation/join, incremental generator, bounded memory, foreign terminal, replay-package, and subprocess lifecycle laws remain green.
  - `cargo nextest run -p cdf-cli -p cdf-project --no-fail-fast` passed 491/491, including cold REST retained-page reuse, fixed-schema file admission, replay/recovery, destination commits, and the new dynamic bootstrap path.
  - `cargo check --workspace --all-targets --all-features` passed.
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings` passed.
  - `cargo fmt --all -- --check` and `git diff --check` passed.

## Review

Pass after fresh adversarial control-flow review. The compiler/project layer sees only the neutral prepared-payload and producer capabilities; Python owns invocation creation, schema extraction, and continuation. No runtime or project branch names Python. The first batch retains its original ledger-backed memory lease rather than allocating or accounting a second buffer. Error, cancellation, retry, and replay behavior continue through the existing neutral foreign-stream and package authorities. Residual risk is limited to future dynamic runtimes, which must implement the same descriptor and prepared-invocation law when activated.

## Retrospective

The bootstrap-barrier law is source-neutral, but proof should proceed through a concrete registered producer. Waiting for speculative runtimes made the dependency graph circular; future producers should inherit the proven contract instead. The existing prepared-payload authority was the decisive reuse seam; adding another discovery cache or Python-specific project orchestration would have duplicated lifecycle ownership. Product testing found a lockfile omission that driver tests could not: source-reference compilation and declarative compilation are distinct front doors and both must pass through the same snapshot hydration authority.
