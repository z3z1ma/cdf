Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Source and destination extension invariant

CDF program work must optimize for the cost and correctness of adding one source or one destination, not for closing one ticket through a local shortcut. This is a P0 architectural closure invariant for P2, P1, and subsequent work.

A source- or destination-specific implementation is valid only at its adapter/driver boundary. Shared semantics belong in kernel contracts, compiler/runtime traits, plans, packages, receipts, checkpoints, capability sheets, and conformance laws. The generic orchestration path must consume those abstractions without branching on concrete source formats or destination names.

Every non-trivial integration review must ask:

- What files and crates would change to add one new source with the same capabilities?
- What files and crates would change to add one new destination with the same capabilities?
- Did the change add a concrete-source or concrete-destination branch to generic orchestration?
- Did behavior that should be falsifiable through a capability sheet or trait become an ad-hoc helper or free function?
- Are lifecycle, evidence, replay, and commit semantics expressed once through kernel/runtime contracts?
- Can conformance falsify a new adapter's claims without editing the conformance engine for that adapter?
- Did a ticket-specific convenience leak filesystem, CLI, executor, parser, or driver types below its proper boundary?

Repeated edits across generic orchestration for each driver, copied source pipelines, source-specific command branches, parallel receipt/checkpoint logic, and one-off helpers in shared crates are stop-line findings. They require repair in the owning ticket when in scope or a bounded P0 owner before the parent program may close. Passing focused tests does not waive this invariant.

Format-specific parsing and destination-specific physical commit code are expected inside adapters. The smell is not specificity itself; it is specificity controlling shared semantics outside the adapter boundary.
