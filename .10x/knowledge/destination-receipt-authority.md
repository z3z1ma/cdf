Status: active
Created: 2026-07-26
Updated: 2026-07-31

# Destination receipt authority

CDF receipts separate typed logical commit authority from destination-physical evidence.
`cdf-package-contract::ReceiptDraft` is the one ordinary/correction assembly boundary. It maps the
typed request into destination, target, package, ordered segment acknowledgements, disposition,
idempotency token, schema, and correction identity; destinations supply receipt-id derivation,
physical acknowledgements, transaction/object metadata, counts, committed time, and their
executable verify clause. Migrations come only from the typed `CommitPlan` or
`DestinationCorrectionCommitPlan`, and request-plan drift fails before finalization.

## Verification parameters are evidence, not input

A verify clause exists so an independent verifier can interrogate the physical destination. Its
string parameters MUST NOT become a second execution or planning authority when a typed request
or plan field exists. This applies beyond receipt construction: package validation, duplicate
lookup, segment range insertion, mirror mutation, receipt-id derivation, and test helpers must all
read the typed value. A migration to typed authority is incomplete until all reads of the
displaced string representation have been audited.

The common finalizer may compare a present standard verify parameter with its typed value and
fail closed on contradiction. It does not derive the typed value from that parameter.

## Time authority

Receipt, correction, mirror, and staging Unix timestamps come from the destination runtime's
injected `ExecutionServices` host. A destination session fails closed when execution services are
absent. Replay preserves recorded receipt time and never recomputes it. Monotonic profiling
telemetry is a separate concern and cannot become commit evidence.

Test fixtures that call a runtime directly must bind services to that runtime, not merely store
them on a wrapper that production orchestration would later bind. Fixed-clock tests should
exercise at least one full ordinary and correction lifecycle, while replay and crash-window gates
prove the timestamp migration did not move the receipt/checkpoint boundary.

Destination resolution has one binding authority. A `DestinationDriver` constructs an unbound,
run-owned runtime; `DestinationRegistry` binds it once, then validates and installs the runtime's
post-bind lane capabilities because native resource demand may derive from the host. Higher
facades may remember a successful registry/facade bind so an exact clone is not rebound, but
wrapper metadata is never proof by itself. Rebinding to a genuinely different
`ExecutionServices` handle must replace the adapter's clock and every other invocation-local
authority before destination work begins.

Resource-owning adapters make rebinding a two-authority operation. An adapter may retain a native
scratch reservation when the incoming services expose the same spill-coordinator object, but it
must recompute memory- and CPU-derived native settings from the incoming services and replace the
complete services handle. Host-wrapper identity is neither necessary nor sufficient: decorated
hosts may share spill while changing clocks or memory authorities. Process-start native-resource
environment settings must not change after binding; DuckDB rejects a post-bind scratch-size change
instead of double-reserving or silently retaining contradictory limits.

## Preservation checklist

When migrating or adding a destination:

1. Build both ordinary and correction receipts through the common finalizer.
2. Keep receipt ID derivation, transaction/object metadata, verify statements, and serialized
   field values byte-for-byte compatible unless separately governed.
3. Retain typed package, token, schema, and ordered segment data through planning; never recover
   them from verify strings.
4. Treat collection order as serialized behavior. Use deliberately non-lexical identifiers in
   preservation tests when replacing map-derived values with typed vectors.
5. Preserve the previous sampling point when replacing a clock source so timing granularity does
   not change accidentally.
6. Scan production receipt, mirror, staging, and correction paths for direct wall-clock reads and
   scan all destination modules for reads of displaced string authority.
7. Run destination receipt/correction/duplicate/rollback tests plus generic
   committed-before-checkpointed and duplicate-replay gates.

## Concurrent same-token outcomes

Two same-token publishers may both pass their initial absence check. At least one must win the
immutable publication and return commit-bound verification. A slower racer may instead observe
the winner's already-verified manifest and take the ordinary duplicate path, whose outcome remains
`Independent` until orchestration verifies the returned receipt. Tests must accept both valid
schedules while requiring identical receipts, immutable manifest bytes, explicit duplicate
reporting for every independent loser, and successful independent readback. Requiring every racer
to report `VerifiedAtCommit` overconstrains scheduling and turns a correct duplicate observation
into a flaky failure.
