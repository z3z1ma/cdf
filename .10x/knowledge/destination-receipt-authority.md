Status: active
Created: 2026-07-26
Updated: 2026-07-26

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
