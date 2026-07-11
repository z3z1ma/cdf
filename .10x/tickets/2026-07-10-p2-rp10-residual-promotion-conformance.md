Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/tickets/done/2026-07-10-p2-rp9-promotion-execution-recovery-gc.md, .10x/tickets/2026-07-08-p2-ws-i-conformance-parity-friction-suite.md

# P2 RP10 residual capture and promotion conformance

## Scope

Gate residual capture/promotion as destination- and source-neutral laws: exact residual round-trip, safe partial admission, unsafe quarantine, addressed correction strategies, crash recovery, replay, retention availability, and sampled-discovery integration.

## Acceptance criteria

- Deterministic Parquet/Arrow/JSON fixtures cover unknown field, scalar mismatch, nested mismatch, control-field failure, unsupported residual encoding, and clean rows.
- A sampled pin encounters unseen drift, loads conforming values plus residuals, then promotes through fresh discovery without source-dependent package replay.
- Postgres and DuckDB in-place corrections and Parquet sidecars pass one common correction-address law appropriate to their sheets.
- Crash/fencing/CAS scenarios prove the pin never advances before all required correction checkpoints.
- GC/tombstone cases prove availability reporting and refusal.
- Preview/run parity covers residual decisions visible to preview bounds.
- Golden packages and repeated promotion plans are byte deterministic.
- Coverage matrix, friction registry, and P2 parent evidence name these tests without overstating unrelated S1-S8 completion.

## Evidence expectations

Conformance matrix output, property results, golden hashes, live destination fixtures, runtime chaos, repeated determinism, full workspace/quality gates, and independent adversarial review.

## Explicit exclusions

No public-network dependency, distributed scheduler, source re-extraction backfill, or automatic promotion.

## Progress and notes

- 2026-07-10: Opened as the mandatory closure gate.
- 2026-07-10: Added the sampled-discovery-to-promotion end-to-end scenario. It exposed a data-loss bug in runtime effective-schema preparation: compatible fields unseen by the sampled pin were being added to the runtime projection, then dropped by pinned output authority instead of becoming residual candidates. Runtime observation now reconciles every physical schema against the immutable baseline with parse coercion disabled; safe extras remain outside the projection and flow into `_cdf_variant`, lossless physical widths still reconcile, and incompatible physical types remain file-level quarantine. The scenario proves two unseen `/score` values survive the sampled run as residuals, fresh exhaustive discovery produces an executable promotion, DuckDB addressed correction materializes the values, and every residual clears.

## Blockers

Depends on RP9.
