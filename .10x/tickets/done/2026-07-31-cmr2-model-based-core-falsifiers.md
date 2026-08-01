Status: done
Created: 2026-07-31
Updated: 2026-07-31
Parent: `.10x/tickets/done/2026-07-31-connector-mode-readiness-program.md`
Depends-On: `.10x/tickets/done/2026-07-31-cmr1-reliable-deep-quality-certificate.md`

# Add model-based core falsifiers

## Scope

Add bounded generative tests for deterministic package identity across execution-shape variation
and for receipt-gated settlement across crash/retry/duplicate recovery sequences, using explicit
reference models and the current production authorities.

## Non-goals

- No TLA+/Kani/Loom program, distributed scheduler model, or production state-machine rewrite.
- No new product semantics or weakening of current deterministic and checkpoint contracts.
- No unbounded fuzz campaign in ordinary pull-request checks.

## Acceptance criteria

- Generated equivalent logical inputs vary batch/partition boundaries and permitted completion or
  scheduling order while asserting identical canonical package/segment identity.
- Generated settlement sequences vary durable receipt, checkpoint proposal/commit, crash/reopen,
  duplicate replay, and stale/tampered inputs against a small explicit reference model.
- The settlement model never admits a checkpoint before a verified matching receipt, never
  accepts conflicting duplicate authority, and converges under valid recovery.
- Case counts and shrink behavior are deterministic and bounded for scheduled CI.
- Focused mutation or deliberately faulty self-tests demonstrate that the harness detects at least
  the core fences it claims to protect.

## References

- `.10x/tickets/done/2026-07-31-connector-mode-readiness-program.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/deterministic-parallel-scheduler.md`
- `.10x/knowledge/product-integration-and-closure-gate.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/knowledge/canonical-arrow-bitmap-identity.md`

## Assumptions

- Record-backed: existing fixed failpoint and chaos tests remain valuable examples but do not
  generate multi-action sequences.
- Record-backed: randomized tests must use fixed reproducible seeds/case limits and remain
  evidence within their generated domain rather than a universal correctness claim.

## Journal

- 2026-07-31: Shaped as two narrow falsifiers around CDF's most consequential invariants, not a
  request to model the whole runtime.
- 2026-07-31: Activated after CMR1 reached a pushed corrective head and its aggregate hosted run
  began. Work in progress adds fixed-seed, bounded reference-model tests for execution-shape
  identity and receipt-gated settlement; validation and any falsified production fences remain
  part of this ticket before handoff.
- 2026-07-31: Added a 12-case execution-shape property varying logical input, source rechunking,
  completion schedule, and jobs 1-8. It compares decoded segment rows plus package hash, segment
  catalog, lineage, execution profile, segment positions, and terminal quarantine authority. Its
  deliberate faulty snapshot control proves that the asserted identity surface is live.
- 2026-07-31: The execution falsifier found a real canonical-identity defect: byte-aligned Arrow
  Boolean and validity slices could retain logically out-of-range bits, which Arrow IPC then wrote
  as padding. Canonical microbatching now copies only slices whose bit offset, all-valid retained
  null mask, or dirty trailing padding requires normalization; direct IPC-byte regressions cover
  both Boolean values and validity masks.
- 2026-07-31: Added a 24-case explicit settlement model over absent/primary/alternate receipts and
  missing/proposed/committed checkpoints. Generated action sequences exercise receipt recording,
  proposal, commit, crash/reopen, duplicate replay, stale/tampered authority, and convergence
  against durable SQLite state. A deliberately unrecorded conflicting commit is rejected by the
  model self-test.
- 2026-07-31: The frozen delegated review found one high nested-identity gap: outer
  `take_record_batch` repairs row-level bitmaps, but Arrow dictionary take retains its values.
  Canonicalization now recursively rebuilds dirty nested children after the outer take/concat, and
  a `Dictionary<Boolean>` regression compares IPC bytes against a fresh logical equivalent.

## Blockers

None.

## Evidence

- `cargo test -p cdf-engine model_based_execution_shapes_preserve_canonical_identity`: 1 passed
  across the fixed 12-case domain in 24.49 seconds.
- `cargo test -p cdf-engine core_identity_snapshot_detects_a_faulty_package_identity`: the faulty
  package-hash control was detected.
- `cargo test -p cdf-engine canonical_microbatch_rebases_sliced_and_dirty_bitmap_padding`: direct
  canonical-vs-fresh Arrow IPC bytes matched for dirty Boolean, validity, and nested dictionary
  value prefixes after the review repair.
- With the documented local DuckDB link environment,
  `cargo test -p cdf-project model_based_receipt_gated_settlement_converges_across_recovery_sequences`
  passed the fixed 24-case action-sequence domain in 1.80 seconds, and
  `settlement_model_detects_a_faulty_unrecorded_commit` rejected its injected invalid state.
- Strict all-target/all-feature Clippy passed for the four touched root packages; formatting and
  diff checks passed.

## Review

The frozen independent review reported one high finding: recursive detection was followed by an
outer-only repair, leaving dirty dictionary values unchanged. The single authorized repair pass
normalizes flagged child `ArrayData` recursively and added a reproducing dictionary IPC law. The
focused regression and strict all-target/all-feature `cdf-engine` Clippy pass. Closure verdict:
pass. Residual risk is bounded to Arrow nested encodings outside the fixed generated/bitmap domain;
no other critical/high finding was reported.

## Retrospective

The small reference models paid for themselves immediately: equality of decoded rows had hidden
that canonical package identity also depends on unused bits in Arrow's byte containers. Comparing
the complete identity snapshot and serialized artifacts, rather than only logical values, exposed
the defect. Fixed seeds, small generated domains, and explicit faulty controls kept the result
reproducible and bounded instead of turning this ticket into an open-ended fuzz campaign.

The settlement model stayed useful because it models authority, not production implementation
details. Three receipt states and three checkpoint states were enough to cover the core fence:
committed checkpoint authority must already exist as the exact durable receipt, conflicts never
collapse into idempotence, and reopening does not change the result.
