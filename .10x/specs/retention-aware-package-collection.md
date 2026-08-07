Status: active
Created: 2026-08-07
Updated: 2026-08-07

# Retention-aware package collection

## Purpose

Bound heavy local package storage during repeated and continuous execution without weakening the
package, receipt, checkpoint, replay, recovery, or schema-promotion authorities.

Packages are durable pre-commit/recovery buffers. After an exact destination receipt and committed
checkpoint prove settlement, their heavy canonical payload bytes become eligible for the selected
environment/trust retention policy. Minimal identity and settlement proof remain durable.

## Policy resolution

The selected environment's existing typed retention policy is the only collection policy. The
resource trust level selects its trust-specific rule, falling back to `default`; absence of both
means automatic collection is disabled.

A rule is exactly one of:

- `N runs`: retain the newest `N` settled package epochs for that resource and checkpoint scope;
- a duration: retain settled package payloads until their recorded settlement time is older than
  the duration.

Selection order is committed checkpoint/source order with settlement time only for duration
eligibility. Filesystem modification time is never authority. A policy change is prospective on
the next plan and may make older settled packages eligible; it does not rewrite receipts or
checkpoints.

## Eligibility and protected states

A package is eligible for payload tombstoning only when all are true:

- the package verifies against its manifest;
- its destination receipt is present, readable, and verifies the exact package/target intent;
- a committed checkpoint references the exact package hash and receipt-covered source frontier;
- it is outside the resolved retention window;
- it is not needed for incomplete group/routed settlement, replay, correction, publication
  recovery, schema promotion, or another active durable lease;
- it is not already tombstoned.

Corrupt, missing, ambiguous, extracting, packaged-but-unreceipted, recovery-required, leased, or
otherwise incompletely proven artifacts fail closed and remain untouched.

## Collection operation

Collection uses the existing crash-safe package tombstone operation. It removes manifest-listed
heavy canonical payload/evidence files and marks the package archived while preserving the package
manifest header/hash, lifecycle tombstone, receipts, checkpoint history, and the minimum
verification metadata required to prove what settled.

Collection MUST NOT delete the package directory, receipt, checkpoint, target/effect identity, or
source frontier. A tombstoned package is no longer replayable from local payload bytes and MUST be
reported as such. Archive/remote retention is a separate explicit operation.

## Automatic and manual execution

After each successful receipt-gated checkpoint commit, normal run execution invokes the bounded
collector for the affected resource/scope. Collection failure is reported as an operational
failure but cannot revoke or falsify the already committed checkpoint. The next run/manual command
may retry the same idempotent plan.

`cdf package gc` remains a no-write plan by default. `cdf package gc --execute` executes exactly
that plan after revalidating every candidate immediately before tombstoning. Human and JSON output
distinguish retained, collectible, collected, already tombstoned, recovery-required, corrupt, and
missing artifacts and report reclaimed file/byte counts when known.

The automatic path and manual command consume one neutral planner/executor. The CLI does not own a
second eligibility implementation.

## Schema-promotion interaction

The plan reports whether collection removes the last locally retained residual/evidence bytes for
an otherwise promotion-eligible schema observation. Active promotion/review leases protect their
artifacts. Absent a lease, the configured retention policy remains authoritative and collection
may make an older observation unavailable for local promotion; the report names that consequence
before manual execution and in automatic telemetry.

## Failure behavior

- unreadable or inconsistent package/receipt/checkpoint state is Data and is retained;
- filesystem permission/capacity/descriptor failures are Environment and preserve source errors;
- a race that changes package lifecycle, lease, receipt, checkpoint, or retention eligibility
  between planning and execution causes that candidate to be skipped/fail closed;
- tombstoning is idempotent; retry never treats an existing tombstone as corruption;
- no collection outcome advances a source checkpoint or destination receipt.

## Acceptance scenarios

1. With `2 runs`, three settled epochs retain the newest two and tombstone only the oldest payload.
2. With duration retention, settlement time—not file mtime—controls eligibility.
3. Missing retention policy produces no automatic collection.
4. Packaged/unreceipted, receipted/uncheckpointed, recovery-required, leased, corrupt, and missing
   artifacts remain untouched with actionable classifications.
5. Dry-run and `--execute` use the same ordered plan; execution revalidates and is idempotent.
6. Automatic collection runs after checkpoint settlement and repeated drain epochs keep heavy
   package bytes inside the configured window.
7. Tombstoning preserves manifest/hash/receipt/checkpoint proof while local replay truthfully
   becomes unavailable.
8. The last local schema-promotion residual is reported and protected only by an active lease or
   the resolved retention window.

## References

- `.10x/research/2026-08-07-cdc-mysql-continuous-readiness.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/schema-promotion-corrections.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/package-io-hashing-durability.md`
