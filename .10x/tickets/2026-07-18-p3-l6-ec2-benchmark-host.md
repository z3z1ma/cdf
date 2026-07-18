Status: open
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md, .10x/specs/performance-lab-and-envelope.md

# P3 L6: dedicated EC2 benchmark host protocol

## Scope

Implement and record the operating procedure/tooling for P3 performance measurements on a reusable AWS EC2 host in the FQ12 environment. The host is provisioned through the AWS CLI PowerUser profile, reused for a tranche of benchmark tickets, receives the repo and CDF workspace by ignore-respecting synchronization, builds the optimized release binary on-host, runs the lab/live workloads with host-labeled evidence, and is torn down when the tranche completes.

## Non-goals

- No data-plane optimization or benchmark target weakening.
- No long-lived unmanaged cloud instance.
- No committed secrets, AWS account identifiers beyond the user-ratified environment label, or host-specific local paths in generated reports.
- No replacement for deterministic CI fixtures; this owns production-like performance evidence, not ordinary fast checks.

## Acceptance Criteria

- A reproducible procedure or script provisions one selected EC2 instance shape in FQ12, records instance type/AMI/kernel/storage/network class, and tags it for CDF benchmark ownership and teardown.
- Repo synchronization honors `.gitignore` or an equivalent explicit include/exclude manifest, preserves `Cargo.lock`, and avoids copying `target/` or local secrets.
- A CDF benchmark workspace synchronization path captures `cdf.toml`, `.cdf/` state required for the workload, and dataset acquisition/generation recipes without embedding private local paths.
- The host builds the CDF release binary with release-profile optimizations from the synchronized revision; build environment facts are recorded.
- Benchmark commands emit machine evidence with host/build/workspace/revision labels and clear setup-versus-timed-region boundaries.
- The same instance can be reused across a tranche, and teardown is explicit, recorded, and idempotent.
- A dry-run or no-cloud local validation covers command construction/redaction before any AWS write is used.

## References

- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/knowledge/runtime-conformance-throughput-rule.md`
- `.10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md`
- `.10x/tickets/2026-07-11-p3-z1-envelope-evidence-reconciliation.md`

## Assumptions

- User-ratified: AWS CLI can use a PowerUser role/profile in the FQ12 environment for provisioning a benchmark instance.
- User-ratified: one EC2 instance should be reused for a whole benchmark tranche and terminated when the tranche completes, not created per ticket and not left indefinitely.
- Record-backed: laptop measurements may be contaminated and are insufficient to promote performance-sensitive defaults.

## Journal

- 2026-07-18: Opened from user benchmark guidance after repeated laptop swap/disk-pressure invalidations and live public-endpoint variance affected G4/G4-adjacent timing. The governing spec now treats dedicated EC2 evidence as the promotion authority for P3 defaults and closeout cells.

## Blockers

None for shaping. Execution must still inspect local AWS CLI profile names/config before issuing cloud writes; if the expected FQ12/PowerUser profile is unavailable, record the exact missing prerequisite rather than guessing credentials.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
