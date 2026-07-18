Status: active
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-18-p3-l6-ec2-benchmark-host.md

# P3 L7: EC2 benchmark tranche lifecycle and teardown

## Scope

Own the lifecycle of the currently running dedicated EC2 benchmark host for the active P3 benchmark tranche. Keep the host measurement-ready while it is being reused, require explicit preflight before promotion evidence, and record idempotent teardown when the tranche is complete.

## Non-goals

- No data-plane optimization.
- No benchmark target changes.
- No replacement for `.10x/tickets/done/2026-07-18-p3-l6-ec2-benchmark-host.md`; L6 owns the reusable protocol/tooling.
- No long-lived unmanaged EC2 instance.

## Acceptance Criteria

- The active host has a recorded owner, purpose, instance id, host class, storage class, and clean build marker.
- Any future promotion measurement first passes `tools/p3-ec2-benchmark-host.sh preflight`, or explicitly records why historical/stale measurement is intentional.
- The tranche owner periodically records whether the instance remains needed or should be torn down.
- When the tranche ends, `tools/p3-ec2-benchmark-host.sh teardown` is run, termination is observed, local ignored state is removed or marked stale, and teardown evidence is recorded.
- No committed record contains SSH private-key material or cloud account secrets.

## References

- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/tickets/done/2026-07-18-p3-l6-ec2-benchmark-host.md`
- `.10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md`

## Assumptions

- User-ratified: one EC2 instance should be reused for a benchmark tranche and terminated when the tranche completes.
- Record-backed: the active host is `i-05011a85b7f2a33fe`, instance type `c7i.4xlarge`, host class `host-class-95da083e15eebd1c`, with gp3 storage tuned to `16000` IOPS / `1000` MiB/s.
- Record-backed: as of the L6 closure refresh, the host was synchronized and built at clean revision `a37a4d8645bfcc1919c04e22615e5364542ad238`.

## Journal

- 2026-07-18: Opened during L6 closure so the benchmark protocol can be marked done while the intentionally running tranche host remains visibly owned. Latest strict preflight passed at clean revision/build `a37a4d8645bfcc1919c04e22615e5364542ad238`, with tuned gp3 storage, workspace present, and `205647929344` free bytes. Current host markers are `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-current-clean-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-current-clean-build.env`.

## Blockers

The active P3 benchmark tranche still needs the host for G4 and follow-on performance measurements. Teardown is intentionally deferred until that tranche completes.

## Evidence

- `.10x/tickets/done/2026-07-18-p3-l6-ec2-benchmark-host.md` proves provisioning, sync, build, preflight, measured-command, fetch, and teardown command construction.
- `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-current-clean-revision.env` records the current clean synced revision marker.
- `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-current-clean-build.env` records the matching current clean release-build marker.

## Review

Pending tranche completion and teardown.

## Retrospective

Pending tranche completion and teardown.
