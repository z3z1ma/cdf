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
- 2026-07-18: Tranche preflight rechecked before the next G4 benchmark slice: `tools/p3-ec2-benchmark-host.sh preflight` passed for instance `i-05011a85b7f2a33fe`, `c7i.4xlarge`, tuned gp3 volume `vol-02f4b599167f8831c` at `16000` IOPS / `1000` MiB/s, synced and built clean revision `33fb860a2b35cc8d8fbe890c38f497eba60dd967`, host class `host-class-95da083e15eebd1c`, workspace present, and `205647839232` free bytes. The host remains intentionally active for G4 and follow-on P3 measurements.
- 2026-07-18: After the G4 DuckDB data-chunk reference slice, the host is synchronized and built at clean revision `3ca88ed8be07a49bbf980dbcd3569de8be87d124`. `cdf-p3-lab` release rebuild took `8m35s`; strict preflight passed with tuned gp3 storage, workspace present, and `205647618048` free bytes. Current host markers were fetched into `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-data-chunk-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-data-chunk-build.env`. The host remains intentionally active for the benchmark tranche.
- 2026-07-18: After the G4 DuckDB Arrow stream-scan reference slice, the host is synchronized and built at clean revision `11061e087fabb6dbf73248fc1ba6540ffecd8b4d`. `cdf-p3-lab` release rebuild took `8m38s`; strict preflight passed with tuned gp3 storage, workspace present, host class `host-class-95da083e15eebd1c`, and `204504100864` free bytes. Current host markers were fetched into `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-arrow-stream-scan-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-arrow-stream-scan-build.env`. The host remains intentionally active for the benchmark tranche.
- 2026-07-18: After adding bounded resource knobs to the G4 DuckDB Arrow stream-scan reference, the host is synchronized and built at clean revision `1525e5baf22c40b51cccdeeba0346f699b21e22d`. `cdf-p3-lab` release rebuild took `8m39s`; strict preflight passed with tuned gp3 storage, workspace present, host class `host-class-95da083e15eebd1c`, and `202419892224` free bytes. Current host markers were fetched into `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-stream-scan-knobs-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-stream-scan-knobs-build.env`. The host remains intentionally active for the benchmark tranche.

## Blockers

The active P3 benchmark tranche still needs the host for G4 and follow-on performance measurements. Teardown is intentionally deferred until that tranche completes.

## Evidence

- `.10x/tickets/done/2026-07-18-p3-l6-ec2-benchmark-host.md` proves provisioning, sync, build, preflight, measured-command, fetch, and teardown command construction.
- `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-current-clean-revision.env` records the current clean synced revision marker.
- `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-current-clean-build.env` records the matching current clean release-build marker.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-data-chunk-revision.env` records the clean synced revision marker after the G4 data-chunk benchmark slice.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-data-chunk-build.env` records the matching clean release-build marker after the G4 data-chunk benchmark slice.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-arrow-stream-scan-revision.env` records the clean synced revision marker after the G4 Arrow stream-scan benchmark slice.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-arrow-stream-scan-build.env` records the matching clean release-build marker after the G4 Arrow stream-scan benchmark slice.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-stream-scan-knobs-revision.env` records the clean synced revision marker after the G4 Arrow stream-scan resource-knob benchmark slice.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-stream-scan-knobs-build.env` records the matching clean release-build marker after the G4 Arrow stream-scan resource-knob benchmark slice.

## Review

Pending tranche completion and teardown.

## Retrospective

Pending tranche completion and teardown.
