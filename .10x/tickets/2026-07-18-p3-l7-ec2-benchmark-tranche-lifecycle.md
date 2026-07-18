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
- Record-backed: as of the latest F1 refresh, the host is synchronized and built at clean revision `e39da00850eeb93a7d22d52cb30a945b70c32f9a`.

## Journal

- 2026-07-18: Opened during L6 closure so the benchmark protocol can be marked done while the intentionally running tranche host remains visibly owned. Latest strict preflight passed at clean revision/build `a37a4d8645bfcc1919c04e22615e5364542ad238`, with tuned gp3 storage, workspace present, and `205647929344` free bytes. Current host markers are `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-current-clean-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-current-clean-build.env`.
- 2026-07-18: Tranche preflight rechecked before the next G4 benchmark slice: `tools/p3-ec2-benchmark-host.sh preflight` passed for instance `i-05011a85b7f2a33fe`, `c7i.4xlarge`, tuned gp3 volume `vol-02f4b599167f8831c` at `16000` IOPS / `1000` MiB/s, synced and built clean revision `33fb860a2b35cc8d8fbe890c38f497eba60dd967`, host class `host-class-95da083e15eebd1c`, workspace present, and `205647839232` free bytes. The host remains intentionally active for G4 and follow-on P3 measurements.
- 2026-07-18: After the G4 DuckDB data-chunk reference slice, the host is synchronized and built at clean revision `3ca88ed8be07a49bbf980dbcd3569de8be87d124`. `cdf-p3-lab` release rebuild took `8m35s`; strict preflight passed with tuned gp3 storage, workspace present, and `205647618048` free bytes. Current host markers were fetched into `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-data-chunk-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-data-chunk-build.env`. The host remains intentionally active for the benchmark tranche.
- 2026-07-18: After the G4 DuckDB Arrow stream-scan reference slice, the host is synchronized and built at clean revision `11061e087fabb6dbf73248fc1ba6540ffecd8b4d`. `cdf-p3-lab` release rebuild took `8m38s`; strict preflight passed with tuned gp3 storage, workspace present, host class `host-class-95da083e15eebd1c`, and `204504100864` free bytes. Current host markers were fetched into `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-arrow-stream-scan-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-arrow-stream-scan-build.env`. The host remains intentionally active for the benchmark tranche.
- 2026-07-18: After adding bounded resource knobs to the G4 DuckDB Arrow stream-scan reference, the host is synchronized and built at clean revision `1525e5baf22c40b51cccdeeba0346f699b21e22d`. `cdf-p3-lab` release rebuild took `8m39s`; strict preflight passed with tuned gp3 storage, workspace present, host class `host-class-95da083e15eebd1c`, and `202419892224` free bytes. Current host markers were fetched into `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-stream-scan-knobs-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-stream-scan-knobs-build.env`. The host remains intentionally active for the benchmark tranche.
- 2026-07-18: Preflight correctly rejected the benchmark host after the local conformance-alias commit because the remote build marker still referenced `1525e5baf22c40b51cccdeeba0346f699b21e22d` while local was `c75a8c428a99518e9fdff6dc54c81d8cfe89124b`. Refreshed the host from the clean local commit with `sync-repo` and `build`; the release `cdf` link took `6m32s`, the release `cdf-p3-lab` link completed and wrote the build marker at `2026-07-18T15:43:47Z`, and the local SSH build wrapper again remained open after remote build completion. Direct host inspection showed no remaining cargo/rustc workload and both release binaries present, so the stuck wrapper was interrupted and strict `preflight` was run. Preflight passed for instance `i-05011a85b7f2a33fe`, `c7i.4xlarge`, tuned gp3 volume `vol-02f4b599167f8831c` at `16000` IOPS / `1000` MiB/s, clean synced/built revision `c75a8c428a99518e9fdff6dc54c81d8cfe89124b`, host class `host-class-95da083e15eebd1c`, workspace present, and `198891827200` free bytes. Current host markers were fetched into `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-postgresql-alias-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-postgresql-alias-build.env`. The host remains intentionally active for the benchmark tranche.
- 2026-07-18: After committing the preceding L7 marker refresh, preflight again rejected stale remote revision `c75a8c428a99518e9fdff6dc54c81d8cfe89124b` against local `d4140bf71ce2315960a160256af64245528b1884`, as designed. A cached `sync-repo` + `build` returned normally in seconds, showing the SSH-wrapper hang is specific to long idle relinks rather than every build invocation. Strict preflight then passed for clean synced/built revision `d4140bf71ce2315960a160256af64245528b1884`, tuned gp3 storage, host class `host-class-95da083e15eebd1c`, workspace present, and `198891851776` free bytes. Current host markers were fetched into `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-host-marker-commit-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-host-marker-commit-build.env`. The host remains intentionally active for the benchmark tranche.
- 2026-07-18: After committing the DuckDB stream-scan disable/rejection patch, refreshed the host from clean local commit `8d9695a9cd5eefd49a86be0e1448ba4c84ea43ae`. The release `cdf` build completed in `6m32s`; the release `cdf-p3-lab` relink completed in `8m36s` and wrote its build marker at `2026-07-18T17:22:04Z`. A read-only process probe during the quiet relink confirmed real `cargo`/`rustc` work rather than the prior stuck SSH-wrapper state. Strict `tools/p3-ec2-benchmark-host.sh preflight` passed for instance `i-05011a85b7f2a33fe`, `c7i.4xlarge`, tuned gp3 volume `vol-02f4b599167f8831c` at `16000` IOPS / `1000` MiB/s, clean synced/built revision `8d9695a9cd5eefd49a86be0e1448ba4c84ea43ae`, host class `host-class-95da083e15eebd1c`, workspace present, and `198890590208` free bytes. Current host markers were fetched into `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-stream-scan-disable-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-stream-scan-disable-build.env`. The host remains intentionally active for G4 and follow-on P3 measurements.
- 2026-07-18: After committing the preceding L7 marker refresh, ran a cached `tools/p3-ec2-benchmark-host.sh sync-repo && tools/p3-ec2-benchmark-host.sh build && tools/p3-ec2-benchmark-host.sh preflight` to keep the host immediately measurement-ready. Both release builds were cache hits (`0.27s` for `cdf`, `0.24s` for `cdf-p3-lab`), and strict preflight passed for clean synced/built revision `6b9c9c7ad49996fb1d0c407fcab9a45a813f51aa`, tuned gp3 storage, host class `host-class-95da083e15eebd1c`, workspace present, and `198890582016` free bytes. Current host markers were fetched into `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-stream-scan-disable-marker-commit-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-stream-scan-disable-marker-commit-build.env`. The host remains intentionally active for G4 and follow-on P3 measurements.
- 2026-07-18: L9 added a slim measured-command runner path. The host was synchronized at local dirty revision `56ced876ee7b555e1d5847b21e032111c9ce693b+dirty`; `tools/p3-ec2-benchmark-host.sh build-measure` kept `cdf` as a cache hit (`0.27s`) and built release `cdf-bench-core` + `cdf-bench-measure` in `10.27s` without rebuilding `cdf-p3-lab`. `tools/p3-ec2-benchmark-host.sh preflight-measure` passed with tuned gp3 storage, host class `host-class-95da083e15eebd1c`, workspace present, and `198829858816` free bytes. Measured-runner markers were fetched into `.10x/evidence/.storage/2026-07-18-p3-l9-ec2-measure-runner-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l9-ec2-measure-runner-build.env`. The host remains intentionally active; full-lab preflight remains available when reference workloads need it.
- 2026-07-18: After committing L9, refreshed the host to clean revision `4412d7150501f8761323da312ec75b8d4533a21c` with `tools/p3-ec2-benchmark-host.sh sync-repo && tools/p3-ec2-benchmark-host.sh build-measure && tools/p3-ec2-benchmark-host.sh preflight-measure`. Release `cdf` was a cache hit (`0.27s`), release `cdf-p3-measure` was a cache hit (`0.12s`), and preflight mode `measure` passed with tuned gp3 storage, host class `host-class-95da083e15eebd1c`, workspace present, and `198829842432` free bytes. Current clean measured-runner markers were fetched into `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-l9-measure-runner-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-l9-measure-runner-build.env`. The host remains intentionally active for G4 and follow-on P3 measurements.
- 2026-07-18: After rejecting and committing the DuckDB row-key default/sequence candidate as evidence-only, refreshed the host back to clean commit `7b582618c005e85dc7ddf31ed9abcd771d07fe4f` with `tools/p3-ec2-benchmark-host.sh sync-repo && tools/p3-ec2-benchmark-host.sh build-measure && tools/p3-ec2-benchmark-host.sh preflight-measure`. The release `cdf` relink completed in `6m30s`, release `cdf-p3-measure` was a cache hit (`0.14s`), and preflight mode `measure` passed with tuned gp3 storage, host class `host-class-95da083e15eebd1c`, workspace present, and `198819827712` free bytes. Current clean measured-runner markers were fetched into `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-rowkey-default-rejection-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-rowkey-default-rejection-build.env`. The host remains intentionally active for G4 and follow-on P3 measurements.
- 2026-07-18: After committing the F1 runtime-budget authority/report slice, refreshed the host to clean commit `e39da00850eeb93a7d22d52cb30a945b70c32f9a` with `tools/p3-ec2-benchmark-host.sh sync-repo && tools/p3-ec2-benchmark-host.sh build-measure && tools/p3-ec2-benchmark-host.sh preflight-measure`. The release `cdf` relink completed in `7m04s`, release `cdf-p3-measure` was a cache hit (`0.14s`), and preflight mode `measure` passed with tuned gp3 storage, host class `host-class-95da083e15eebd1c`, workspace present, and `198818938880` free bytes. Current clean measured-runner markers were fetched into `.10x/evidence/.storage/2026-07-18-p3-f1-ec2-runtime-memory-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-f1-ec2-runtime-memory-build.env`. A live `cdf doctor --json` memory-authority probe was fetched into `.10x/evidence/.storage/2026-07-18-p3-f1-ec2-runtime-memory-doctor.json`; it shows this EC2 host is not a cgroup v2 enforced-memory proof host, while remaining valid for performance benchmarking. The host remains intentionally active for G4 and follow-on P3 measurements.

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
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-postgresql-alias-revision.env` records the clean synced revision marker after the conformance-alias unblock commit.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-postgresql-alias-build.env` records the matching clean release-build marker after the conformance-alias unblock commit.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-host-marker-commit-revision.env` records the clean synced revision marker after the L7 host-marker refresh commit.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-host-marker-commit-build.env` records the matching clean release-build marker after the L7 host-marker refresh commit.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-stream-scan-disable-revision.env` records the clean synced revision marker after the DuckDB stream-scan disable/rejection patch.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-stream-scan-disable-build.env` records the matching clean release-build marker after the DuckDB stream-scan disable/rejection patch.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-stream-scan-disable-marker-commit-revision.env` records the clean synced revision marker after the L7 marker-refresh commit.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-stream-scan-disable-marker-commit-build.env` records the matching clean release-build marker after the L7 marker-refresh commit.
- `.10x/evidence/.storage/2026-07-18-p3-l9-ec2-measure-runner-revision.env` records the synced revision marker for the lean measured-command runner build.
- `.10x/evidence/.storage/2026-07-18-p3-l9-ec2-measure-runner-build.env` records the matching lean measured-command runner build marker.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-l9-measure-runner-revision.env` records the current clean synced revision marker after the L9 commit.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-l9-measure-runner-build.env` records the matching current clean measured-runner build marker after the L9 commit.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-rowkey-default-rejection-revision.env` records the current clean synced revision marker after rejecting the DuckDB row-key default/sequence candidate.
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-rowkey-default-rejection-build.env` records the matching current clean measured-runner build marker after rejecting the DuckDB row-key default/sequence candidate.
- `.10x/evidence/.storage/2026-07-18-p3-f1-ec2-runtime-memory-revision.env` records the current clean synced revision marker after the F1 runtime-budget authority/report slice.
- `.10x/evidence/.storage/2026-07-18-p3-f1-ec2-runtime-memory-build.env` records the matching current clean measured-runner build marker after the F1 runtime-budget authority/report slice.
- `.10x/evidence/.storage/2026-07-18-p3-f1-ec2-runtime-memory-doctor.json` records the live EC2 `cdf doctor --json` memory-authority observation for the F1 slice.

## Review

Pending tranche completion and teardown.

## Retrospective

Pending tranche completion and teardown.
