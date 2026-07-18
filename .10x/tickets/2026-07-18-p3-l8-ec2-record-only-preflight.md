Status: active
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-18-p3-l6-ec2-benchmark-host.md, .10x/tickets/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md

# P3 L8: EC2 preflight record-only drift

## Scope

Repair the dedicated EC2 benchmark helper so durable record commits do not recursively stale an otherwise current benchmark host. Strict preflight must still reject binary-affecting drift, dirty worktrees, missing builds, untuned storage, missing workspaces, or low disk. When the remote built revision differs from local `HEAD` only by `.10x/` record changes, preflight may pass but must print that it accepted record-only drift and keep benchmark evidence tied to the actual built revision.

## Non-goals

- No weakening of preflight for source, Cargo, tooling, benchmark, workspace, or configuration changes outside `.10x/`.
- No implicit rebuilds from preflight.
- No hidden stale promotion path; intentional arbitrary stale runs still require `CDF_BENCH_PREFLIGHT_ALLOW_STALE=1`.
- No teardown of the active L7 host.

## Acceptance Criteria

- Exact remote/local revision matches continue to pass unchanged.
- Remote/local revision mismatches with only `.10x/` path changes pass and emit a machine-readable marker naming the accepted drift class.
- Remote/local revision mismatches with any non-`.10x/` changed path still fail unless `CDF_BENCH_PREFLIGHT_ALLOW_STALE=1`.
- Dry-run validation covers command construction without contacting the host.
- Live validation proves the current L7 host, built at the previous record commit, passes preflight after a local `.10x/`-only commit.

## References

- `.10x/tickets/done/2026-07-18-p3-l6-ec2-benchmark-host.md`
- `.10x/tickets/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md`
- `.10x/specs/performance-lab-and-envelope.md`

## Assumptions

- Record-backed: `.10x/` records are durable project memory and do not participate in release binary code generation.
- Record-backed: the active benchmark host is currently running and intentionally retained under L7.

## Journal

- 2026-07-18: Opened after committing benchmark-host marker evidence made strict preflight immediately stale the host again. This is a protocol defect, not operator error: durable evidence commits should not force a release rebuild when the synchronized/built source inputs are unchanged.
- 2026-07-18: Added a narrow preflight classifier for clean commit-to-clean commit drift where every changed path is under `.10x/`. Exact revision matches remain unchanged; dirty labels and non-record path drift continue to fail unless the explicit stale override is set. Preflight now prints `local_revision` and `revision_drift` so accepted record-only drift is machine visible.

## Blockers

None.

## Evidence

- 2026-07-18: `bash -n tools/p3-ec2-benchmark-host.sh && tools/p3-ec2-benchmark-host.sh --dry-run preflight` — passed shell syntax and dry-run command-construction validation without contacting the host.
- 2026-07-18: With local dirty changes present, `tools/p3-ec2-benchmark-host.sh preflight` failed as expected: remote revision `d4140bf71ce2315960a160256af64245528b1884` did not match local `bb5f9f7c9c253ab510a83d98c779dd32d2224f62+dirty`.
- 2026-07-18: After committing the helper change, `tools/p3-ec2-benchmark-host.sh sync-repo && timeout 180s tools/p3-ec2-benchmark-host.sh build && tools/p3-ec2-benchmark-host.sh preflight` passed at clean synced/built revision `a5042ca8b781e96f3812f16677cb1e2e74929a7e` with `revision_drift=none`, tuned gp3 storage, host class `host-class-95da083e15eebd1c`, workspace present, and `198892359680` free bytes.

## Review

Pending.

## Retrospective

Pending.
