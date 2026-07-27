---
name: investigate-cdf-performance-regressions
description: Use when CDF throughput, CPU use, memory, I/O, build time, or end-to-end latency unexpectedly worsens or risks worsening.
metadata:
  created: 2026-07-26
  updated: 2026-07-26
---

# Investigate CDF Performance Regressions

## Objective

Find the first bad change and the owning architectural boundary with the least total measurement
work. Do not begin by poking tuning knobs. Preserve correctness and compare complete product paths
before optimizing an isolated phase.

Read:

- `.10x/knowledge/performance-evidence-and-regression-triage.md`
- `.10x/knowledge/runtime-performance-authorities.md`
- `.10x/specs/performance-lab-and-envelope.md`
- `QUALITY.md`

## 1. Classify the claim

Write down:

- exact dataset/resource/destination and expected rows/bytes;
- old and new wall, CPU, RSS, managed memory, spill, source I/O, package, destination, receipt, and
  checkpoint facts;
- revision, dependency tuple, build profile, host class, I/O mode, memory/cgroup authority, and
  tuning environment;
- whether the claim concerns a local smoke, deterministic regression predicate, or promotable
  performance default.

Reject comparisons that mix debug/release, different datasets, different destination state,
different segment/codec semantics, different host classes, different warm/cold modes, or different
timed regions.

## 2. Rule out host contamination cheaply

On a developer host inspect, do not mutate:

```bash
df -h .
du -sh target 2>/dev/null || true
vm_stat 2>/dev/null || true
sysctl vm.swapusage 2>/dev/null || true
ps -Ao pid,ppid,%cpu,%mem,etime,command | sort -k3 -nr | head -30
```

Disk-full conditions, swap, thermal throttling, background builds, a nearly full target filesystem,
or public-network variance can invalidate laptop timing. They do not explain a deterministic
same-host regression after the contamination is removed.

Run the workload once with `/usr/bin/time` and structured CDF JSON. Do not run a long matrix yet.

## 3. Determine where wall time lives

Use the complete topology:

```text
inventory/discovery
→ source transfer/read
→ decode
→ validation/normalization
→ canonical segment encode
→ persist/hash/finalize
→ destination ingress/materialization
→ receipt verification
→ checkpoint commit
```

CDF phase durations overlap and may sum above wall time. Treat them as operation-time attribution,
not a serial critical path. Compare phase changes, queue/pressure telemetry, CPU utilization,
physical I/O, and wall together.

Use `SourceIoMetrics` for transferred bytes. Planned file/task sizes are estimates, not physical
I/O. Compare remote full scans with a contemporaneous sequential transfer floor only when the same
object/generation is used.

## 4. Form ranked hypotheses from the diff

Inspect git history and the relevant control flow before measuring variants:

```bash
git log --oneline --decorate --graph -n 80
git log --stat -- crates/cdf-runtime crates/cdf-project
git show --stat SUSPECT_COMMIT
git show SUSPECT_COMMIT -- crates/cdf-runtime crates/cdf-project
```

Rank the five most credible commits/boundaries. Prefer changes that can explain the observed phase,
CPU, memory, I/O, or liveness signature:

- global job admission or nested CPU permits;
- byte-bounded channel/lease retention;
- segment size/count or schema framing;
- remote range versus sequential spool selection;
- footer/session reuse;
- hash rereads or extra materialization;
- destination bulk/scanner path;
- state reuse turning a run into a no-op or duplicate;
- debug/static/source-build differences.

Do not treat every constant as a suspect merely because it is tunable.

## 5. Bisect with a deterministic predicate

Use a local finite fixture when possible, not a mutable public endpoint. Keep the source,
destination, state reset, release profile, DuckDB linkage, and command identical.

Start with 50% jumps:

```bash
git bisect start KNOWN_BAD_COMMIT KNOWN_GOOD_COMMIT
```

At each revision:

- build optimized CDF with `DUCKDB_DOWNLOAD_LIB=1`;
- use a separate worktree or clean, isolated build target without touching another worker's files;
- reset CDF workspace and destination state identically;
- run a bounded predicate with a generous timeout around the good/bad gap;
- record wall, CPU, row/package identity, and exit classification.

Example predicate shape:

```text
same 12 local TLC files → DuckDB
expected rows and verified receipt required
good: completes below conservative threshold
bad: exceeds threshold, stalls, or fails
```

Never let a correctness failure count as a performance “good”. If the predicate is noisy, improve
the fixture or threshold before continuing the bisect.

After locating the first bad commit, trace the code path and explain why it creates the measured
signature. Do not stop at the hash.

## 6. Test the smallest complete repair

Prefer deletion or restoration of the prior fast path. If the repair changes a default:

- measure it against the old default on the same host and workload;
- preserve explicit user knobs as authoritative;
- avoid hard-coded caps;
- use adaptive host/CPU/memory/device evidence only at the correct stage;
- prove jobs/replay/package identity invariance where output is identity-bearing;
- prove managed memory returns to zero and cancellation releases ownership.

A correctness-critical repair may precede benchmark promotion, but it still needs a measured
non-regression before closure. A speculative safety/performance change that loses should be
deleted or left as an explicit opt-in knob—not made default.

## 7. Promote on the dedicated host

Use `.10x/skills/run-cdf-ec2-benchmarks/SKILL.md` when the candidate survives focused local
falsification. Compare median-of-N under the same host class and timed-region authority.

Retain:

- raw samples;
- exact old/new revisions;
- product and reference cells;
- semantic hashes/row counts;
- RSS/managed/cgroup/spill facts;
- bias and null results.

Do not reset a baseline to clear a failure. A >10% like-for-like regression fails. Smaller movement
may still be unacceptable for a hot path, but it must be interpreted with dispersion.

## 8. Close the loop

The owning ticket must state one of:

- retained improvement with before/after evidence;
- correctness repair with measured non-regression;
- rejected candidate and deletion;
- accepted residual with exact future reactivation trigger.

Remove superseded production paths. Preserve negative experiments in terminal records or isolated
benchmarks, not in runtime code. Update generated/current performance authority when a later
measurement supersedes an older published number.
