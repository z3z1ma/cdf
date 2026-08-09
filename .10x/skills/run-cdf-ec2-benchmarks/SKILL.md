---
name: run-cdf-ec2-benchmarks
description: Use when provisioning, reusing, measuring on, fetching evidence from, or tearing down CDF's dedicated AWS EC2 performance host.
metadata:
  created: 2026-07-26
  updated: 2026-08-08
---

# Run CDF EC2 Benchmarks

## Objective

Produce comparable, host-labelled CDF performance evidence on one reusable FQ12 EC2 host, then
terminate the cost-bearing instance. Use `tools/p3-ec2-benchmark-host.sh`; do not recreate its AWS,
SSH, synchronization, preflight, timeout, or evidence logic manually.

Read these authorities before the first live command:

- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/knowledge/performance-evidence-and-regression-triage.md`
- `.10x/knowledge/developer-build-duckdb-linkage.md`
- `.10x/tickets/done/2026-07-18-p3-l6-ec2-benchmark-host.md`
- `.10x/tickets/done/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md`

## Non-negotiable operating rules

1. AWS mutations require explicit user authority. A request to benchmark on FQ12 is authority for
   the CDF-tagged host workflow, not for touching unrelated instances, security groups, buckets,
   roles, or datasets.
2. Reuse one host for a coherent measurement tranche. Do not provision one host per ticket.
3. Create or identify a lifecycle owner before provisioning. Its acceptance includes termination.
4. Run the relevant preflight immediately before every promotion measurement.
5. Fetch raw machine evidence into `.10x/evidence/.storage/`; an SSH transcript is not durable
   benchmark evidence.
6. Run `teardown` as soon as the tranche ends and independently confirm no active CDF host remains.
7. Never print, commit, or paste the private key or ignored state files. If a private key reaches
   tool/chat output, rotate it immediately and treat the old key as compromised.
8. Never reuse an unrelated FQ12 instance or security group merely because it already permits SSH.
9. Do not compare cells whose host class, workload identity, timed-region version, dependency
   tuple, I/O mode, memory authority, or external state differs.

## Preflight the local environment

From the repository root:

```bash
git status --short
tools/p3-ec2-benchmark-host.sh --help
tools/p3-ec2-benchmark-host.sh plan
tools/p3-ec2-benchmark-host.sh --dry-run prepare-ssh
tools/p3-ec2-benchmark-host.sh --dry-run provision
```

When more than one `PowerUser-*` profile exists, set `AWS_PROFILE` explicitly instead of relying on
the helper's “first configured PowerUser profile” fallback:

```bash
aws configure list-profiles
AWS_PROFILE=PowerUser-FQ12 aws sts get-caller-identity
```

If SSO credentials are expired:

```bash
aws sso login --profile PowerUser-FQ12
```

Use the actual configured FQ12 profile name if it differs. Do not put account ids, credentials, or
private host facts into a committed skill or knowledge record.

## Provision and prepare one host

The defaults are a `c7i.4xlarge`, 250 GiB gp3 root volume, 16,000 IOPS, and 1,000 MiB/s in
`us-west-2`. They are a known benchmark class, not a universal workload recommendation.

```bash
AWS_PROFILE=PowerUser-FQ12 tools/p3-ec2-benchmark-host.sh prepare-ssh
AWS_PROFILE=PowerUser-FQ12 tools/p3-ec2-benchmark-host.sh provision
AWS_PROFILE=PowerUser-FQ12 tools/p3-ec2-benchmark-host.sh tune-volume
AWS_PROFILE=PowerUser-FQ12 tools/p3-ec2-benchmark-host.sh wait-ssh
AWS_PROFILE=PowerUser-FQ12 tools/p3-ec2-benchmark-host.sh bootstrap
AWS_PROFILE=PowerUser-FQ12 tools/p3-ec2-benchmark-host.sh status
```

`prepare-ssh` creates or reuses only CDF-tagged resources, restricts port 22 to the current public
IPv4 `/32`, and records ignored resource state under `target/cdf-benchmarks/ec2-host/`.
`provision` adopts a running CDF/P3Benchmark-tagged instance only when exactly one exists; it
refuses to choose among multiple candidates.

The active instance lives in `target/cdf-benchmarks/ec2-host/state.env`. Reusable network/key
metadata lives separately in `ssh-resources.env`. Absence of `state.env` means no helper-owned
active host is recorded; it does not by itself prove AWS has no leaked instance.

## Synchronize the exact code and workspace

```bash
tools/p3-ec2-benchmark-host.sh sync-repo
CDF_BENCH_WORKSPACE=/absolute/path/to/cdf-workspace \
  tools/p3-ec2-benchmark-host.sh sync-workspace
```

Repo synchronization:

- honors `.gitignore`;
- excludes `.git`, `target`, `.env*`, AWS/Codex state, and common secret directories;
- writes `.cdf-bench-revision.env` because `.git` is intentionally absent remotely;
- labels a dirty tree as `<commit>+dirty`.

Workspace synchronization defaults to `minimal`: config, lockfile, resources, state needed for
planning, schema snapshots, and observation cache. Use
`CDF_BENCH_WORKSPACE_SYNC_MODE=full` only when the workload genuinely needs the ignore-filtered
workspace tree. Full mode still excludes packages, temporary spools, destination databases, and
common secrets.

Do not synchronize generated multi-gigabyte local outputs merely because they happen to sit in the
workspace. Prefer reproducible fixture generation on-host or `sync-package` for one verified
finalized package.

## Select the smallest build graph

For ordinary measured `cdf` commands:

```bash
tools/p3-ec2-benchmark-host.sh build-measure
tools/p3-ec2-benchmark-host.sh verify-measure
tools/p3-ec2-benchmark-host.sh preflight-measure
```

This builds `bench-max` `cdf` plus the lean `cdf-p3-measure` runner. `bench-max` is the
non-incremental fat-LTO profile required for roofline evidence; ordinary iteration uses the faster
thin-LTO `release` profile. The measure path avoids additionally relinking the heavy reference lab.

Only when a raw/reference lab workload is required:

```bash
tools/p3-ec2-benchmark-host.sh build
tools/p3-ec2-benchmark-host.sh verify
tools/p3-ec2-benchmark-host.sh preflight
```

Both paths explicitly set `DUCKDB_DOWNLOAD_LIB=1` and record
`duckdb_linkage=downloaded-prebuilt`. That is correct for developer/benchmark iteration. It is not
the hosted release build, which uses the static `bundled-duckdb` feature.

Preflight rejects:

- a non-running instance;
- root storage that does not match configured gp3 IOPS/throughput;
- stale synchronized revision or stale build marker;
- missing `bench-max` binaries;
- a missing required workspace;
- less than the configured disk floor;
- mismatched full-lab versus measured-runner build authority.

Record-only local commits may pass through the helper's exact record-only revision-drift rule.
Product changes require a new sync and build.

## Run a product measurement

Default to at least three samples for promotion evidence:

```bash
TICKET_CELL=p3-example-control
DATASET_ID=tlc-full-year
WORKLOAD_ID=tlc-to-duckdb-v1
RESOURCE_ID=tlc.yellow
EXPECTED_ROWS=41169720

CDF_BENCH_SAMPLES=3 \
CDF_BENCH_IO_MODE=warm \
CDF_BENCH_TIMEOUT_MS=900000 \
CDF_BENCH_EXPECTED_ROWS="$EXPECTED_ROWS" \
tools/p3-ec2-benchmark-host.sh measure-cdf \
  "target/cdf-benchmarks/$TICKET_CELL.json" \
  "$DATASET_ID" \
  "$WORKLOAD_ID" \
  -- run "$RESOURCE_ID" --json --progress never --color never --unicode never
```

Use stable dataset/workload ids. Changing the timed command, fixture semantics, setup boundary, or
derived-byte definition requires a new workload or timed-region version.

Useful optional authorities:

```text
CDF_BENCH_DERIVED_LOGICAL_BYTES
CDF_BENCH_EXPECTED_PHYSICAL_BYTES
CDF_BENCH_EXPECTED_PACKAGE_HASH
CDF_BENCH_EXPECTED_SCHEMA_HASH
CDF_BENCH_MEASURE_ENV_JSON
CDF_BENCH_SYSTEMD_MEMORY_MAX
```

Pass CDF tuning knobs through `CDF_BENCH_MEASURE_ENV_JSON`, for example:

```bash
CDF_BENCH_MEASURE_ENV_JSON='{"CDF_DUCKDB_THREADS":"4","CDF_DUCKDB_MEMORY_LIMIT":"3GiB"}'
```

Do not set ad-hoc remote shell state for a measured child. The supervised request must record the
environment.

`measure-cdf` copies the workspace outside the timed region and deletes `.cdf/state.db` by default.
This prevents a prior checkpoint from converting an ingest benchmark into a no-op. Set
`CDF_BENCH_MEASURE_PRESERVE_STATE=1` only for a deliberately named resume/no-op workload.

Fresh workspace copies do not reset external systems. A benchmark mutating PostgreSQL, S3, or
another external destination must reset that destination per sample or use one explicitly
uncontrolled sample and assert the external postcondition. A warm-up against the same external
database can otherwise double rows before the timed sample.

For enforced-memory evidence:

```bash
CDF_BENCH_SYSTEMD_MEMORY_MAX=6G \
  tools/p3-ec2-benchmark-host.sh measure-cdf \
    "target/cdf-benchmarks/$TICKET_CELL-memory-6g.json" \
    "$DATASET_ID" \
    "$WORKLOAD_ID-memory-6g" \
    -- run "$RESOURCE_ID" --json --progress never --color never --unicode never
```

This changes the host-class comparability key because effective memory authority is part of the
host. Never compare bounded and unbounded host classes as the same cell.

## Run reference or arbitrary commands

```bash
tools/p3-ec2-benchmark-host.sh lab -- --help
tools/p3-ec2-benchmark-host.sh cdf -- version
tools/p3-ec2-benchmark-host.sh run -- uname -a
```

Use `lab` for named reference/roofline workloads. `cdf` is useful for untimed inspection.
`run` is an escape hatch, not a replacement for `measure-cdf`; arbitrary stopwatch output lacks
the standard comparability envelope.

## Fetch and record evidence

```bash
EVIDENCE_BASENAME=2026-07-26-p3-example-control

tools/p3-ec2-benchmark-host.sh fetch \
  "target/cdf-benchmarks/$TICKET_CELL.json" \
  ".10x/evidence/.storage/$EVIDENCE_BASENAME.json"

tools/p3-ec2-benchmark-host.sh fetch \
  .cdf-bench-revision.env \
  ".10x/evidence/.storage/$EVIDENCE_BASENAME-revision.env"

tools/p3-ec2-benchmark-host.sh fetch \
  .cdf-bench-measure-build.env \
  ".10x/evidence/.storage/$EVIDENCE_BASENAME-build.env"
```

For full-lab runs fetch `.cdf-bench-build.env` instead. The ticket/evidence record must state:

- exact command and setup boundary;
- revision/dirty status and binary hash when relevant;
- host class and storage class;
- dataset/workload/timed-region ids;
- sample count, raw values, median, and dispersion;
- logical versus physical byte definitions;
- row/package/schema assertions;
- peak RSS, managed memory, cgroup memory, spill, OOM/pressure facts;
- reference bias and limits;
- whether the result retains or rejects a product/default change.

## Teardown

When the tranche ends:

```bash
tools/p3-ec2-benchmark-host.sh status
tools/p3-ec2-benchmark-host.sh teardown
aws --profile PowerUser-FQ12 --region us-west-2 ec2 describe-instances \
  --filters Name=tag:Project,Values=CDF \
            Name=tag:Purpose,Values=P3Benchmark \
            Name=instance-state-name,Values=pending,running
```

`teardown` waits for termination before removing active `state.env`. The reusable
`ssh-resources.env` may remain for a future explicitly authorized tranche; it is not evidence of
an active instance. Record the termination observation in the lifecycle owner.

If the helper has no `state.env` but the read-only tag query finds a live CDF benchmark instance,
adopt or terminate it deliberately—never leave it invisible.

## Known failure modes

- **Stale revision/build:** rerun `sync-repo`, the smallest required build, then matching preflight.
- **First build cannot download DuckDB:** confirm GitHub access and `DUCKDB_DOWNLOAD_LIB=1`; do not
  switch the benchmark to a source-built/static DuckDB without changing the comparison authority.
- **Long quiet release link:** inspect remote cargo/rustc once. A fat-LTO link may use one core for
  minutes. Do not start another build or kill it merely because SSH is quiet.
- **SSH wrapper remains open after build:** verify the remote process tree and build marker. Only
  interrupt the local wrapper after cargo/rustc have exited and the complete marker exists.
- **Disk below preflight floor:** remove disposable benchmark outputs or provision a larger
  explicit volume; do not lower the floor to force a pass.
- **Benchmark unexpectedly finishes instantly:** inspect copied checkpoint/destination state.
- **Rows doubled across samples:** external destination state was not reset.
- **Public endpoint variance:** compare against a contemporaneous transfer/reference and retain
  the result as diagnostic unless the workload is variance-controlled.
- **Private key printed:** rotate local and EC2 key material immediately; delete the old pair.
