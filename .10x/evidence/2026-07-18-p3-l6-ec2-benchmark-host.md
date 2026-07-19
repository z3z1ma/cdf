Status: recorded
Created: 2026-07-18
Updated: 2026-07-18

# P3 L6 EC2 benchmark host live proof

## Observation

The dedicated EC2 benchmark-host workflow now works against a live FQ12 benchmark instance instead of only dry-run command construction. The live path provisioned a CDF-tagged `c7i.4xlarge` Amazon Linux 2023 host, bootstrapped the Rust/build environment, synchronized the repository and a minimal CDF workspace, built optimized release binaries on-host, verified both binaries, compiled the user's exploratory CDF workspace, emitted a three-sample machine baseline report, and corrected the root-volume storage floor from default gp3 throughput to an explicit benchmark-class gp3 configuration.

The machine report is stored at `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-baseline-report.json`.

Measured CDF command observations are stored at:

- `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-measure-cdf-smoke.json`
- `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-measure-cdf-tlc-state-reset.json`
- `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-measure-cdf-timeout-proof.json`
- `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-measure-cdf-env-forward.json`
- `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-measure-cdf-env-forward-request.json`
- `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-preflight-revision.env`
- `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-preflight-build.env`
- `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-current-clean-revision.env`
- `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-current-clean-build.env`

## Procedure

- `tools/p3-ec2-benchmark-host.sh prepare-ssh`
- `tools/p3-ec2-benchmark-host.sh provision`
- `tools/p3-ec2-benchmark-host.sh wait-ssh && tools/p3-ec2-benchmark-host.sh status`
- `tools/p3-ec2-benchmark-host.sh bootstrap`
- `tools/p3-ec2-benchmark-host.sh sync-repo`
- `tools/p3-ec2-benchmark-host.sh build`
- `tools/p3-ec2-benchmark-host.sh verify`
- `CDF_BENCH_WORKSPACE=/Users/alexanderbut/code_projects/tmp tools/p3-ec2-benchmark-host.sh sync-workspace`
- `tools/p3-ec2-benchmark-host.sh cdf -- inspect resources --color never --unicode never`
- `tools/p3-ec2-benchmark-host.sh run -- bash -lc 'set -euo pipefail; . ./.cdf-bench-revision.env; mkdir -p target/cdf-benchmarks/ec2-baseline; ./target/release/cdf-p3-lab baseline-run target/cdf-benchmarks/ec2-baseline "$repo_revision_label" Cargo.lock rustc-1.97.1 3 > target/cdf-benchmarks/ec2-baseline/report.json; wc -c target/cdf-benchmarks/ec2-baseline/report.json; ./target/release/cdf-p3-lab host-class'`
- `bash -n tools/p3-ec2-benchmark-host.sh && tools/p3-ec2-benchmark-host.sh --dry-run tune-volume`
- `tools/p3-ec2-benchmark-host.sh tune-volume`
- `tools/p3-ec2-benchmark-host.sh run -- bash -lc 'set -euo pipefail; dir=target/cdf-benchmarks/ec2-disk; mkdir -p "$dir"; rm -f "$dir"/direct.bin; /usr/bin/time -f "direct_write_1g_real=%e user=%U sys=%S maxrss_kb=%M" dd if=/dev/zero of="$dir/direct.bin" bs=16M count=64 oflag=direct conv=fdatasync status=none; /usr/bin/time -f "direct_read_1g_real=%e user=%U sys=%S maxrss_kb=%M" dd if="$dir/direct.bin" of=/dev/null bs=16M iflag=direct status=none; ls -lh "$dir/direct.bin"'`
- `tools/p3-ec2-benchmark-host.sh --dry-run measure-cdf target/cdf-benchmarks/example.json fixture workload -- run tlc.yellow --json --progress never`
- `tools/p3-ec2-benchmark-host.sh sync-repo && tools/p3-ec2-benchmark-host.sh build`
- `CDF_BENCH_SAMPLES=1 CDF_BENCH_TIMEOUT_MS=120000 tools/p3-ec2-benchmark-host.sh measure-cdf target/cdf-benchmarks/l6-measure-cdf-smoke.json tmp_workspace inspect_resources -- inspect resources --color never --unicode never`
- `CDF_BENCH_SAMPLES=1 CDF_BENCH_IO_MODE=uncontrolled CDF_BENCH_TIMEOUT_MS=180000 tools/p3-ec2-benchmark-host.sh measure-cdf target/cdf-benchmarks/l6-measure-cdf-tlc-state-reset.json nyc_tlc_yellow_2024 tlc_e2e_duckdb_smoke -- run tlc.yellow --json --progress never`
- `CDF_BENCH_REMOTE_WORKSPACE=/home/ec2-user/cdf-bench/repo/target/cdf-benchmarks/g4-scheduler-default-20260718085558/local-workspace CDF_BENCH_SAMPLES=1 CDF_BENCH_IO_MODE=uncontrolled CDF_BENCH_TIMEOUT_MS=5000 tools/p3-ec2-benchmark-host.sh measure-cdf target/cdf-benchmarks/l6-timeout-proof.json nyc_tlc_yellow_2024 timeout_proof -- run tlc.yellow --jobs 12 --json --progress never`
- `CDF_BENCH_MEASURE_ENV_JSON='{"CDF_DUCKDB_THREADS":"4","CDF_DUCKDB_MEMORY_LIMIT":"3GiB"}' CDF_BENCH_SAMPLES=1 CDF_BENCH_IO_MODE=uncontrolled CDF_BENCH_TIMEOUT_MS=60000 tools/p3-ec2-benchmark-host.sh measure-cdf target/cdf-benchmarks/l6-measure-cdf-env-forward.json tmp_workspace inspect_with_child_env -- inspect resources --color never --unicode never`
- `tools/p3-ec2-benchmark-host.sh --dry-run preflight`
- `tools/p3-ec2-benchmark-host.sh --dry-run fetch target/cdf-benchmarks/example.json .10x/evidence/.storage/example.json`
- `tools/p3-ec2-benchmark-host.sh sync-repo && tools/p3-ec2-benchmark-host.sh build`
- `tools/p3-ec2-benchmark-host.sh preflight`
- `tools/p3-ec2-benchmark-host.sh fetch .cdf-bench-revision.env .10x/evidence/.storage/2026-07-18-p3-l6-ec2-preflight-revision.env`
- `tools/p3-ec2-benchmark-host.sh fetch .cdf-bench-build.env .10x/evidence/.storage/2026-07-18-p3-l6-ec2-preflight-build.env`
- `tools/p3-ec2-benchmark-host.sh sync-repo && tools/p3-ec2-benchmark-host.sh build && tools/p3-ec2-benchmark-host.sh preflight`
- `tools/p3-ec2-benchmark-host.sh fetch .cdf-bench-revision.env .10x/evidence/.storage/2026-07-18-p3-l6-ec2-current-clean-revision.env`
- `tools/p3-ec2-benchmark-host.sh fetch .cdf-bench-build.env .10x/evidence/.storage/2026-07-18-p3-l6-ec2-current-clean-build.env`

Observed outputs included:

- Host class: `host-class-95da083e15eebd1c`
- Release binary verification: `cdf 0.1.0` plus sanitized `cdf-p3-lab host` JSON
- CPU/memory class: 16 logical cores, 8 physical cores, Intel Xeon Platinum 8488C, about 30 GiB RAM
- OS/storage: Amazon Linux 2023, kernel `6.1.176-221.360.amzn2023.x86_64`, xfs root filesystem
- Toolchain: `rustc 1.97.1`, `cargo 1.97.1`
- Workspace smoke: seven compiled resources from `/Users/alexanderbut/code_projects/tmp`
- Baseline report size: 15,478 bytes
- Root volume before tuning: gp3 3,000 IOPS / 125 MiB/s throughput
- Root volume after tuning: gp3 16,000 IOPS / 1,000 MiB/s throughput
- Direct durable 1 GiB disk probe after tuning: `direct_write_1g_real=0.43`, `direct_read_1g_real=0.61`
- Benchmark SSH access was rotated after accidental local private-key exposure in tool output; the old EC2 key pair was deleted, the old local private key was removed, and `tools/p3-ec2-benchmark-host.sh status` succeeds with the replacement key.
- `measure-cdf` smoke: observed host-labeled `inspect_resources` cell, revision `eeef664909b29cf0ee57b2f78423af7e8965b484+dirty`, one sample, median `24,180,945ns`.
- `measure-cdf` TLC state-reset smoke: observed host-labeled `cdf run tlc.yellow --json --progress never` cell, one uncontrolled sample, 2,964,624 rows, ten extracted phase metrics, median `3,769,248,397ns`.
- `measure-cdf` timeout proof: failed host-labeled cell with `CDF command exceeded worker timeout of 4000ms`; immediate process-tree inspection found no remaining `cdf-p3-lab` worker or release `cdf` workload process.
- `measure-cdf` environment-forwarding proof: observed host-labeled `inspect_with_child_env` cell, one uncontrolled sample, median `30,362,231ns`; the generated worker request records `environment = {"CDF_DUCKDB_MEMORY_LIMIT":"3GiB","CDF_DUCKDB_THREADS":"4"}`, `workspace_mode = "fresh_copy"`, `preserve_state = false`, and child timeout `59000`.
- Strict preflight proof: `preflight=ok`; instance `i-05011a85b7f2a33fe`; instance type `c7i.4xlarge`; root volume `vol-02f4b599167f8831c` as gp3 `16000` IOPS / `1000` MiB/s; synced revision and built revision both `92887c11ae50779d00773095d5f6da67e9dcce3d+dirty`; build marker timestamp `2026-07-18T12:05:21Z`; host class `host-class-95da083e15eebd1c`; `cdf 0.1.0`; host fingerprint schema `1`; about `217` GB free; synced workspace present.
- Artifact fetch proof: remote `.cdf-bench-revision.env` and `.cdf-bench-build.env` were copied into local `.10x/evidence/.storage/`, proving benchmark JSON and request artifacts can return through the helper instead of manual `scp` or pasted transcripts.
- Closure clean preflight proof: after the final L6 diagnostic commit, `sync-repo && build && preflight` passed at clean revision/build `a37a4d8645bfcc1919c04e22615e5364542ad238`; release builds were cache hits (`0.27s` and `0.25s`); root volume remained gp3 `16000` IOPS / `1000` MiB/s; host class remained `host-class-95da083e15eebd1c`; workspace was present; free space was `205647929344` bytes; current revision/build markers were fetched into local evidence storage.

## What it supports or challenges

This supports `.10x/tickets/done/2026-07-18-p3-l6-ec2-benchmark-host.md` by proving that benchmark promotion evidence can be collected on a reusable, labeled EC2 host rather than on the user's laptop. It also supports the P3 performance discipline: performance-sensitive defaults now have a practical path to host-labeled evidence with revision, dependency, toolchain, host-class labels, and an explicit storage floor.

The live path challenged the dry-run-only assumption that shell command construction was sufficient. Real execution exposed missing host prerequisites (`python3-devel`), an Amazon Linux `curl-minimal` package conflict, the need for explicit revision labels when `.git` is excluded from repo sync, the need for minimal workspace sync so generated benchmark artifacts do not distort host state, and the need to avoid default gp3 throughput for any storage-sensitive P3 measurement.

The measured-command path challenged another bad assumption: a fresh copied workspace is not enough if `.cdf/state.db` is copied too. A first TLC smoke became a checkpoint no-op, so `cdf-command-worker` now drops runtime state by default and requires `CDF_BENCH_MEASURE_PRESERVE_STATE=1` for intentional resume/no-op measurements. This supports the reliability requirement directly: benchmark setup is outside the timed region, but cannot silently change the workload class.

The timeout proof challenged the assumption that the outer lab process timeout was enough. `run-cell` observes the worker process, but the expensive release `cdf` process is spawned one level deeper. The worker now enforces its own timeout and terminates the child process group, so failed/timed-out benchmark cells cannot leave hidden work consuming the reusable EC2 host.

The environment-forwarding proof closes a practical benchmark gap exposed by DuckDB/native-resource diagnostics: tuning knobs must be part of the supervised child request, not ad-hoc SSH shell state. `CDF_BENCH_MEASURE_ENV_JSON` is decoded and validated into the `cdf-command-worker` request, so destination/source tuning cells keep the same timeout, workspace-copy, and state-reset semantics as ordinary measured `cdf` commands.

The preflight proof closes the last major measurement-authority gap found in this tranche: a synced remote revision is not enough unless the optimized release binaries are known to have been built from that same revision. `build` now writes `.cdf-bench-build.env` after successful release builds, and `preflight` rejects stale or missing build markers before a host result can be treated as promotion evidence.

## Limits

The EC2 instance was intentionally left running for reuse across the current benchmark tranche. Therefore this evidence proves provisioning, reuse readiness, bootstrap, synchronization, build, verification, workspace compilation, and baseline emission, but not final tranche teardown. `.10x/tickets/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md` owns the eventual teardown evidence.

The baseline report is a smoke-scale machine-evidence proof, not a claim that the P3 throughput envelope is met. Large TLC/TPC-H/stress cells remain owned by their P3 performance tickets and must reuse the same host-labeled path.

The TLC `measure-cdf` observation is also a smoke of the measured-command path, not the full-year G4 envelope. It exercised one selected TLC partition from the synced workspace and proves row/phase extraction plus runtime-state reset behavior. G4's full-year target remains owned by `.10x/tickets/done/2026-07-11-p3-g4-tlc-remote-io-envelope.md`.

Any storage-sensitive measurement recorded before the gp3 tuning is diagnostic only, because the host was capped at the default 125 MiB/s gp3 throughput at that time.

The SSH key rotation proves continued access to this live tranche host, but the replacement private key remains ignored local state under `target/`; it is not committed evidence and must still be torn down with the instance at tranche close.

The preflight result was collected against a dirty local revision because this ticket intentionally validated the helper changes before commit. Future promotion cells should either be clean-commit measurements or explicitly label the dirty diff and retain the patch context.
