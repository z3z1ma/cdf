Status: recorded
Created: 2026-07-18
Updated: 2026-07-18

# P3 L6 EC2 benchmark host live proof

## Observation

The dedicated EC2 benchmark-host workflow now works against a live FQ12 benchmark instance instead of only dry-run command construction. The live path provisioned a CDF-tagged `c7i.4xlarge` Amazon Linux 2023 host, bootstrapped the Rust/build environment, synchronized the repository and a minimal CDF workspace, built optimized release binaries on-host, verified both binaries, compiled the user's exploratory CDF workspace, emitted a three-sample machine baseline report, and corrected the root-volume storage floor from default gp3 throughput to an explicit benchmark-class gp3 configuration.

The machine report is stored at `.10x/evidence/.storage/2026-07-18-p3-l6-ec2-baseline-report.json`.

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

## What it supports or challenges

This supports `.10x/tickets/2026-07-18-p3-l6-ec2-benchmark-host.md` by proving that benchmark promotion evidence can be collected on a reusable, labeled EC2 host rather than on the user's laptop. It also supports the P3 performance discipline: performance-sensitive defaults now have a practical path to host-labeled evidence with revision, dependency, toolchain, host-class labels, and an explicit storage floor.

The live path challenged the dry-run-only assumption that shell command construction was sufficient. Real execution exposed missing host prerequisites (`python3-devel`), an Amazon Linux `curl-minimal` package conflict, the need for explicit revision labels when `.git` is excluded from repo sync, the need for minimal workspace sync so generated benchmark artifacts do not distort host state, and the need to avoid default gp3 throughput for any storage-sensitive P3 measurement.

## Limits

The EC2 instance was intentionally left running for reuse across the current benchmark tranche. Therefore this evidence proves provisioning, reuse readiness, bootstrap, synchronization, build, verification, workspace compilation, and baseline emission, but not final tranche teardown. Teardown remains the acceptance condition that will close the L6 ticket after the benchmark tranche is complete.

The baseline report is a smoke-scale machine-evidence proof, not a claim that the P3 throughput envelope is met. Large TLC/TPC-H/stress cells remain owned by their P3 performance tickets and must reuse the same host-labeled path.

Any storage-sensitive measurement recorded before the gp3 tuning is diagnostic only, because the host was capped at the default 125 MiB/s gp3 throughput at that time.
