Status: active
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
- 2026-07-18: Added `tools/p3-ec2-benchmark-host.sh` as the first dry-runable benchmark-host helper. The script centralizes the candidate FQ12 PowerUser profile, region, instance shape, state-file location, launch/reuse, ignore-respecting repo/workspace sync, on-host release build, remote command execution, and explicit teardown. Live provisioning requires explicit subnet/security group/key or launch template inputs; dry-run command construction works without cloud writes.
- 2026-07-18: Read-only EC2 discovery found a default `us-west-2` VPC, public default subnets, the default security group in that VPC, and several account key-pair names. No private SSH key was present under `~/.ssh`, so no instance was launched. The next live tranche must either provide `CDF_BENCH_SSH_KEY` for a known key pair, create an explicitly owned ephemeral key as part of the tranche, or switch the helper to an SSM/Instance-Connect sync path.
- 2026-07-18: Tightened the helper around the user-ratified benchmarking workflow: a single recorded host is reused for the tranche; `bootstrap` prepares Amazon Linux build prerequisites and Rust; `build` now produces both optimized release binaries (`cdf` and `cdf-p3-lab`) and prints host facts; `cdf -- ...` runs the release CDF binary from the synced workspace; `lab -- ...` runs the release lab binary from the synced repo; `status` reports recorded EC2 state and remote host/toolchain facts when SSH is available. `crates/cdf-benchmarks/README.md` now documents this sequence as the dedicated-host path.

## Blockers

Live benchmark execution still requires selecting or providing subnet/security group/key/launch-template inputs and a private SSH key path. The script intentionally does not invent those network/security choices. Current local host has no private key in `~/.ssh`.

## Evidence

- 2026-07-18 read-only AWS inspection:
  - `command -v aws` found `/run/current-system/sw/bin/aws`.
  - `aws --version` reported `aws-cli/2.34.24 Python/3.13.13 Darwin/25.5.0 source/arm64`.
  - `aws configure list-profiles` reported `PowerUser-617739438897`.
  - `AWS_PROFILE=PowerUser-617739438897 aws sts get-caller-identity --output json` succeeded for account `617739438897` through an assumed PowerUser SSO role; no secret values were recorded.
  - `AWS_PROFILE=PowerUser-617739438897 aws configure get region` reported `us-west-2`.
- 2026-07-18 dry-run/script validation:
  - `bash -n tools/p3-ec2-benchmark-host.sh` — passed.
  - `tools/p3-ec2-benchmark-host.sh --dry-run plan` — printed repo/state/profile/region/default host facts, read-only caller identity, and current Amazon Linux 2023 x86_64 AMI id.
  - `CDF_BENCH_SUBNET_ID=subnet-dryrun CDF_BENCH_SECURITY_GROUP_ID=sg-dryrun CDF_BENCH_KEY_NAME=cdf-dryrun tools/p3-ec2-benchmark-host.sh --dry-run provision` — printed the exact `aws ec2 run-instances` command with benchmark tags, gp3 delete-on-termination root volume, selected AMI, and supplied dry-run network inputs; no instance was created.
  - `CDF_BENCH_HOST=example.invalid CDF_BENCH_SSH_KEY=/tmp/cdf-bench-key tools/p3-ec2-benchmark-host.sh --dry-run sync-repo` — printed SSH mkdir and rsync commands that honor `.gitignore` and exclude `.git/`, `target/`, `.env*`, `.aws/`, `.codex/`, `secrets/`, and `.cdf/secrets/`.
  - `CDF_BENCH_HOST=example.invalid CDF_BENCH_SSH_KEY=/tmp/cdf-bench-key tools/p3-ec2-benchmark-host.sh --dry-run build` — printed the on-host release build command using `CARGO_BUILD_JOBS=$(nproc)` and `cargo build -p cdf-cli --bin cdf --release --locked -j $(nproc)`.
  - `CDF_BENCH_HOST=example.invalid CDF_BENCH_SSH_KEY=/tmp/cdf-bench-key CDF_BENCH_WORKSPACE=/tmp/cdf-workspace tools/p3-ec2-benchmark-host.sh --dry-run sync-workspace` — printed workspace mkdir/rsync commands excluding local env/secret paths.
  - Limit: no cloud resource was created, no SSH connection was attempted, and no release build ran on EC2. This validates command construction/redaction only.
- 2026-07-18 read-only launch-input discovery:
  - `aws ec2 describe-vpcs --filters Name=is-default,Values=true` found default VPC `vpc-d097bfa8`.
  - `aws ec2 describe-subnets --filters Name=default-for-az,Values=true` found public default subnets in `us-west-2a` through `us-west-2d`; `subnet-acc642d4` in `us-west-2a` is a viable candidate input, but not selected as authority.
  - `aws ec2 describe-security-groups --filters Name=group-name,Values=default` found default-VPC security group `sg-7deb0443`; it currently only permits self-referential ingress and all outbound, so direct SSH would require an explicit ingress/security decision before use.
  - `aws ec2 describe-key-pairs` found existing key-pair names including `local-dev`, but `find ~/.ssh -maxdepth 1 -type f` found no local private key file. No key pair was selected.
- 2026-07-18 tranche-command validation:
  - `bash -n tools/p3-ec2-benchmark-host.sh` — passed.
  - `tools/p3-ec2-benchmark-host.sh --dry-run plan` — passed against read-only AWS identity/AMI lookup.
  - `CDF_BENCH_HOST=example.invalid CDF_BENCH_SSH_KEY=/tmp/cdf-bench-key tools/p3-ec2-benchmark-host.sh --dry-run status` — printed recorded-state absence and dry-run remote host-fact command; no connection attempted.
  - `CDF_BENCH_HOST=example.invalid CDF_BENCH_SSH_KEY=/tmp/cdf-bench-key tools/p3-ec2-benchmark-host.sh --dry-run bootstrap` — printed Amazon Linux package/Rust setup command.
  - `CDF_BENCH_HOST=example.invalid CDF_BENCH_SSH_KEY=/tmp/cdf-bench-key tools/p3-ec2-benchmark-host.sh --dry-run build` — printed optimized release builds for both `cdf-cli --bin cdf` and `cdf-benchmarks --bin cdf-p3-lab`, followed by version/host-fingerprint commands.
  - `CDF_BENCH_HOST=example.invalid CDF_BENCH_SSH_KEY=/tmp/cdf-bench-key CDF_BENCH_WORKSPACE=/tmp/cdf-workspace tools/p3-ec2-benchmark-host.sh --dry-run cdf -- run tlc.yellow --progress never` — printed a workspace-rooted invocation of the synced release `cdf` binary with remote-quoted arguments.
  - `CDF_BENCH_HOST=example.invalid CDF_BENCH_SSH_KEY=/tmp/cdf-bench-key tools/p3-ec2-benchmark-host.sh --dry-run lab -- host` — printed a repo-rooted invocation of the synced release `cdf-p3-lab` binary.
  - Limit: this still validates command construction only. L6 remains active until a real host proves provision/reuse/bootstrap/sync/build/run/teardown end to end.

## Review

Pass for the dry-run/procedure slice. The helper does not create hidden long-lived cloud state, defaults only the benchmark shape/profile/region while requiring explicit network/security inputs for live writes, and records teardown as a first-class command. L6 remains active until a real benchmark tranche proves provision/reuse/build/run/teardown end to end.

## Retrospective

Benchmark infra needs the same fail-closed discipline as runtime code: dry-run first, explicit state file, explicit teardown, and no silent defaults for VPC/security inputs. Laptop timings can remain useful as a rejection filter, but promotion needs a host whose load and hardware class are part of the evidence.
