#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: tools/p3-ec2-benchmark-host.sh [--dry-run] <command>

Commands:
  plan             Print the configured benchmark-host plan and read-only AWS identity.
  status           Print recorded instance status, and host facts when SSH is available.
  provision        Launch/reuse a benchmark EC2 instance and write target/cdf-benchmarks/ec2-host/state.env.
  bootstrap        Install host build prerequisites and Rust toolchain.
  sync-repo        Rsync this repo to the benchmark host, honoring .gitignore and excluding local state.
  sync-workspace   Rsync a CDF workspace to the benchmark host.
  build            Build optimized release CDF and cdf-p3-lab binaries on the benchmark host.
  cdf -- ARGS...   Run the on-host release CDF binary from the synced CDF workspace.
  lab -- ARGS...   Run the on-host release cdf-p3-lab binary from the synced repo.
  run -- CMD...    Run an arbitrary command from the synced repo on the benchmark host.
  teardown         Terminate the benchmark instance recorded in state.env.

Environment:
  AWS_PROFILE                       default: PowerUser-617739438897
  AWS_REGION                        default: aws configure get region, then us-west-2
  CDF_BENCH_INSTANCE_TYPE           default: c7i.4xlarge
  CDF_BENCH_VOLUME_GB               default: 250
  CDF_BENCH_SUBNET_ID               required for provision without a launch template
  CDF_BENCH_SECURITY_GROUP_ID       required for provision without a launch template
  CDF_BENCH_KEY_NAME                required for SSH provisioning/sync
  CDF_BENCH_LAUNCH_TEMPLATE_ID      optional alternative to subnet/security group/key launch args
  CDF_BENCH_LAUNCH_TEMPLATE_VERSION default: latest
  CDF_BENCH_SSH_KEY                 private key path for ssh/rsync
  CDF_BENCH_SSH_USER                default: ec2-user
  CDF_BENCH_HOST                    overrides host read from state.env for ssh/rsync
  CDF_BENCH_REMOTE_ROOT             default: /home/ec2-user/cdf-bench
  CDF_BENCH_WORKSPACE               local CDF workspace to sync for sync-workspace
  CDF_BENCH_RUST_TOOLCHAIN          default: stable
  CDF_BENCH_STATE                   default: target/cdf-benchmarks/ec2-host/state.env

Safety:
  --dry-run prints live mutating commands without executing them.
  Repo sync excludes .git, target, .env*, local AWS/Codex config, and common secret directories.
  The same recorded instance is reused until teardown removes state.env after termination.
EOF
}

dry_run=0
if [[ "${1:-}" == "--dry-run" ]]; then
  dry_run=1
  shift
fi

command="${1:-}"
if [[ -z "${command}" || "${command}" == "-h" || "${command}" == "--help" ]]; then
  usage
  exit 0
fi
shift || true

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
aws_profile="${AWS_PROFILE:-PowerUser-617739438897}"
aws_region="${AWS_REGION:-$(AWS_PROFILE="${aws_profile}" aws configure get region 2>/dev/null || true)}"
aws_region="${aws_region:-us-west-2}"
state_file="${CDF_BENCH_STATE:-${repo_root}/target/cdf-benchmarks/ec2-host/state.env}"
remote_root="${CDF_BENCH_REMOTE_ROOT:-/home/ec2-user/cdf-bench}"
remote_repo="${remote_root}/repo"
remote_workspace="${remote_root}/workspace"
ssh_user="${CDF_BENCH_SSH_USER:-ec2-user}"
instance_type="${CDF_BENCH_INSTANCE_TYPE:-c7i.4xlarge}"
volume_gb="${CDF_BENCH_VOLUME_GB:-250}"
rust_toolchain="${CDF_BENCH_RUST_TOOLCHAIN:-stable}"

aws_cmd=(aws --profile "${aws_profile}" --region "${aws_region}")

quote_cmd() {
  local quoted=()
  local arg
  for arg in "$@"; do
    quoted+=("$(printf '%q' "${arg}")")
  done
  printf '%s\n' "${quoted[*]}"
}

run_cmd() {
  if [[ "${dry_run}" -eq 1 ]]; then
    quote_cmd "$@"
  else
    "$@"
  fi
}

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "missing required environment variable: ${name}" >&2
    exit 2
  fi
}

load_state() {
  if [[ -f "${state_file}" ]]; then
    # shellcheck disable=SC1090
    source "${state_file}"
  fi
}

target_host() {
  load_state
  local host="${CDF_BENCH_HOST:-${public_dns_name:-}}"
  if [[ -z "${host}" ]]; then
    echo "missing benchmark host; set CDF_BENCH_HOST or run provision first" >&2
    exit 2
  fi
  printf '%s\n' "${host}"
}

ssh_base() {
  require_env CDF_BENCH_SSH_KEY
  local host
  host="$(target_host)"
  printf '%s\n' ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}"
}

remote_command() {
  local quoted=()
  local arg
  for arg in "$@"; do
    quoted+=("$(printf '%q' "${arg}")")
  done
  printf '%s\n' "${quoted[*]}"
}

remote_prelude='if [ -f "$HOME/.cargo/env" ]; then . "$HOME/.cargo/env"; fi'

case "${command}" in
  plan)
    echo "repo_root=${repo_root}"
    echo "state_file=${state_file}"
    echo "aws_profile=${aws_profile}"
    echo "aws_region=${aws_region}"
    echo "instance_type=${instance_type}"
    echo "volume_gb=${volume_gb}"
    echo "remote_root=${remote_root}"
    echo "remote_repo=${remote_repo}"
    echo "remote_workspace=${remote_workspace}"
    echo "ssh_user=${ssh_user}"
    echo "rust_toolchain=${rust_toolchain}"
    "${aws_cmd[@]}" sts get-caller-identity --output json
    "${aws_cmd[@]}" ssm get-parameter \
      --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
      --query 'Parameter.Value' \
      --output text
    ;;

  status)
    load_state
    if [[ -n "${instance_id:-}" ]]; then
      "${aws_cmd[@]}" ec2 describe-instances \
        --instance-ids "${instance_id}" \
        --query 'Reservations[0].Instances[0].{InstanceId:InstanceId,State:State.Name,PublicDnsName:PublicDnsName,PublicIpAddress:PublicIpAddress,InstanceType:InstanceType,ImageId:ImageId,LaunchTime:LaunchTime,SubnetId:SubnetId,SecurityGroups:SecurityGroups[].GroupId,BlockDevices:BlockDeviceMappings[].Ebs.VolumeId}' \
        --output json
    else
      echo "no instance_id in ${state_file}" >&2
    fi
    if [[ -n "${CDF_BENCH_SSH_KEY:-}" && ( -n "${CDF_BENCH_HOST:-}" || -n "${public_dns_name:-}" ) ]]; then
      host="$(target_host)"
      run_cmd ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}" \
        "${remote_prelude}; uname -a; echo '--- cpu ---'; nproc; lscpu | sed -n '1,24p'; echo '--- disk ---'; df -h '${remote_root}' || df -h; echo '--- rust ---'; rustc --version || true; cargo --version || true"
    fi
    ;;

  provision)
    mkdir -p "$(dirname "${state_file}")"
    existing_instance_id=""
    if [[ -f "${state_file}" ]]; then
      # shellcheck disable=SC1090
      source "${state_file}"
      existing_instance_id="${instance_id:-}"
    fi
    if [[ -n "${existing_instance_id}" ]]; then
      echo "reusing recorded instance_id=${existing_instance_id}" >&2
      "${aws_cmd[@]}" ec2 describe-instances \
        --instance-ids "${existing_instance_id}" \
        --query 'Reservations[0].Instances[0].{InstanceId:InstanceId,State:State.Name,PublicDnsName:PublicDnsName,InstanceType:InstanceType,LaunchTime:LaunchTime}' \
        --output json
      exit 0
    fi
    ami_id="$("${aws_cmd[@]}" ssm get-parameter \
      --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
      --query 'Parameter.Value' \
      --output text)"
    launch_args=(
      ec2 run-instances
      --image-id "${ami_id}"
      --instance-type "${instance_type}"
      --block-device-mappings "[{\"DeviceName\":\"/dev/xvda\",\"Ebs\":{\"VolumeSize\":${volume_gb},\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]"
      --tag-specifications 'ResourceType=instance,Tags=[{Key=Project,Value=CDF},{Key=Purpose,Value=P3Benchmark},{Key=Owner,Value=Codex},{Key=Teardown,Value=Required}]' 'ResourceType=volume,Tags=[{Key=Project,Value=CDF},{Key=Purpose,Value=P3Benchmark},{Key=Owner,Value=Codex},{Key=Teardown,Value=Required}]'
      --query 'Instances[0].InstanceId'
      --output text
    )
    if [[ -n "${CDF_BENCH_LAUNCH_TEMPLATE_ID:-}" ]]; then
      launch_args+=(--launch-template "LaunchTemplateId=${CDF_BENCH_LAUNCH_TEMPLATE_ID},Version=${CDF_BENCH_LAUNCH_TEMPLATE_VERSION:-latest}")
    else
      require_env CDF_BENCH_SUBNET_ID
      require_env CDF_BENCH_SECURITY_GROUP_ID
      require_env CDF_BENCH_KEY_NAME
      launch_args+=(--subnet-id "${CDF_BENCH_SUBNET_ID}" --security-group-ids "${CDF_BENCH_SECURITY_GROUP_ID}" --key-name "${CDF_BENCH_KEY_NAME}")
    fi
    if [[ "${dry_run}" -eq 1 ]]; then
      run_cmd "${aws_cmd[@]}" "${launch_args[@]}"
      exit 0
    fi
    instance_id="$("${aws_cmd[@]}" "${launch_args[@]}")"
    "${aws_cmd[@]}" ec2 wait instance-running --instance-ids "${instance_id}"
    read -r public_dns_name public_ip_address < <("${aws_cmd[@]}" ec2 describe-instances \
      --instance-ids "${instance_id}" \
      --query 'Reservations[0].Instances[0].[PublicDnsName,PublicIpAddress]' \
      --output text)
    {
      echo "instance_id=${instance_id}"
      echo "public_dns_name=${public_dns_name}"
      echo "public_ip_address=${public_ip_address}"
      echo "aws_profile=${aws_profile}"
      echo "aws_region=${aws_region}"
      echo "instance_type=${instance_type}"
      echo "volume_gb=${volume_gb}"
      echo "created_revision=$(git -C "${repo_root}" rev-parse HEAD)"
    } > "${state_file}"
    cat "${state_file}"
    ;;

  sync-repo)
    require_env CDF_BENCH_SSH_KEY
    host="$(target_host)"
    run_cmd ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}" "mkdir -p '${remote_repo}'"
    run_cmd rsync -az --delete \
      --filter=':- .gitignore' \
      --exclude='.git/' \
      --exclude='target/' \
      --exclude='.env' \
      --exclude='.env.*' \
      --exclude='.aws/' \
      --exclude='.codex/' \
      --exclude='secrets/' \
      --exclude='.cdf/secrets/' \
      -e "ssh -i ${CDF_BENCH_SSH_KEY} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new" \
      "${repo_root}/" "${ssh_user}@${host}:${remote_repo}/"
    ;;

  bootstrap)
    require_env CDF_BENCH_SSH_KEY
    host="$(target_host)"
    run_cmd ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}" \
      "sudo dnf install -y git rsync gcc gcc-c++ clang llvm cmake make perl pkgconf-pkg-config openssl-devel sqlite-devel python3 curl tar gzip xz zstd && if ! command -v rustup >/dev/null 2>&1; then curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain '${rust_toolchain}' --profile minimal; fi && ${remote_prelude}; rustup toolchain install '${rust_toolchain}' --profile minimal && rustup default '${rust_toolchain}' && rustc --version && cargo --version"
    ;;

  sync-workspace)
    require_env CDF_BENCH_WORKSPACE
    require_env CDF_BENCH_SSH_KEY
    host="$(target_host)"
    run_cmd ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}" "mkdir -p '${remote_workspace}'"
    run_cmd rsync -az --delete \
      --filter=':- .gitignore' \
      --exclude='.git/' \
      --exclude='target/' \
      --exclude='.env' \
      --exclude='.env.*' \
      --exclude='secrets/' \
      --exclude='.cdf/secrets/' \
      -e "ssh -i ${CDF_BENCH_SSH_KEY} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new" \
      "${CDF_BENCH_WORKSPACE%/}/" "${ssh_user}@${host}:${remote_workspace}/"
    ;;

  build)
    require_env CDF_BENCH_SSH_KEY
    host="$(target_host)"
    run_cmd ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}" \
      "${remote_prelude}; cd '${remote_repo}' && CARGO_BUILD_JOBS=\$(nproc) cargo build -p cdf-cli --bin cdf --release --locked -j \$(nproc) && CARGO_BUILD_JOBS=\$(nproc) cargo build -p cdf-benchmarks --bin cdf-p3-lab --release --locked -j \$(nproc) && target/release/cdf --version && target/release/cdf-p3-lab host"
    ;;

  cdf)
    if [[ "${1:-}" == "--" ]]; then
      shift
    fi
    require_env CDF_BENCH_SSH_KEY
    host="$(target_host)"
    remote_args="$(remote_command "$@")"
    run_cmd ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}" \
      "${remote_prelude}; cd '${remote_workspace}' && '${remote_repo}/target/release/cdf' ${remote_args}"
    ;;

  lab)
    if [[ "${1:-}" == "--" ]]; then
      shift
    fi
    require_env CDF_BENCH_SSH_KEY
    host="$(target_host)"
    remote_args="$(remote_command "$@")"
    run_cmd ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}" \
      "${remote_prelude}; cd '${remote_repo}' && '${remote_repo}/target/release/cdf-p3-lab' ${remote_args}"
    ;;

  run)
    if [[ "${1:-}" == "--" ]]; then
      shift
    fi
    if [[ "$#" -eq 0 ]]; then
      echo "run requires a command after --" >&2
      exit 2
    fi
    require_env CDF_BENCH_SSH_KEY
    host="$(target_host)"
    remote_args="$(remote_command "$@")"
    run_cmd ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}" \
      "${remote_prelude}; cd '${remote_repo}' && ${remote_args}"
    ;;

  teardown)
    load_state
    if [[ -z "${instance_id:-}" ]]; then
      echo "no instance_id in ${state_file}" >&2
      exit 0
    fi
    run_cmd "${aws_cmd[@]}" ec2 terminate-instances --instance-ids "${instance_id}" --output json
    if [[ "${dry_run}" -eq 0 ]]; then
      "${aws_cmd[@]}" ec2 wait instance-terminated --instance-ids "${instance_id}"
      rm -f "${state_file}"
    fi
    ;;

  *)
    usage >&2
    exit 2
    ;;
esac
