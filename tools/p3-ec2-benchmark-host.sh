#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: tools/p3-ec2-benchmark-host.sh [--dry-run] <command>

Commands:
  plan             Print the configured benchmark-host plan and read-only AWS identity.
  prepare-ssh      Create/reuse CDF-owned SSH launch inputs: subnet, SG, current-IP ingress, key.
  status           Print recorded instance status, and host facts when SSH is available.
  provision        Launch/reuse a benchmark EC2 instance and write target/cdf-benchmarks/ec2-host/state.env.
  wait-ssh         Wait until SSH accepts commands on the recorded host.
  bootstrap        Install host build prerequisites and Rust toolchain.
  sync-repo        Rsync this repo to the benchmark host, honoring .gitignore and excluding local state.
  sync-workspace   Rsync a CDF workspace to the benchmark host.
  build            Build optimized release CDF and cdf-p3-lab binaries on the benchmark host.
  verify           Run on-host cdf --version and cdf-p3-lab host.
  cdf -- ARGS...   Run the on-host release CDF binary from the synced CDF workspace.
  lab -- ARGS...   Run the on-host release cdf-p3-lab binary from the synced repo.
  run -- CMD...    Run an arbitrary command from the synced repo on the benchmark host.
  teardown         Terminate the benchmark instance recorded in state.env.

Environment:
  AWS_PROFILE                       default: first configured PowerUser-* profile, then PowerUser-FQ12
  AWS_REGION                        default: aws configure get region, then us-west-2
  CDF_BENCH_INSTANCE_TYPE           default: c7i.4xlarge
  CDF_BENCH_VOLUME_GB               default: 250
  CDF_BENCH_VPC_ID                  optional; default VPC when prepare-ssh is used
  CDF_BENCH_SUBNET_ID               required for provision without a launch template
  CDF_BENCH_SECURITY_GROUP_ID       required for provision without a launch template
  CDF_BENCH_SECURITY_GROUP_NAME     default: cdf-p3-benchmark-sg
  CDF_BENCH_SSH_CIDR                default: current public IPv4 /32
  CDF_BENCH_KEY_NAME                required for SSH provisioning/sync
  CDF_BENCH_LAUNCH_TEMPLATE_ID      optional alternative to subnet/security group/key launch args
  CDF_BENCH_LAUNCH_TEMPLATE_VERSION default: latest
  CDF_BENCH_SSH_KEY                 private key path for ssh/rsync
  CDF_BENCH_SSH_USER                default: ec2-user
  CDF_BENCH_HOST                    overrides host read from state.env for ssh/rsync
  CDF_BENCH_REMOTE_ROOT             default: /home/ec2-user/cdf-bench
  CDF_BENCH_WORKSPACE               local CDF workspace to sync for sync-workspace
  CDF_BENCH_WORKSPACE_SYNC_MODE     default: minimal; use full for an ignore-filtered full tree
  CDF_BENCH_RUST_TOOLCHAIN          default: stable
  CDF_BENCH_STATE                   default: target/cdf-benchmarks/ec2-host/state.env
  CDF_BENCH_RESOURCE_STATE          default: target/cdf-benchmarks/ec2-host/ssh-resources.env

Safety:
  --dry-run prints live mutating commands without executing them.
  prepare-ssh creates only CDF-tagged security/key resources and restricts SSH to one CIDR.
  Repo sync excludes .git, target, .env*, local AWS/Codex config, and common secret directories.
  Workspace sync defaults to a minimal control-plane manifest and has an explicit full mode.
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

default_aws_profile() {
  aws configure list-profiles 2>/dev/null | awk '/^PowerUser-/ { print; exit }'
}

aws_profile="${AWS_PROFILE:-$(default_aws_profile)}"
aws_profile="${aws_profile:-PowerUser-FQ12}"
aws_region="${AWS_REGION:-$(AWS_PROFILE="${aws_profile}" aws configure get region 2>/dev/null || true)}"
aws_region="${aws_region:-us-west-2}"
state_file="${CDF_BENCH_STATE:-${repo_root}/target/cdf-benchmarks/ec2-host/state.env}"
state_dir="$(dirname "${state_file}")"
resource_state_file="${CDF_BENCH_RESOURCE_STATE:-${state_dir}/ssh-resources.env}"
remote_root="${CDF_BENCH_REMOTE_ROOT:-/home/ec2-user/cdf-bench}"
remote_repo="${remote_root}/repo"
remote_workspace="${remote_root}/workspace"
ssh_user="${CDF_BENCH_SSH_USER:-ec2-user}"
instance_type="${CDF_BENCH_INSTANCE_TYPE:-c7i.4xlarge}"
volume_gb="${CDF_BENCH_VOLUME_GB:-250}"
rust_toolchain="${CDF_BENCH_RUST_TOOLCHAIN:-stable}"
security_group_name="${CDF_BENCH_SECURITY_GROUP_NAME:-cdf-p3-benchmark-sg}"

safe_local_user="$(printf '%s' "${USER:-codex}" | tr -c '[:alnum:]-' '-')"
default_key_name="cdf-p3-benchmark-${safe_local_user}"

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

load_resource_state() {
  if [[ -f "${resource_state_file}" ]]; then
    # shellcheck disable=SC1090
    source "${resource_state_file}"
  fi
  CDF_BENCH_VPC_ID="${CDF_BENCH_VPC_ID:-${vpc_id:-}}"
  CDF_BENCH_SUBNET_ID="${CDF_BENCH_SUBNET_ID:-${subnet_id:-}}"
  CDF_BENCH_SECURITY_GROUP_ID="${CDF_BENCH_SECURITY_GROUP_ID:-${security_group_id:-}}"
  CDF_BENCH_KEY_NAME="${CDF_BENCH_KEY_NAME:-${key_name:-}}"
  CDF_BENCH_SSH_KEY="${CDF_BENCH_SSH_KEY:-${ssh_key:-}}"
  CDF_BENCH_SSH_CIDR="${CDF_BENCH_SSH_CIDR:-${ssh_cidr:-}}"
  export CDF_BENCH_VPC_ID CDF_BENCH_SUBNET_ID CDF_BENCH_SECURITY_GROUP_ID
  export CDF_BENCH_KEY_NAME CDF_BENCH_SSH_KEY CDF_BENCH_SSH_CIDR
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

current_public_cidr() {
  if [[ -n "${CDF_BENCH_SSH_CIDR:-}" ]]; then
    printf '%s\n' "${CDF_BENCH_SSH_CIDR}"
    return 0
  fi
  local ip
  ip="$(curl -fsSL https://checkip.amazonaws.com | tr -d '\r\n')"
  if [[ -z "${ip}" || "${ip}" == *[!0-9.]* ]]; then
    echo "could not determine current public IPv4 address; set CDF_BENCH_SSH_CIDR" >&2
    exit 2
  fi
  printf '%s/32\n' "${ip}"
}

aws_text_or_empty() {
  "${aws_cmd[@]}" "$@" --output text 2>/dev/null || true
}

none_to_empty() {
  local value="$1"
  if [[ "${value}" == "None" || "${value}" == "null" ]]; then
    printf '\n'
  else
    printf '%s\n' "${value}"
  fi
}

ensure_ssh_resources() {
  mkdir -p "${state_dir}"
  load_resource_state

  local resolved_vpc="${CDF_BENCH_VPC_ID:-}"
  if [[ -z "${resolved_vpc}" ]]; then
    resolved_vpc="$(none_to_empty "$(aws_text_or_empty ec2 describe-vpcs \
      --filters Name=is-default,Values=true \
      --query 'Vpcs[0].VpcId')")"
  fi
  if [[ -z "${resolved_vpc}" ]]; then
    echo "prepare-ssh requires CDF_BENCH_VPC_ID because no default VPC was found" >&2
    exit 2
  fi

  local resolved_subnet="${CDF_BENCH_SUBNET_ID:-}"
  if [[ -z "${resolved_subnet}" ]]; then
    resolved_subnet="$(none_to_empty "$(aws_text_or_empty ec2 describe-subnets \
      --filters "Name=vpc-id,Values=${resolved_vpc}" Name=default-for-az,Values=true \
      --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId')")"
  fi
  if [[ -z "${resolved_subnet}" ]]; then
    echo "prepare-ssh requires CDF_BENCH_SUBNET_ID because no default subnet was found" >&2
    exit 2
  fi

  local resolved_sg="${CDF_BENCH_SECURITY_GROUP_ID:-}"
  if [[ -z "${resolved_sg}" ]]; then
    resolved_sg="$(none_to_empty "$(aws_text_or_empty ec2 describe-security-groups \
      --filters "Name=vpc-id,Values=${resolved_vpc}" "Name=group-name,Values=${security_group_name}" \
      --query 'SecurityGroups[0].GroupId')")"
  fi
  if [[ -z "${resolved_sg}" ]]; then
    if [[ "${dry_run}" -eq 1 ]]; then
      run_cmd "${aws_cmd[@]}" ec2 create-security-group \
        --group-name "${security_group_name}" \
        --description "CDF P3 benchmark SSH access" \
        --vpc-id "${resolved_vpc}" \
        --query GroupId \
        --output text
      resolved_sg="sg-created-by-prepare-ssh"
    else
      resolved_sg="$("${aws_cmd[@]}" ec2 create-security-group \
        --group-name "${security_group_name}" \
        --description "CDF P3 benchmark SSH access" \
        --vpc-id "${resolved_vpc}" \
        --query GroupId \
        --output text)"
      "${aws_cmd[@]}" ec2 create-tags \
        --resources "${resolved_sg}" \
        --tags Key=Project,Value=CDF Key=Purpose,Value=P3Benchmark Key=Owner,Value=Codex Key=Teardown,Value=Required
    fi
  fi

  local cidr
  cidr="$(current_public_cidr)"
  if [[ "${resolved_sg}" == "sg-created-by-prepare-ssh" ]]; then
    run_cmd "${aws_cmd[@]}" ec2 authorize-security-group-ingress \
      --group-id "${resolved_sg}" \
      --ip-permissions "[{\"IpProtocol\":\"tcp\",\"FromPort\":22,\"ToPort\":22,\"IpRanges\":[{\"CidrIp\":\"${cidr}\",\"Description\":\"CDF benchmark SSH\"}]}]"
  else
    local existing_cidrs
    existing_cidrs="$("${aws_cmd[@]}" ec2 describe-security-groups \
      --group-ids "${resolved_sg}" \
      --query "SecurityGroups[0].IpPermissions[?IpProtocol=='tcp' && FromPort==\`22\` && ToPort==\`22\`].IpRanges[].CidrIp" \
      --output text | tr '\t' '\n')"
    if ! printf '%s\n' "${existing_cidrs}" | grep -qx "${cidr}"; then
      run_cmd "${aws_cmd[@]}" ec2 authorize-security-group-ingress \
        --group-id "${resolved_sg}" \
        --ip-permissions "[{\"IpProtocol\":\"tcp\",\"FromPort\":22,\"ToPort\":22,\"IpRanges\":[{\"CidrIp\":\"${cidr}\",\"Description\":\"CDF benchmark SSH\"}]}]"
    fi
  fi

  local resolved_key_name="${CDF_BENCH_KEY_NAME:-${default_key_name}}"
  local resolved_ssh_key="${CDF_BENCH_SSH_KEY:-${state_dir}/${resolved_key_name}.pem}"
  local key_exists
  key_exists="$(none_to_empty "$(aws_text_or_empty ec2 describe-key-pairs \
    --key-names "${resolved_key_name}" \
    --query 'KeyPairs[0].KeyName')")"
  if [[ -z "${key_exists}" ]]; then
    if [[ "${dry_run}" -eq 1 ]]; then
      run_cmd "${aws_cmd[@]}" ec2 create-key-pair \
        --key-name "${resolved_key_name}" \
        --key-type rsa \
        --query KeyMaterial \
        --output text
    else
      if [[ -e "${resolved_ssh_key}" ]]; then
        echo "refusing to overwrite existing SSH key path ${resolved_ssh_key}" >&2
        exit 2
      fi
      umask 077
      "${aws_cmd[@]}" ec2 create-key-pair \
        --key-name "${resolved_key_name}" \
        --key-type rsa \
        --tag-specifications 'ResourceType=key-pair,Tags=[{Key=Project,Value=CDF},{Key=Purpose,Value=P3Benchmark},{Key=Owner,Value=Codex},{Key=Teardown,Value=Required}]' \
        --query KeyMaterial \
        --output text > "${resolved_ssh_key}"
      chmod 600 "${resolved_ssh_key}"
    fi
  elif [[ ! -f "${resolved_ssh_key}" ]]; then
    echo "EC2 key pair ${resolved_key_name} exists but local private key ${resolved_ssh_key} is absent; set CDF_BENCH_KEY_NAME/CDF_BENCH_SSH_KEY for a key you hold or delete/recreate the key pair intentionally" >&2
    exit 2
  else
    chmod 600 "${resolved_ssh_key}" 2>/dev/null || true
  fi

  CDF_BENCH_VPC_ID="${resolved_vpc}"
  CDF_BENCH_SUBNET_ID="${resolved_subnet}"
  CDF_BENCH_SECURITY_GROUP_ID="${resolved_sg}"
  CDF_BENCH_KEY_NAME="${resolved_key_name}"
  CDF_BENCH_SSH_KEY="${resolved_ssh_key}"
  CDF_BENCH_SSH_CIDR="${cidr}"
  export CDF_BENCH_VPC_ID CDF_BENCH_SUBNET_ID CDF_BENCH_SECURITY_GROUP_ID
  export CDF_BENCH_KEY_NAME CDF_BENCH_SSH_KEY CDF_BENCH_SSH_CIDR

  if [[ "${dry_run}" -eq 0 ]]; then
    {
      echo "vpc_id=${CDF_BENCH_VPC_ID}"
      echo "subnet_id=${CDF_BENCH_SUBNET_ID}"
      echo "security_group_id=${CDF_BENCH_SECURITY_GROUP_ID}"
      echo "security_group_name=${security_group_name}"
      echo "ssh_cidr=${CDF_BENCH_SSH_CIDR}"
      echo "key_name=${CDF_BENCH_KEY_NAME}"
      echo "ssh_key=${CDF_BENCH_SSH_KEY}"
      echo "aws_profile=${aws_profile}"
      echo "aws_region=${aws_region}"
    } > "${resource_state_file}"
    cat "${resource_state_file}"
  else
    echo "would write ${resource_state_file}" >&2
  fi
}

case "${command}" in
  plan)
    echo "repo_root=${repo_root}"
    echo "state_file=${state_file}"
    echo "resource_state_file=${resource_state_file}"
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

  prepare-ssh)
    ensure_ssh_resources
    ;;

  status)
    load_resource_state
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
    mkdir -p "${state_dir}"
    load_resource_state
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
    if [[ -z "${CDF_BENCH_LAUNCH_TEMPLATE_ID:-}" && ( -z "${CDF_BENCH_SUBNET_ID:-}" || -z "${CDF_BENCH_SECURITY_GROUP_ID:-}" || -z "${CDF_BENCH_KEY_NAME:-}" ) ]]; then
      ensure_ssh_resources
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
      launch_args+=(
        --network-interfaces "[{\"DeviceIndex\":0,\"SubnetId\":\"${CDF_BENCH_SUBNET_ID}\",\"Groups\":[\"${CDF_BENCH_SECURITY_GROUP_ID}\"],\"AssociatePublicIpAddress\":true}]"
        --key-name "${CDF_BENCH_KEY_NAME}"
      )
    fi
    if [[ "${dry_run}" -eq 1 ]]; then
      run_cmd "${aws_cmd[@]}" "${launch_args[@]}"
      exit 0
    fi
    instance_id="$("${aws_cmd[@]}" "${launch_args[@]}")"
    "${aws_cmd[@]}" ec2 wait instance-running --instance-ids "${instance_id}"
    "${aws_cmd[@]}" ec2 wait instance-status-ok --instance-ids "${instance_id}"
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
      echo "subnet_id=${CDF_BENCH_SUBNET_ID:-}"
      echo "security_group_id=${CDF_BENCH_SECURITY_GROUP_ID:-}"
      echo "key_name=${CDF_BENCH_KEY_NAME:-}"
      echo "ssh_key=${CDF_BENCH_SSH_KEY:-}"
      echo "created_revision=$(git -C "${repo_root}" rev-parse HEAD)"
    } > "${state_file}"
    cat "${state_file}"
    ;;

  wait-ssh)
    load_resource_state
    require_env CDF_BENCH_SSH_KEY
    host="$(target_host)"
    if [[ "${dry_run}" -eq 1 ]]; then
      run_cmd ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}" "true"
      exit 0
    fi
    for attempt in $(seq 1 60); do
      if ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 "${ssh_user}@${host}" "true" >/dev/null 2>&1; then
        echo "ssh_ready=${host}"
        exit 0
      fi
      echo "waiting for SSH (${attempt}/60)..." >&2
      sleep 5
    done
    echo "SSH did not become ready for ${host}" >&2
    exit 1
    ;;

  sync-repo)
    load_resource_state
    require_env CDF_BENCH_SSH_KEY
    host="$(target_host)"
    repo_revision="$(git -C "${repo_root}" rev-parse HEAD)"
    if [[ -n "$(git -C "${repo_root}" status --porcelain)" ]]; then
      repo_dirty="dirty"
      repo_revision_label="${repo_revision}+dirty"
    else
      repo_dirty="clean"
      repo_revision_label="${repo_revision}"
    fi
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
    run_cmd ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}" \
      "cat > '${remote_repo}/.cdf-bench-revision.env' <<'EOF'
repo_revision=${repo_revision}
repo_dirty=${repo_dirty}
repo_revision_label=${repo_revision_label}
EOF"
    ;;

  bootstrap)
    load_resource_state
    require_env CDF_BENCH_SSH_KEY
    host="$(target_host)"
    run_cmd ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}" \
      "set -euo pipefail; sudo dnf install -y git rsync gcc gcc-c++ clang llvm cmake make perl pkgconf-pkg-config openssl-devel sqlite-devel python3 python3-devel tar gzip xz zstd; if ! command -v rustup >/dev/null 2>&1; then /usr/bin/curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain '${rust_toolchain}' --profile minimal; fi; ${remote_prelude}; rustup toolchain install '${rust_toolchain}' --profile minimal; rustup default '${rust_toolchain}'; rustc --version; cargo --version"
    ;;

  sync-workspace)
    load_resource_state
    require_env CDF_BENCH_WORKSPACE
    require_env CDF_BENCH_SSH_KEY
    host="$(target_host)"
    run_cmd ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}" "mkdir -p '${remote_workspace}'"
    workspace_mode="${CDF_BENCH_WORKSPACE_SYNC_MODE:-minimal}"
    case "${workspace_mode}" in
      minimal)
        run_cmd rsync -az --delete --delete-excluded \
          --include='/cdf.toml' \
          --include='/cdf.lock' \
          --include='/README.md' \
          --include='/resources/***' \
          --include='/data/***' \
          --include='/.cdf/' \
          --include='/.cdf/state.db' \
          --include='/.cdf/schemas/***' \
          --include='/.cdf/cache/' \
          --include='/.cdf/cache/schema-observations/***' \
          --exclude='*' \
          -e "ssh -i ${CDF_BENCH_SSH_KEY} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new" \
          "${CDF_BENCH_WORKSPACE%/}/" "${ssh_user}@${host}:${remote_workspace}/"
        ;;
      full)
        run_cmd rsync -az --delete --delete-excluded \
          --filter=':- .gitignore' \
          --exclude='.git/' \
          --exclude='target/' \
          --exclude='.env' \
          --exclude='.env.*' \
          --exclude='secrets/' \
          --exclude='.cdf/secrets/' \
          --exclude='*.duckdb' \
          --exclude='*.duckdb.wal' \
          --exclude='.cdf/dev.duckdb' \
          --exclude='.cdf/dev.duckdb.wal' \
          --exclude='.cdf/packages/' \
          --exclude='.cdf/tmp/' \
          --exclude='.cdf/spool/' \
          -e "ssh -i ${CDF_BENCH_SSH_KEY} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new" \
          "${CDF_BENCH_WORKSPACE%/}/" "${ssh_user}@${host}:${remote_workspace}/"
        ;;
      *)
        echo "CDF_BENCH_WORKSPACE_SYNC_MODE must be minimal or full" >&2
        exit 2
        ;;
    esac
    ;;

  build)
    load_resource_state
    require_env CDF_BENCH_SSH_KEY
    host="$(target_host)"
    run_cmd ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}" \
      "set -euo pipefail; ${remote_prelude}; cd '${remote_repo}'; CARGO_BUILD_JOBS=\$(nproc) cargo build -p cdf-cli --bin cdf --release --locked -j \$(nproc); CARGO_BUILD_JOBS=\$(nproc) cargo build -p cdf-benchmarks --bin cdf-p3-lab --release --locked -j \$(nproc); ls -lh target/release/cdf target/release/cdf-p3-lab; rustc --version; cargo --version"
    ;;

  verify)
    load_resource_state
    require_env CDF_BENCH_SSH_KEY
    host="$(target_host)"
    run_cmd ssh -i "${CDF_BENCH_SSH_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "${ssh_user}@${host}" \
      "set -euo pipefail; ${remote_prelude}; cd '${remote_repo}'; target/release/cdf --version; target/release/cdf-p3-lab host"
    ;;

  cdf)
    if [[ "${1:-}" == "--" ]]; then
      shift
    fi
    load_resource_state
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
    load_resource_state
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
    load_resource_state
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
