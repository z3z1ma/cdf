#!/usr/bin/env bash
set -euo pipefail

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'USAGE'
Run the product-shaped constant-memory Parquet stress law.

Usage:
  run-constant-memory-stress.sh ROOT FILE_COUNT LOGICAL_BYTES_PER_FILE [MEMORY_BUDGET]

Environment:
  CDF_STRESS_CDF        cdf executable; default: target/release/cdf
  CDF_STRESS_LAB        cdf-p3-lab executable; default: target/release/cdf-p3-lab
  CDF_STRESS_BATCH_ROWS generator batch rows; default: 65536
  CDF_STRESS_PAYLOAD_BYTES generated payload bytes per row; default: 192

ROOT must be absent or empty. Generator setup is outside the timed CDF run. The
script preserves generator, run, process-RSS, package-verification, and summary
JSON evidence below ROOT.
USAGE
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

root="${1:-}"
file_count="${2:-}"
logical_bytes_per_file="${3:-}"
memory_budget="${4:-2GiB}"
[[ -n "$root" && -n "$file_count" && -n "$logical_bytes_per_file" ]] || {
  usage
  die 'ROOT, FILE_COUNT, and LOGICAL_BYTES_PER_FILE are required'
}
[[ "$file_count" =~ ^[1-9][0-9]*$ ]] || die 'FILE_COUNT must be a positive integer'
[[ "$logical_bytes_per_file" =~ ^[1-9][0-9]*$ ]] || die 'LOGICAL_BYTES_PER_FILE must be a positive integer'

cdf="${CDF_STRESS_CDF:-${repo_root}/target/release/cdf}"
lab="${CDF_STRESS_LAB:-${repo_root}/target/release/cdf-p3-lab}"
batch_rows="${CDF_STRESS_BATCH_ROWS:-65536}"
payload_bytes="${CDF_STRESS_PAYLOAD_BYTES:-192}"
[[ -x "$cdf" ]] || die "cdf executable is absent: $cdf"
[[ -x "$lab" ]] || die "cdf-p3-lab executable is absent: $lab"

mkdir -p "$root"
root="$(cd "$root" && pwd)"
if find "$root" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
  die "stress root must be empty: $root"
fi
mkdir -p "$root/data" "$root/resources"

cat >"$root/cdf.toml" <<'TOML'
[project]
name = "constant_memory_stress"
default_environment = "stress"
normalizer = "namecase-v1"

[environments.stress]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "parquet://.cdf/destination"

[resources."stress.rows"]
source = "resources/stress.toml"
TOML

cat >"$root/resources/stress.toml" <<'TOML'
[source.stress]
kind = "files"
root = "data"

[resource.rows]
source = "stress"
glob = "part-*.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "row_id", type = "int64", nullable = false },
  { name = "payload", type = "utf8", nullable = false },
] }
TOML

if [[ "$(uname -s)" == "Darwin" ]]; then
  /usr/bin/time -lp "$lab" generate-constant-memory-parquet \
    "$root/data" \
    "$file_count" \
    "$logical_bytes_per_file" \
    "$batch_rows" \
    "$payload_bytes" >"$root/generator.json" 2>"$root/generator-time.txt"
else
  /usr/bin/time -v -o "$root/generator-time.txt" \
    "$lab" generate-constant-memory-parquet \
    "$root/data" \
    "$file_count" \
    "$logical_bytes_per_file" \
    "$batch_rows" \
    "$payload_bytes" >"$root/generator.json"
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  (
    cd "$root"
    /usr/bin/time -lp "$cdf" run stress.rows \
      --memory-budget "$memory_budget" \
      --progress never \
      --color never \
      --unicode never \
      --json >run.json
  ) 2>"$root/process-time.txt"
else
  (
    cd "$root"
    /usr/bin/time -v -o process-time.txt "$cdf" run stress.rows \
      --memory-budget "$memory_budget" \
      --progress never \
      --color never \
      --unicode never \
      --json >run.json
  )
fi

package_dir="$(
  python3 - "$root/run.json" <<'PY'
import json
import pathlib
import sys

result = json.loads(pathlib.Path(sys.argv[1]).read_text())
if result.get("ok") is not True:
    raise SystemExit("cdf stress run did not succeed")
print(result["result"]["package_dir"])
PY
)"
(
  cd "$root"
  "$cdf" package verify "$package_dir" --json >package-verify.json
)

python3 - "$root" "$memory_budget" <<'PY'
import json
import pathlib
import platform
import re
import sys

root = pathlib.Path(sys.argv[1])
memory_budget = sys.argv[2]
generator = json.loads((root / "generator.json").read_text())
run_envelope = json.loads((root / "run.json").read_text())
verification = json.loads((root / "package-verify.json").read_text())
result = run_envelope["result"]
if result["row_count"] != generator["total_rows"]:
    raise SystemExit(
        f"row count mismatch: generated {generator['total_rows']}, committed {result['row_count']}"
    )
if result["receipt"]["counts"]["rows_written"] != generator["total_rows"]:
    raise SystemExit("destination receipt row count does not match generated rows")
if not result["checkpoint"]["committed"]:
    raise SystemExit("stress checkpoint was not committed")
if verification.get("ok") is not True:
    raise SystemExit("stress package verification failed")

def peak_rss(path):
    time_text = path.read_text()
    if platform.system() == "Darwin":
        match = re.search(
            r"^\s*(\d+)\s+maximum resident set size$", time_text, re.MULTILINE
        )
        return int(match.group(1)) if match else None
    match = re.search(
        r"Maximum resident set size \(kbytes\):\s*(\d+)", time_text
    )
    return int(match.group(1)) * 1024 if match else None

peak_rss_bytes = peak_rss(root / "process-time.txt")
generator_peak_rss_bytes = peak_rss(root / "generator-time.txt")
if peak_rss_bytes is None:
    raise SystemExit("process RSS provider did not report a maximum")
if generator_peak_rss_bytes is None:
    raise SystemExit("generator RSS provider did not report a maximum")

managed = result["memory"]["managed"]
resolution = result["memory"]["budget"]["resolution"]
if managed["peak_bytes"] > managed["budget_bytes"]:
    raise SystemExit("managed memory peak exceeded its admitted budget")
if peak_rss_bytes > resolution["process_budget_bytes"]:
    raise SystemExit("process RSS peak exceeded the configured process budget")

destination_bytes = sum(
    path.stat().st_size
    for path in (root / ".cdf" / "destination").rglob("*")
    if path.is_file()
)
summary = {
    "schema_version": 1,
    "generator": {
        **generator,
        "peak_rss_bytes": generator_peak_rss_bytes,
    },
    "run": {
        "elapsed_ms": result["elapsed_ms"],
        "row_count": result["row_count"],
        "segment_count": result["segment_count"],
        "package_hash": result["package_hash"],
        "checkpoint_id": result["checkpoint_id"],
        "receipt_id": result["receipt_id"],
        "managed_peak_bytes": managed["peak_bytes"],
        "managed_budget_bytes": managed["budget_bytes"],
        "spill_bytes": managed["spill_bytes"],
        "process_budget_bytes": resolution["process_budget_bytes"],
        "peak_rss_bytes": peak_rss_bytes,
        "destination_bytes": destination_bytes,
    },
    "requested_memory_budget": memory_budget,
    "timed_region": {
        "includes": [
            "metadata inventory",
            "source decode",
            "validation and normalization",
            "canonical package persistence and hashing",
            "Parquet destination commit",
            "receipt verification",
            "checkpoint commit",
        ],
        "excludes": ["fixture generation", "project setup", "summary serialization"],
    },
}
(root / "summary.json").write_text(
    json.dumps(summary, sort_keys=True, indent=2) + "\n"
)
print(json.dumps(summary, sort_keys=True))
PY
