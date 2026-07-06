#!/usr/bin/env bash
set -euo pipefail

db="target/quality/codeql-db-rust"
config="target/quality/codeql-rust-config.yml"
report="target/quality/reports/codeql-rust-current.sarif"
cargo_target="target/codeql-cargo-target"
metadata="$db/codeql-database.yml"
fingerprint="$db/firn-codeql-inputs.sha256"

input_fingerprint() {
  find Cargo.toml Cargo.lock crates -type f \( -name '*.rs' -o -name 'Cargo.toml' -o -name 'Cargo.lock' \) -print \
    | LC_ALL=C sort \
    | while IFS= read -r file; do
      shasum -a 256 "$file"
    done \
    | shasum -a 256 \
    | awk '{print $1}'
}

mkdir -p "$(dirname "$config")" "$(dirname "$report")"
cat >"$config" <<'YAML'
paths-ignore:
  - target/**
  - reports/**
YAML

current_fingerprint="$(input_fingerprint)"
reason=""
if [[ ! -f "$metadata" ]]; then
  reason="missing database metadata"
else
  db_cli_version="$(sed -n 's/^  cliVersion: //p' "$metadata" | head -n 1)"
  current_cli_version="$(codeql --version | sed -n 's/^CodeQL command-line toolchain release //p' | head -n 1)"
  current_cli_version="${current_cli_version%.}"
  if [[ "$db_cli_version" != "$current_cli_version" ]]; then
    reason="CodeQL version changed: ${db_cli_version:-unknown} -> ${current_cli_version:-unknown}"
  elif [[ ! -f "$fingerprint" ]]; then
    reason="missing CodeQL input fingerprint"
  elif [[ "$(cat "$fingerprint")" != "$current_fingerprint" ]]; then
    reason="Rust source, manifest, or lockfile content changed"
  fi
fi

if [[ -n "$reason" ]]; then
  echo "Refreshing CodeQL Rust database at $db ($reason)."
  codeql database create "$db" \
    --language=rust \
    --source-root . \
    --overwrite \
    --command "env CARGO_TARGET_DIR=$cargo_target cargo check --workspace --all-targets --locked" \
    --codescanning-config "$config"
  printf '%s\n' "$current_fingerprint" >"$fingerprint"
else
  echo "Reusing fresh CodeQL Rust database at $db."
fi

codeql database analyze "$db" \
  codeql/rust-queries \
  --format=sarif-latest \
  --output="$report" \
  --rerun

if command -v jq >/dev/null 2>&1; then
  jq -r '
    .runs[0].properties.metricResults
    | map(select(.ruleId == "rust/summary/reduced-summary-statistics" or .ruleId == "rust/summary/summary-statistics"))
    | map(select(.message.text as $text
        | $text == "Extraction errors"
        or $text == "Extraction warnings"
        or $text == "Files extracted - total"
        or $text == "Files extracted - with errors"
        or $text == "Files extracted - without errors"
        or $text == "Files extracted - without errors %"
        or $text == "Macro calls - resolved"
        or $text == "Macro calls - total"
        or $text == "Macro calls - unresolved"))
    | unique_by(.message.text)
    | .[]
    | "\(.message.text): \(.value)"
  ' "$report"
fi
