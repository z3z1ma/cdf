#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! cargo nextest --version >/dev/null 2>&1; then
  echo "error: cargo-nextest is required for the product smoke matrix" >&2
  exit 2
fi

cargo nextest run -p cdf-cli --locked -E \
  'test(p2_s1_add_http_parquet_pins_and_runs_with_zero_typed_fields) |
   test(pinned_multi_file_parquet_preview_attests_unopened_observed_partitions) |
   test(run_local_parquet_discover_autopins_and_commits_pinned_schema) |
   test(package_verify_uses_lower_package_reader) |
   test(replay_package_duckdb_replays_from_artifacts_without_source_contact)'

cargo nextest run -p cdf-project --locked -E \
  'test(file_manifest_append_run_skips_unchanged_files_and_loads_only_changes) |
   test(general_project_run_commits_file_resource_to_parquet_with_ledger_order)'

cargo nextest run -p cdf-conformance --locked -E \
  'test(p2_preview_run_parity_law_covers_supported_archetypes)'

cargo nextest run -p cdf-source-iceberg --locked -E \
  'test(projection_passes_only_top_level_field_ids_to_the_arrow_reader) |
   test(task_authority_preserves_declared_projection_order) |
   test(execution_schema_is_the_compiled_reader_projection_in_declared_order)'
