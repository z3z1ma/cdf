use cdf_dest_duckdb::DuckDbDestination;
use cdf_kernel::{
    CheckpointStore, CursorValue, PipelineId, ResourceId, Result, ScopeKey, SourcePosition,
    WriteDisposition, source_name,
};
use cdf_package::PackageReader;
use cdf_state_sqlite::SqliteCheckpointStore;
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    ffi::OsString,
    fs,
    io::{ErrorKind, Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use super::{
    MatrixDestination, MatrixDisposition, RunMatrixCell, SourceArchetype, core,
    destinations::ConformanceEnvironment, file_fixture, plan_json, source_catalog,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CoverageStatus {
    Covered,
    Excluded,
}

#[derive(Clone, Copy, Debug)]
struct P2Scenario {
    id: &'static str,
    title: &'static str,
    status: CoverageStatus,
    rationale: &'static str,
    tests: &'static [&'static str],
    tickets: &'static [&'static str],
}

#[derive(Clone, Copy, Debug)]
struct P2FrictionRow {
    id: u8,
    closed_tests: &'static [&'static str],
    open_tickets: &'static [&'static str],
}

#[derive(Debug, PartialEq, Eq)]
struct PreviewFingerprint {
    source: SourceArchetype,
    row_count: u64,
    field_names: Vec<String>,
    partition_count: usize,
}

fn planned_partitions(
    resource: &dyn cdf_kernel::QueryableResource,
    scan: &cdf_kernel::ScanPlan,
) -> Result<Vec<cdf_kernel::PartitionPlan>> {
    if let Some(partitions) = scan.inline_partitions() {
        return Ok(partitions.to_vec());
    }
    let reference = scan.external_task_set().ok_or_else(|| {
        cdf_kernel::CdfError::internal("scan omitted canonical partition authority")
    })?;
    let mut reader = resource.planned_partition_reader(reference)?;
    let mut partitions = Vec::with_capacity(usize::try_from(reference.task_count).unwrap_or(0));
    for ordinal in 0..reference.task_count {
        partitions.push(
            reader
                .next_partition(ordinal)?
                .ok_or_else(|| {
                    cdf_kernel::CdfError::data(format!(
                        "external partition authority ended before ordinal {ordinal}"
                    ))
                })?
                .plan()
                .clone(),
        );
    }
    Ok(partitions)
}

const P2_SCENARIOS: &[P2Scenario] = &[
    P2Scenario {
        id: "S1",
        title: "Public HTTPS Parquet single file, zero typed schema fields, through cdf add and run",
        status: CoverageStatus::Covered,
        rationale: "deterministic HTTP Parquet conformance runs cdf add, pins the ranged-footer schema, plans, and commits through the ordinary package/receipt/checkpoint path with zero typed fields; public TLC remains separately recorded live evidence",
        tests: &[
            "crates/cdf-cli/src/tests.rs::p2_s1_add_http_parquet_pins_and_runs_with_zero_typed_fields",
        ],
        tickets: &[],
    },
    P2Scenario {
        id: "S2",
        title: "Public HTTPS Parquet monthly glob with default FileManifest incrementality and no-change no-op rerun",
        status: CoverageStatus::Covered,
        rationale: "deterministic production HTTP conformance expands the canonical year-month glob, skips typed 404 absences, previews the exact partition set, loads present months, performs a no-change no-op, and loads only a newly present month",
        tests: &[
            "crates/cdf-cli/src/tests.rs::p2_s2_http_month_glob_is_incremental_and_no_change_is_a_noop",
        ],
        tickets: &[],
    },
    P2Scenario {
        id: "S3",
        title: "S3 compressed NDJSON recursive glob with transparent gzip and drift governed by contract policy",
        status: CoverageStatus::Covered,
        rationale: "the object-store fixture recursively resolves, bounded-discovers, pins, previews, streams gzip NDJSON, preserves remote FileManifest identity, and executes 10,000 rows; recorded HTTP fixtures additionally prove bounded transform/decode backpressure, cancellation before download completion, and jobs-invariant multi-file packages; drift quarantine remains covered by the shared file-contract conformance",
        tests: &[
            "crates/cdf-project/src/tests.rs::object_store_gzip_ndjson_discovers_pins_and_executes_through_one_transport",
            "crates/cdf-project/src/tests.rs::http_gzip_ndjson_backpressures_and_cancels_before_download_completion",
            "crates/cdf-project/src/tests.rs::recorded_http_multifile_packages_are_jobs_invariant",
            "crates/cdf-conformance/src/live_run/drift_quarantine/mod.rs::drift_quarantine_duckdb_conformance_asserts_unsupported_mirror_exclusion",
        ],
        tickets: &[],
    },
    P2Scenario {
        id: "S4",
        title: "Postgres table discovery with optional schema block and cursor candidates",
        status: CoverageStatus::Covered,
        rationale: "standalone local-Postgres conformance runs cdf add from a direct table DSN, persists only a private secret reference, pins catalog discovery, reports cursor suggestions without selecting one, then plans, previews, and runs after explicit cursor selection",
        tests: &[
            "crates/cdf-cli/src/tests.rs::p2_s4_postgres_add_pins_private_secret_and_runs_discovered_table",
        ],
        tickets: &[],
    },
    P2Scenario {
        id: "S5",
        title: "REST API in discover mode with a recorded sample page and pinned snapshot",
        status: CoverageStatus::Covered,
        rationale: "standalone deterministic conformance pins one recorded REST sample page, previews, packages, verifies the receipt, and commits the pinned schema/cursor identity",
        tests: &[
            "crates/cdf-conformance/src/run_matrix/data_onramp.rs::p2_s5_rest_discover_pin_preview_run_package_checkpoint_conformance",
        ],
        tickets: &[],
    },
    P2Scenario {
        id: "S6",
        title: "Drift quarantines with accepted stream unblocked and file/column remediation rendered",
        status: CoverageStatus::Covered,
        rationale: "deterministic governed and financial fixtures complete with incompatible files quarantined, preserve accepted-stream and manifest advancement, and now expose typed file/field/type/rule/remediation verdicts in JSON and P1 human run output",
        tests: &[
            "crates/cdf-conformance/src/live_run/drift_quarantine/mod.rs::drift_quarantine_duckdb_conformance_asserts_unsupported_mirror_exclusion",
            "crates/cdf-conformance/src/live_run/drift_quarantine/mod.rs::drift_quarantine_postgres_conformance_asserts_supported_mirror",
            "crates/cdf-cli/src/tests.rs::sampled_discovery_renders_every_cli_path_and_routes_unseen_drift_to_package_quarantine",
            "crates/cdf-cli/src/tests.rs::financial_freeze_quarantines_deviating_file_and_commits_mixed_processed_manifest",
            "crates/cdf-cli/src/tests.rs::governed_evolve_quarantines_incompatible_file_with_exact_arrow_field_evidence",
        ],
        tickets: &[],
    },
    P2Scenario {
        id: "S7",
        title: "Append requires no key; merge without key fails with precise remediation",
        status: CoverageStatus::Covered,
        rationale: "standalone deterministic conformance exercises keyless append through the operator path and merge-without-key failure before source contact or project mutation",
        tests: &[
            "crates/cdf-conformance/src/run_matrix/data_onramp.rs::p2_s7_keyless_append_and_precontact_merge_failure_conformance",
        ],
        tickets: &[],
    },
    P2Scenario {
        id: "S8",
        title: "Preview/run parity per source archetype",
        status: CoverageStatus::Covered,
        rationale: "the shared preview engine covers local multi-file, REST, Postgres, dated HTTP Parquet, and recursive object-store gzip NDJSON through the same partition, discovery, reconciliation, normalization, and bounded payload paths used by run",
        tests: &[
            "crates/cdf-conformance/src/run_matrix/data_onramp.rs::p2_preview_run_parity_law_covers_supported_archetypes",
            "crates/cdf-conformance/src/run_matrix/data_onramp.rs::p2_s8_multifile_preview_traverses_the_same_planned_partitions_as_run",
            "crates/cdf-cli/src/tests.rs::pinned_multi_file_parquet_keeps_fixed_schema_and_admits_new_physical_schemas_in_stream",
            "crates/cdf-cli/src/tests.rs::sampled_discovery_renders_every_cli_path_and_routes_unseen_drift_to_package_quarantine",
            "crates/cdf-cli/src/tests.rs::sampled_pin_captures_unseen_field_then_fresh_discovery_promotes_without_source_replay",
            "crates/cdf-project/src/discovery_manifest.rs::stratified_hash_selector_large_set_is_executor_budget_independent",
            "crates/cdf-project/src/tests.rs::sampled_probe_budget_failure_does_not_substitute_an_unselected_candidate",
            "crates/cdf-cli/src/tests.rs::p2_s2_http_month_glob_is_incremental_and_no_change_is_a_noop",
            "crates/cdf-project/src/tests.rs::object_store_gzip_ndjson_discovers_pins_and_executes_through_one_transport",
        ],
        tickets: &[],
    },
];

const P2_EXCLUSIONS: &[P2Scenario] = &[P2Scenario {
    id: "live-public-network",
    title: "Live public-network S1/S2 smoke evidence in ordinary focused conformance",
    status: CoverageStatus::Excluded,
    rationale: "ordinary conformance uses deterministic fixtures; public-network terminal-session evidence is required before final P2 closure but excluded from this matrix foundation",
    tests: &[],
    tickets: &[],
}];

const P2_FRICTIONS: &[P2FrictionRow] = &[
    P2FrictionRow {
        id: 1,
        closed_tests: &[
            "crates/cdf-cli/src/tests.rs::schema_discover_local_parquet_reports_schema_without_project_writes",
            "crates/cdf-cli/src/tests.rs::run_local_parquet_discover_autopins_and_commits_pinned_schema",
            "crates/cdf-project/src/tests.rs::http_parquet_schema_discovery_uses_bounded_ranges_without_artifacts",
            "crates/cdf-project/src/tests.rs::http_parquet_auto_pin_plan_preview_and_run_use_file_runtime",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 2,
        closed_tests: &[
            "crates/cdf-cli/src/tests.rs::schema_discover_local_parquet_reports_schema_without_project_writes",
            "crates/cdf-cli/src/tests.rs::schema_discover_rest_reports_sample_schema_without_project_writes_or_secret_leak",
            "crates/cdf-cli/src/tests.rs::schema_discover_postgres_catalog_uses_project_secret_without_writes_or_secret_leak",
            "crates/cdf-cli/src/tests.rs::schema_pin_show_and_diff_local_parquet_snapshot_with_lockfile_reference",
            "crates/cdf-cli/src/tests.rs::add_local_parquet_pins_schema_and_writes_resource_config",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 3,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::arrow_type_vocabulary_covers_widths_decimal_temporal_binary_and_nested_types",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 4,
        closed_tests: &[
            "crates/cdf-contract/src/tests.rs::schema_reconciliation_records_lossless_widenings_and_physical_type",
            "crates/cdf-contract/src/tests.rs::schema_coercion_plan_from_reconciled_schema_records_widened_and_preserved_fields",
            "crates/cdf-contract/src/tests.rs::shared_coercion_materializer_widens_projects_and_materializes_missing_nulls",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 5,
        closed_tests: &[
            "crates/cdf-contract/src/tests.rs::schema_reconciliation_preserves_constraint_names_and_classifies_extra_fields",
            "crates/cdf-contract/src/tests.rs::schema_reconciliation_rejects_lossy_casts_until_policy_allows_them",
            "crates/cdf-contract/src/tests.rs::reconciled_schema_metadata_preserves_extra_field_decisions_for_package_evidence",
            "crates/cdf-source-files/src/runtime.rs::tests::local_parquet_uses_registered_native_driver_as_bounded_stream",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 6,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::declared_schema_is_normalized_and_preserves_source_identity",
            "crates/cdf-contract/src/tests.rs::destination_identifier_policy_preserves_postgres_max_length",
            "crates/cdf-cli/src/tests.rs::duckdb_destination_policy_normalizes_plan_preview_package_and_commit",
            "crates/cdf-cli/src/tests.rs::destination_normalization_collision_fails_before_writes",
            "crates/cdf-project/src/runtime_tests.rs::postgres_destination_policy_truncates_package_and_committed_column_identically",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 7,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::declared_schema_is_normalized_and_preserves_source_identity",
            "crates/cdf-cli/src/tests.rs::duckdb_destination_policy_normalizes_plan_preview_package_and_commit",
            "crates/cdf-project/src/runtime_tests.rs::postgres_destination_policy_truncates_package_and_committed_column_identically",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 8,
        closed_tests: &[
            "crates/cdf-source-files/src/runtime.rs::tests::object_store_recursive_glob_resolves_stable_multi_file_partitions",
            "crates/cdf-project/src/runtime_tests.rs::general_project_run_commits_multi_file_resource_manifest_checkpoint",
            "crates/cdf-project/src/runtime_tests.rs::file_manifest_append_run_skips_unchanged_files_and_loads_only_changes",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 9,
        closed_tests: &[
            "crates/cdf-conformance/src/run_matrix/data_onramp.rs::p2_preview_run_parity_law_covers_supported_archetypes",
            "crates/cdf-conformance/src/run_matrix/data_onramp.rs::p2_s8_multifile_preview_traverses_the_same_planned_partitions_as_run",
            "crates/cdf-cli/src/tests.rs::pinned_multi_file_parquet_keeps_fixed_schema_and_admits_new_physical_schemas_in_stream",
            "crates/cdf-cli/src/tests.rs::sampled_discovery_renders_every_cli_path_and_routes_unseen_drift_to_package_quarantine",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 10,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::registry_compilation_produces_one_compiled_source_plan_and_canonical_id",
            "crates/cdf-project/src/tests.rs::declarative_resource_mapping_pattern_must_match_compiled_id",
            "crates/cdf-cli/src/tests.rs::resource_mapping_pattern_mismatch_reports_validate_and_plan_commands",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 11,
        closed_tests: &[
            "crates/cdf-cli/src/tests.rs::resource_not_compiled_error_names_compiled_ids_origins_and_fix",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 12,
        closed_tests: &[
            "crates/cdf-cli/src/scan_command.rs::tests::plan_error_wording_uses_plan_command_name",
            "crates/cdf-cli/src/tests.rs::resource_mapping_pattern_mismatch_reports_validate_and_plan_commands",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 13,
        closed_tests: &[
            "crates/cdf-cli/src/tests.rs::resource_not_compiled_error_names_compiled_ids_origins_and_fix",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 14,
        closed_tests: &[
            "crates/cdf-cli/src/tests.rs::validate_deep_reports_source_front_end_checks_without_writes",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 15,
        closed_tests: &[
            "crates/cdf-object-access/src/transport.rs::tests::file_transport_http_metadata_uses_headers_only_client",
            "crates/cdf-object-access/src/transport.rs::tests::file_transport_http_metadata_falls_back_from_head_errors_and_keeps_access_ephemeral",
            "crates/cdf-project/src/tests.rs::http_parquet_schema_discovery_uses_bounded_ranges_without_artifacts",
            "crates/cdf-project/src/tests.rs::http_parquet_auto_pin_plan_preview_and_run_use_file_runtime",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 16,
        closed_tests: &[
            "crates/cdf-source-files/src/runtime.rs::tests::object_store_gzip_ndjson_streams_without_spill_and_preserves_remote_position",
            "crates/cdf-project/src/tests.rs::http_gzip_ndjson_backpressures_and_cancels_before_download_completion",
            "crates/cdf-transform-gzip/src/lib.rs::tests::streams_concatenated_members_across_single_byte_input_chunks",
            "crates/cdf-transform-zstd/src/lib.rs::tests::streams_concatenated_frames_across_single_byte_input_chunks",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 17,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::append_is_keyless_by_default_and_merge_names_both_fixes",
            "crates/cdf-declarative/src/tests.rs::merge_and_exact_row_dedup_compile_only_for_their_valid_dispositions",
            "crates/cdf-project/src/tests.rs::local_project_scaffold_writes_valid_project_without_runtime_artifacts",
            "crates/cdf-cli/src/tests.rs::keyless_append_file_validate_plan_preview_run_has_no_key_nudge",
            "crates/cdf-cli/src/tests.rs::keyless_append_rest_validate_plan_preview_run_has_no_key_nudge",
            "crates/cdf-cli/src/tests.rs::merge_without_key_fails_all_entry_commands_before_contact_or_writes",
            "crates/cdf-conformance/src/run_matrix/data_onramp.rs::p2_s7_keyless_append_and_precontact_merge_failure_conformance",
        ],
        open_tickets: &[],
    },
    P2FrictionRow {
        id: 18,
        closed_tests: &[
            "crates/cdf-source-files/src/runtime.rs::tests::local_parquet_uses_registered_native_driver_as_bounded_stream",
            "crates/cdf-contract/src/tests.rs::schema_reconciliation_records_lossless_widenings_and_physical_type",
            "crates/cdf-cli/src/tests.rs::run_local_parquet_discover_autopins_and_commits_pinned_schema",
            "crates/cdf-project/src/tests.rs::http_parquet_auto_pin_plan_preview_and_run_use_file_runtime",
            "crates/cdf-cli/src/tests.rs::p2_s1_add_http_parquet_pins_and_runs_with_zero_typed_fields",
        ],
        open_tickets: &[],
    },
];

#[test]
fn p2_data_onramp_scenario_matrix_records_s1_through_s8() {
    assert_eq!(
        P2_SCENARIOS
            .iter()
            .map(|scenario| scenario.id)
            .collect::<Vec<_>>(),
        vec!["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8"]
    );

    for scenario in P2_SCENARIOS {
        assert!(!scenario.title.is_empty(), "{} title", scenario.id);
        assert!(!scenario.rationale.is_empty(), "{} rationale", scenario.id);
        match scenario.status {
            CoverageStatus::Covered => {
                assert!(
                    !scenario.tests.is_empty(),
                    "{} covered scenarios must name tests",
                    scenario.id
                );
                assert!(
                    scenario.tickets.is_empty(),
                    "{} covered scenarios must not carry active-ticket blockers",
                    scenario.id
                );
            }
            CoverageStatus::Excluded => {
                assert!(
                    !scenario.rationale.is_empty(),
                    "{} exclusions must explain the boundary",
                    scenario.id
                );
            }
        }
    }
    for exclusion in P2_EXCLUSIONS {
        assert_eq!(exclusion.status, CoverageStatus::Excluded);
        assert!(!exclusion.title.is_empty(), "{} title", exclusion.id);
        assert!(
            !exclusion.rationale.is_empty(),
            "{} rationale",
            exclusion.id
        );
        assert!(
            exclusion.tests.is_empty(),
            "{} excluded tests",
            exclusion.id
        );
    }

    assert_eq!(scenario("S1").status, CoverageStatus::Covered);
    assert_eq!(scenario("S2").status, CoverageStatus::Covered);
    assert_eq!(scenario("S3").status, CoverageStatus::Covered);
    assert_eq!(scenario("S4").status, CoverageStatus::Covered);
    assert_eq!(scenario("S5").status, CoverageStatus::Covered);
    assert_eq!(scenario("S6").status, CoverageStatus::Covered);
    assert_eq!(scenario("S7").status, CoverageStatus::Covered);
    assert_eq!(scenario("S8").status, CoverageStatus::Covered);
}

#[test]
fn p2_friction_registry_maps_closed_slices_to_tests_and_open_rows_to_tickets() {
    assert_eq!(
        P2_FRICTIONS.iter().map(|row| row.id).collect::<Vec<_>>(),
        (1..=18).collect::<Vec<_>>()
    );

    for row in P2_FRICTIONS {
        assert!(
            !row.closed_tests.is_empty() || !row.open_tickets.is_empty(),
            "friction {} must have a closed test or active owner",
            row.id
        );
        for test in row.closed_tests {
            assert!(
                test.contains("::"),
                "friction {} closed slice must name a concrete test: {test}",
                row.id
            );
        }
        if !row.open_tickets.is_empty() {
            assert_active_tickets(&format!("friction {}", row.id), row.open_tickets);
        }
    }

    assert!(friction(4).closed_tests.iter().any(|test| {
        test.contains("schema_reconciliation_records_lossless_widenings_and_physical_type")
    }));
    assert!(
        friction(5)
            .closed_tests
            .iter()
            .any(|test| test.contains("schema_reconciliation_preserves_constraint_names"))
    );
    assert!(
        friction(9)
            .closed_tests
            .contains(&"crates/cdf-conformance/src/run_matrix/data_onramp.rs::p2_preview_run_parity_law_covers_supported_archetypes")
    );
    assert!(friction(11).closed_tests.iter().any(|test| {
        test.contains("resource_not_compiled_error_names_compiled_ids_origins_and_fix")
    }));
    assert!(friction(14).closed_tests.iter().any(|test| {
        test.contains("validate_deep_reports_source_front_end_checks_without_writes")
    }));
    assert!(
        friction(16)
            .closed_tests
            .iter()
            .any(|test| test.contains("cdf-transform-gzip"))
    );
    assert!(
        friction(16)
            .closed_tests
            .iter()
            .any(|test| test.contains("cdf-transform-zstd"))
    );
}

#[test]
fn p2_s5_s7_registry_names_standalone_conformance_without_other_promotions() {
    let s5 = scenario("S5");
    assert_eq!(s5.status, CoverageStatus::Covered);
    assert!(s5.tickets.is_empty());
    assert_eq!(
        s5.tests,
        &[
            "crates/cdf-conformance/src/run_matrix/data_onramp.rs::p2_s5_rest_discover_pin_preview_run_package_checkpoint_conformance"
        ]
    );

    let s7 = scenario("S7");
    let standalone = "crates/cdf-conformance/src/run_matrix/data_onramp.rs::p2_s7_keyless_append_and_precontact_merge_failure_conformance";
    assert_eq!(s7.status, CoverageStatus::Covered);
    assert!(s7.tickets.is_empty());
    assert_eq!(s7.tests, &[standalone]);
    assert!(friction(17).closed_tests.contains(&standalone));
    assert!(friction(17).open_tickets.is_empty());
}

#[test]
fn p2_s5_rest_discover_pin_preview_run_package_checkpoint_conformance() {
    const SECRET: &str = "s5-recorded-rest-secret";
    const BODY: &str = r#"{ "items": [
        { "VendorID": 1, "updated_at": 10 },
        { "VendorID": 2, "updated_at": 20 }
    ] }"#;

    let server = RecordedHttpServer::new([BODY, BODY, BODY, BODY]);
    let temp = tempfile::tempdir().unwrap();
    write_s5_project(temp.path(), server.base_url(), SECRET);

    let first_pin = invoke_success_json(temp.path(), &["schema", "pin", "api.items"], Some(SECRET));
    let first = &first_pin["result"];
    assert_eq!(first["status"], "added");
    assert_eq!(
        first["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    assert_eq!(first["snapshot_metadata"]["source_driver"], "rest");
    assert_eq!(first["source_identity"]["driver.sample_pages"], "1");
    assert_eq!(first["source_identity"]["driver.sample_records"], "2");
    assert_eq!(first["writes"]["schema_snapshot"], true);
    assert_eq!(first["writes"]["lockfile"], true);

    let pinned_hash = first["schema_hash"].as_str().unwrap().to_owned();
    let snapshot_path = first["schema_snapshot_path"].as_str().unwrap().to_owned();
    let snapshot_bytes = fs::read(temp.path().join(&snapshot_path)).unwrap();
    let lock_bytes = fs::read(temp.path().join("cdf.lock")).unwrap();
    let snapshot: Value = serde_json::from_slice(&snapshot_bytes).unwrap();
    assert_eq!(snapshot["metadata"]["probe"], "registered-source-discovery");
    let vendor = snapshot["schema"]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|field| field["name"] == "vendor_id")
        .expect("normalized VendorID snapshot field");
    assert_eq!(vendor["metadata"]["cdf:source_name"], "VendorID");

    let second_pin =
        invoke_success_json(temp.path(), &["schema", "pin", "api.items"], Some(SECRET));
    let second = &second_pin["result"];
    assert_eq!(second["status"], "unchanged");
    assert_eq!(second["schema_hash"], pinned_hash);
    assert_eq!(second["schema_snapshot_path"], snapshot_path);
    assert_eq!(second["writes"]["schema_snapshot"], false);
    assert_eq!(second["writes"]["lockfile"], false);
    assert_eq!(
        fs::read(temp.path().join(&snapshot_path)).unwrap(),
        snapshot_bytes
    );
    assert_eq!(fs::read(temp.path().join("cdf.lock")).unwrap(), lock_bytes);

    let before_preview = project_tree_snapshot(temp.path());
    let preview = invoke_success_json(temp.path(), &["preview", "api.items"], Some(SECRET));
    assert_eq!(preview["result"]["resource"], "api.items");
    assert_eq!(preview["result"]["partition"], "rest");
    assert_eq!(preview["result"]["row_count"], 2);
    assert_eq!(project_tree_snapshot(temp.path()), before_preview);

    let run = invoke_success_json(temp.path(), &["run", "api.items"], Some(SECRET));
    let report = &run["result"];
    assert_eq!(report["resource_id"], "api.items");
    assert_eq!(report["schema_hash"], pinned_hash);
    assert_eq!(report["schema_snapshot"]["outcome"], "unchanged");
    assert_eq!(report["row_count"], 2);
    assert_eq!(report["checkpoint"]["status"], "committed");
    let package_id = report["package_id"].as_str().unwrap();
    let checkpoint_id = report["checkpoint_id"].as_str().unwrap();

    let package_dir = temp.path().join(".cdf/packages").join(package_id);
    let reader = PackageReader::open(&package_dir).unwrap();
    reader.verify().unwrap();
    let mut receipts = Vec::new();
    reader
        .for_each_receipt(&mut |receipt| {
            receipts.push(receipt);
            Ok(())
        })
        .unwrap();
    assert_eq!(receipts.len(), 1);
    let receipt = &receipts[0];
    assert_eq!(receipt.schema_hash.as_str(), pinned_hash);
    assert_eq!(receipt.disposition, WriteDisposition::Append);
    assert_eq!(receipt.counts.rows_written, 2);

    let destination = DuckDbDestination::new(temp.path().join(".cdf/s5.duckdb")).unwrap();
    assert!(destination.verify_receipt(receipt).unwrap().verified);

    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap(),
    );
    let mut segments = reader
        .verified_segment_stream(memory, 64 * 1024 * 1024)
        .unwrap();
    let segment = segments.next().unwrap().unwrap();
    let output_schema = segment.batches[0].schema();
    let vendor = output_schema.field_with_name("vendor_id").unwrap();
    assert_eq!(source_name(vendor), Some("VendorID"));

    let store = SqliteCheckpointStore::open(temp.path().join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("api.items").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("S5 committed checkpoint head");
    assert_eq!(head.delta.checkpoint_id.as_str(), checkpoint_id);
    assert_eq!(head.delta.schema_hash.as_str(), pinned_hash);
    assert!(receipt.covers_state_delta(&head.delta));
    let SourcePosition::Cursor(cursor) = &head.delta.output_position else {
        panic!("S5 checkpoint must carry the declared REST cursor");
    };
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));

    assert_generated_artifacts_do_not_contain(temp.path(), SECRET);
    let requests = server.requests().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests.iter().all(|request| {
        request.contains("GET /items HTTP/1.1")
            && request.contains(&format!("authorization: Bearer {SECRET}"))
    }));
}

#[test]
fn p2_s7_keyless_append_and_precontact_merge_failure_conformance() {
    const MISSING_SECRET_SENTINEL: &str = "missing-merge-secret-must-not-resolve";

    let append = tempfile::tempdir().unwrap();
    write_s7_append_project(append.path());
    let validate = invoke_cli(append.path(), &["validate"]);
    assert_success_without_key_nudge(&validate);
    let plan = invoke_cli(append.path(), &["plan", "local.events"]);
    assert_success_without_key_nudge(&plan);
    let plan_json = success_json(&plan);
    assert_eq!(plan_json["result"]["destination"]["disposition"], "append");
    let preview = invoke_cli(append.path(), &["preview", "local.events"]);
    assert_success_without_key_nudge(&preview);
    assert_eq!(success_json(&preview)["result"]["row_count"], 2);
    let run = invoke_cli(append.path(), &["run", "local.events"]);
    assert_success_without_key_nudge(&run);
    let run_json = success_json(&run);
    assert_eq!(run_json["result"]["receipt"]["disposition"], "append");
    assert_eq!(run_json["result"]["row_count"], 2);

    let merge_server = RecordedHttpServer::new([r#"{ "items": [] }"#]);
    let merge = tempfile::tempdir().unwrap();
    write_s7_merge_project(
        merge.path(),
        merge_server.base_url(),
        MISSING_SECRET_SENTINEL,
    );
    assert!(!merge.path().join(MISSING_SECRET_SENTINEL).exists());
    let before = project_tree_snapshot(merge.path());
    let rejected = invoke_cli(merge.path(), &["plan", "api.items"]);
    assert_eq!(rejected.exit_code, 3, "{}", rejected.stderr);
    assert!(rejected.stdout.is_empty());
    assert!(!rejected.stderr.contains(MISSING_SECRET_SENTINEL));
    assert!(
        !rejected
            .stderr
            .contains(&format!("secret://file/{MISSING_SECRET_SENTINEL}"))
    );
    let error: Value = serde_json::from_str(&rejected.stderr).unwrap();
    assert_eq!(error["error"]["code"], "CDF-PROJECT-MERGE-KEY");
    let message = error["error"]["message"].as_str().unwrap();
    assert_eq!(message.matches("missing merge_key").count(), 1);
    assert!(message.contains("cdf plan"));
    assert!(message.contains("resource `api.items`"));
    assert!(message.contains("add `merge_key = [...]`"));
    assert!(message.contains("use `write_disposition = \"append\"`"));
    let steps = error["error"]["remediation"]["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 2);
    assert!(steps[0].as_str().unwrap().contains("merge_key = [...]"));
    assert!(
        steps[1]
            .as_str()
            .unwrap()
            .contains("write_disposition = \"append\"")
    );
    assert_eq!(merge_server.requests().unwrap(), Vec::<String>::new());
    assert_eq!(project_tree_snapshot(merge.path()), before);
}

#[test]
fn p2_registry_named_tests_resolve_to_test_functions() {
    for scenario in P2_SCENARIOS {
        for test in scenario.tests {
            assert_named_test_exists(scenario.id, test);
        }
    }
    for row in P2_FRICTIONS {
        for test in row.closed_tests {
            assert_named_test_exists(&format!("friction {}", row.id), test);
        }
    }
}

#[test]
fn p2_closed_registry_has_no_open_owners_and_rejects_terminal_ones_as_active() {
    assert!(
        P2_FRICTIONS.iter().all(|row| row.open_tickets.is_empty()),
        "closed P2 friction rows must not retain open owners"
    );

    let missing = ticket_owner_status(".10x/tickets/2099-01-01-missing.md").unwrap_err();
    assert!(missing.contains("cannot be read"), "{missing}");

    let terminal =
        ticket_owner_status(".10x/tickets/done/2026-07-09-p2-ws-a7-schema-pin-show-diff-cli.md")
            .unwrap_err();
    assert!(terminal.contains("terminal status `done`"), "{terminal}");

    let not_a_ticket = ticket_owner_status(".10x/specs/data-onramp-conformance.md").unwrap_err();
    assert!(
        not_a_ticket.contains("not a ticket record"),
        "{not_a_ticket}"
    );
}

#[test]
fn p2_preview_run_parity_law_covers_supported_archetypes() {
    let environment = ConformanceEnvironment::start().expect(
        "P2 S8 parity conformance requires Postgres coverage; set TEST_DATABASE_URL or install initdb/pg_ctl",
    );
    let cases = source_catalog::archetypes().into_iter().map(|source| {
        RunMatrixCell::new(
            source,
            MatrixDestination::new("duckdb").unwrap(),
            MatrixDisposition::Append,
        )
    });

    for cell in cases {
        let preview = preview_fingerprint(cell.clone(), &environment).unwrap_or_else(|error| {
            panic!(
                "{} preview failed before parity comparison: {error}",
                cell.source_archetype.as_str()
            )
        });
        let executed = core::execute_cell(cell.clone(), &environment).unwrap_or_else(|error| {
            panic!(
                "{} run failed before parity comparison: {error}",
                cell.source_archetype.as_str()
            )
        });

        assert_eq!(preview.source, cell.source_archetype);
        assert_eq!(
            preview.row_count,
            executed.row_count,
            "{} preview row count must match package-producing run",
            cell.source_archetype.as_str()
        );
        assert_eq!(preview.row_count, core::ROW_COUNT);
        assert_eq!(
            u64::try_from(preview.partition_count).unwrap(),
            core::SEGMENT_COUNT
        );
        assert!(
            preview.field_names.iter().any(|name| name == "id"),
            "{} preview schema should expose the id column consumed by run",
            cell.source_archetype.as_str()
        );
    }
}

#[test]
fn p2_s8_multifile_preview_traverses_the_same_planned_partitions_as_run() {
    let temp = tempfile::tempdir().unwrap();
    let compiled = file_fixture::multi_resource(temp.path(), MatrixDisposition::Append).unwrap();
    let resource = crate::source_fixture::resolve_local_file(&compiled, temp.path()).unwrap();
    let plan = plan_json::file_engine_plan(
        resource.queryable(),
        "p2-s8-multifile-preview-run",
        MatrixDisposition::Append,
        None,
    )
    .unwrap();
    let plan = resource.bind_plan(plan).unwrap();
    let partitions = planned_partitions(resource.queryable(), &plan.scan).unwrap();
    assert_eq!(partitions.len(), 2);
    assert!(
        partitions[0].planned_file().unwrap().unwrap().path
            < partitions[1].planned_file().unwrap().unwrap().path
    );
    let before_preview = project_tree_snapshot(temp.path());

    let preview = futures_executor::block_on(cdf_engine::preview_resource(
        &plan,
        resource.queryable(),
        cdf_engine::EnginePreviewLimits::default(),
    ))
    .unwrap();

    assert_eq!(project_tree_snapshot(temp.path()), before_preview);
    assert_eq!(preview.planned_partition_count, 2);
    assert_eq!(preview.payload_eligible_partition_count, 2);
    assert_eq!(preview.selected_partition_count, 2);
    assert_eq!(preview.payload_opened_partition_count, 2);
    assert_eq!(preview.attested_partition_count, 0);
    assert_eq!(preview.inspected_partition_count, 2);
    assert_eq!(preview.inspected_batch_count, 2);
    assert_eq!(preview.partially_inspected_partition_count, 0);
    assert_eq!(preview.payload_uninspected_partition_count, 0);
    assert_eq!(preview.row_count, 2);
    assert_eq!(
        preview.selection.policy,
        cdf_engine::PREVIEW_POLICY_BALANCED_STRATIFIED_V1
    );
    assert_eq!(
        preview.selection.selector,
        cdf_kernel::STRATIFIED_HASH_SELECTOR_V1
    );
    assert!(preview.fields.iter().any(|field| field == "id"));

    let package = temp.path().join("package");
    let run = futures_executor::block_on(cdf_engine::execute_to_package(
        &plan,
        resource.queryable(),
        &package,
    ))
    .unwrap();
    assert_eq!(run.profile.output_rows, preview.row_count);
    assert_eq!(run.profile.output_batches, preview.inspected_batch_count);
    cdf_package::PackageReader::open(package)
        .unwrap()
        .verify()
        .unwrap();
}

fn scenario(id: &str) -> &'static P2Scenario {
    P2_SCENARIOS
        .iter()
        .find(|scenario| scenario.id == id)
        .unwrap_or_else(|| panic!("missing P2 scenario {id}"))
}

fn friction(id: u8) -> &'static P2FrictionRow {
    P2_FRICTIONS
        .iter()
        .find(|row| row.id == id)
        .unwrap_or_else(|| panic!("missing P2 friction {id}"))
}

fn assert_active_tickets(label: &str, tickets: &[&str]) {
    assert!(
        !tickets.is_empty(),
        "{label} must name active ticket owners"
    );
    for ticket in tickets {
        ticket_owner_status(ticket)
            .unwrap_or_else(|error| panic!("{label} must name an active ticket owner: {error}"));
    }
}

fn ticket_owner_status(ticket: &str) -> std::result::Result<String, String> {
    if !ticket.starts_with(".10x/tickets/") {
        return Err(format!("`{ticket}` is not a ticket record"));
    }

    let contents = fs::read_to_string(workspace_root().join(ticket))
        .map_err(|error| format!("ticket owner `{ticket}` cannot be read: {error}"))?;
    let status = contents
        .lines()
        .find_map(|line| line.strip_prefix("Status: "))
        .ok_or_else(|| format!("ticket owner `{ticket}` has no Status header"))?;

    match status {
        "open" | "active" | "blocked" => Ok(status.to_owned()),
        "done" | "cancelled" => Err(format!(
            "ticket owner `{ticket}` has terminal status `{status}`"
        )),
        other => Err(format!(
            "ticket owner `{ticket}` has unsupported status `{other}`"
        )),
    }
}

fn assert_named_test_exists(label: &str, test: &str) {
    let (path, _) = test
        .split_once("::")
        .unwrap_or_else(|| panic!("{label} test must name a source path and function: {test}"));
    let function = test
        .rsplit("::")
        .next()
        .unwrap_or_else(|| panic!("{label} test must name a function: {test}"));
    let contents = fs::read_to_string(workspace_root().join(path))
        .unwrap_or_else(|error| panic!("{label} test source `{path}` cannot be read: {error}"));
    assert!(
        contents.contains(&format!("fn {function}(")),
        "{label} names missing test function `{function}` in `{path}`"
    );
}

fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("cdf-conformance must be located under <workspace>/crates")
}

fn preview_fingerprint(
    cell: RunMatrixCell,
    environment: &ConformanceEnvironment,
) -> Result<PreviewFingerprint> {
    let temp = tempfile::tempdir().map_err(|error| {
        cdf_kernel::CdfError::data(format!("create P2 parity preview tempdir: {error}"))
    })?;
    let package_id = format!(
        "p2-preview-parity-{}-{}",
        cell.source_archetype.as_str(),
        cell.disposition.as_str()
    );

    let source = source_catalog::prepare(&cell, temp.path(), environment)?;
    let plan = source.engine_plan(&package_id, cell.disposition, None)?;
    let partitions = planned_partitions(source.queryable(), &plan.scan)?;
    let preview = futures_executor::block_on(cdf_engine::preview_resource(
        &plan,
        source.queryable(),
        cdf_engine::EnginePreviewLimits::default(),
    ))?;
    let partition_count = partitions.len();

    Ok(PreviewFingerprint {
        source: cell.source_archetype,
        row_count: preview.row_count,
        field_names: preview.fields,
        partition_count,
    })
}

fn write_s5_project(root: &Path, base_url: &str, secret: &str) {
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::create_dir_all(root.join(".cdf")).unwrap();
    fs::write(root.join("rest-token"), format!("{secret}\n")).unwrap();
    fs::write(
        root.join("cdf.toml"),
        r#"
[project]
name = "p2_s5_conformance"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/s5.duckdb"

[resources."api.*"]
source = "resources/api.toml"
"#,
    )
    .unwrap();
    fs::write(
        root.join("resources/api.toml"),
        format!(
            r#"
[source.api]
kind = "rest"
base_url = "{base_url}"
auth = {{ kind = "bearer", token = "secret://file/rest-token" }}
egress_allowlist = ["127.0.0.1"]

[resource.items]
path = "/items"
records = "$.items"
cursor = {{ field = "updated_at", param = "since", ordering = "exact", lag = "0ms" }}
write_disposition = "append"
trust = "governed"
"#
        ),
    )
    .unwrap();
}

fn write_s7_append_project(root: &Path) {
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::create_dir_all(root.join("data")).unwrap();
    fs::create_dir_all(root.join(".cdf")).unwrap();
    fs::write(
        root.join("cdf.toml"),
        r#"
[project]
name = "p2_s7_append_conformance"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/s7.duckdb"

[resources."local.*"]
source = "resources/files.toml"
"#,
    )
    .unwrap();
    fs::write(
        root.join("resources/files.toml"),
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events.ndjson"
format = "ndjson"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "updated_at", type = "int64", nullable = false },
] }
"#,
    )
    .unwrap();
    fs::write(
        root.join("data/events.ndjson"),
        "{\"id\":1,\"updated_at\":10}\n{\"id\":2,\"updated_at\":20}\n",
    )
    .unwrap();
}

fn write_s7_merge_project(root: &Path, base_url: &str, missing_secret: &str) {
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::create_dir_all(root.join(".cdf")).unwrap();
    fs::write(
        root.join("cdf.toml"),
        r#"
[project]
name = "p2_s7_merge_conformance"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/s7-merge.duckdb"

[resources."api.*"]
source = "resources/api.toml"
"#,
    )
    .unwrap();
    fs::write(
        root.join("resources/api.toml"),
        format!(
            r#"
[source.api]
kind = "rest"
base_url = "{base_url}"
auth = {{ kind = "bearer", token = "secret://file/{missing_secret}" }}
egress_allowlist = ["127.0.0.1"]

[resource.items]
path = "/items"
records = "$.items"
write_disposition = "merge"
trust = "governed"
"#
        ),
    )
    .unwrap();
}

fn invoke_cli(root: &Path, args: &[&str]) -> cdf_cli_core::output::InvocationResult {
    let mut argv = vec![
        OsString::from("cdf"),
        OsString::from("--json"),
        OsString::from("--project"),
        root.as_os_str().to_os_string(),
    ];
    argv.extend(args.iter().map(|arg| OsString::from(*arg)));
    cdf_cli::invoke(argv)
}

fn invoke_success_json(root: &Path, args: &[&str], secret: Option<&str>) -> Value {
    let result = invoke_cli(root, args);
    assert_eq!(
        result.exit_code, 0,
        "stdout:\n{}\nstderr:\n{}",
        result.stdout, result.stderr
    );
    if let Some(secret) = secret {
        assert!(!result.stdout.contains(secret));
        assert!(!result.stderr.contains(secret));
    }
    success_json(&result)
}

fn success_json(result: &cdf_cli_core::output::InvocationResult) -> Value {
    serde_json::from_str(&result.stdout).unwrap()
}

fn assert_success_without_key_nudge(result: &cdf_cli_core::output::InvocationResult) {
    assert_eq!(
        result.exit_code, 0,
        "stdout:\n{}\nstderr:\n{}",
        result.stdout, result.stderr
    );
    let output = format!("{}{}", result.stdout, result.stderr).to_ascii_lowercase();
    for forbidden in [
        "primary_key",
        "merge_key",
        "primary key",
        "merge key",
        "composite key",
        "missing key",
        "add a key",
        "invent a key",
    ] {
        assert!(
            !output.contains(forbidden),
            "keyless append output contained {forbidden:?}:\n{output}"
        );
    }
}

fn assert_generated_artifacts_do_not_contain(root: &Path, secret: &str) {
    for (path, bytes) in project_tree_snapshot(root) {
        if path == "cdf.lock" || path.starts_with(".cdf/") {
            assert!(
                !bytes
                    .windows(secret.len())
                    .any(|window| window == secret.as_bytes()),
                "generated artifact {path} leaked the source secret"
            );
        }
    }
}

fn project_tree_snapshot(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn visit(root: &Path, directory: &Path, entries: &mut BTreeMap<String, Vec<u8>>) {
        let mut paths = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        paths.sort();
        for path in paths {
            let relative = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            if path.is_dir() {
                entries.insert(format!("{relative}/"), Vec::new());
                visit(root, &path, entries);
            } else {
                entries.insert(relative, fs::read(path).unwrap());
            }
        }
    }

    let mut entries = BTreeMap::new();
    visit(root, root, &mut entries);
    entries
}

struct RecordedHttpServer {
    base_url: String,
    requests: Arc<Mutex<Vec<String>>>,
    failure: Arc<Mutex<Option<String>>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

const RECORDED_HTTP_HEADER_CAP: usize = 8192;
const RECORDED_HTTP_HEADER_DEADLINE: Duration = Duration::from_secs(1);
const RECORDED_HTTP_RESPONSE_DEADLINE: Duration = Duration::from_secs(1);

fn read_recorded_http_header(
    stream: &mut impl Read,
    deadline: Duration,
) -> std::io::Result<Vec<u8>> {
    let started = Instant::now();
    let mut request = Vec::new();

    loop {
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            return Ok(request);
        }
        if request.len() == RECORDED_HTTP_HEADER_CAP {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "recorded HTTP fixture request header exceeded the {RECORDED_HTTP_HEADER_CAP}-byte cap before the header terminator"
                ),
            ));
        }
        if started.elapsed() >= deadline {
            return Err(std::io::Error::new(
                ErrorKind::TimedOut,
                format!(
                    "recorded HTTP fixture request header remained incomplete after {} ms",
                    deadline.as_millis()
                ),
            ));
        }

        let remaining = RECORDED_HTTP_HEADER_CAP - request.len();
        let mut chunk = [0_u8; 1024];
        let read_len = remaining.min(chunk.len());
        match stream.read(&mut chunk[..read_len]) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    format!(
                        "recorded HTTP fixture request header ended after {} bytes before the header terminator",
                        request.len()
                    ),
                ));
            }
            Ok(bytes_read) => request.extend_from_slice(&chunk[..bytes_read]),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn run_recorded_http_server(
    listener: TcpListener,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    mut bodies: VecDeque<String>,
) -> std::result::Result<(), String> {
    while !stop.load(Ordering::Relaxed) && !bodies.is_empty() {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream.set_nonblocking(true).map_err(|error| {
                    format!(
                        "recorded HTTP fixture could not make accepted socket nonblocking: {error}"
                    )
                })?;
                let request = read_recorded_http_header(&mut stream, RECORDED_HTTP_HEADER_DEADLINE)
                    .map_err(|error| {
                        format!("recorded HTTP fixture request capture failed: {error}")
                    })?;
                requests
                    .lock()
                    .map_err(|_| "recorded HTTP fixture request log was poisoned".to_owned())?
                    .push(String::from_utf8_lossy(&request).into_owned());
                let body = bodies.pop_front().ok_or_else(|| {
                    "recorded HTTP fixture accepted more requests than configured responses"
                        .to_owned()
                })?;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );

                stream.set_nonblocking(false).map_err(|error| {
                    format!(
                        "recorded HTTP fixture could not restore blocking response I/O: {error}"
                    )
                })?;
                stream
                    .set_write_timeout(Some(RECORDED_HTTP_RESPONSE_DEADLINE))
                    .map_err(|error| {
                        format!("recorded HTTP fixture could not bound response writes: {error}")
                    })?;
                stream.write_all(response.as_bytes()).map_err(|error| {
                    format!("recorded HTTP fixture response write failed: {error}")
                })?;
                stream.flush().map_err(|error| {
                    format!("recorded HTTP fixture response flush failed: {error}")
                })?;
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => {
                return Err(format!("recorded HTTP fixture accept failed: {error}"));
            }
        }
    }
    Ok(())
}

fn store_recorded_http_failure(failure: &Arc<Mutex<Option<String>>>, message: String) {
    let mut failure = failure
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if failure.is_none() {
        *failure = Some(message);
    }
}

#[test]
fn recorded_http_server_waits_for_split_request_headers() {
    let server = RecordedHttpServer::new([r#"{"items":[]}"#]);
    let address = server.base_url().strip_prefix("http://").unwrap();
    let mut client = TcpStream::connect(address).unwrap();

    client
        .write_all(b"GET /items HTTP/1.1\r\nhost: local\r\n")
        .unwrap();
    thread::sleep(Duration::from_millis(20));
    client
        .write_all(b"authorization: Bearer split-secret\r\n\r\n")
        .unwrap();

    let mut response = String::new();
    client.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    let requests = server.requests().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("authorization: Bearer split-secret"));
}

#[test]
fn recorded_http_server_restores_blocking_response_writes() {
    let body = "x".repeat(4 * 1024 * 1024);
    let server = RecordedHttpServer::new([body.clone()]);
    let address = server.base_url().strip_prefix("http://").unwrap();
    let mut client = TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client
        .write_all(b"GET /items HTTP/1.1\r\nhost: local\r\n\r\n")
        .unwrap();

    thread::sleep(Duration::from_millis(20));
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    let body_start = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|offset| offset + 4)
        .expect("recorded HTTP response header terminator");
    assert_eq!(&response[body_start..], body.as_bytes());
    assert_eq!(server.requests().unwrap().len(), 1);
}

#[test]
fn recorded_http_server_surfaces_capture_failure_without_drop_panic() {
    let server = RecordedHttpServer::new([r#"{"items":[]}"#]);
    let address = server.base_url().strip_prefix("http://").unwrap();
    let mut client = TcpStream::connect(address).unwrap();
    client
        .write_all(b"GET /items HTTP/1.1\r\nhost: local\r\n")
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let failure = loop {
        match server.requests() {
            Err(failure) => break failure,
            Ok(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(5)),
            Ok(_) => panic!("recorded HTTP fixture did not surface incomplete header failure"),
        }
    };
    assert!(failure.contains("request capture failed"));
    assert!(failure.contains("before the header terminator"));
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(server))).is_ok());
}

#[test]
fn recorded_http_header_capture_retries_would_block_and_bounds_incomplete_requests() {
    struct BecomesReady {
        would_block: bool,
        bytes: std::io::Cursor<Vec<u8>>,
    }
    impl Read for BecomesReady {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.would_block {
                self.would_block = false;
                return Err(std::io::Error::from(ErrorKind::WouldBlock));
            }
            self.bytes.read(buffer)
        }
    }

    let complete = b"GET /items HTTP/1.1\r\nauthorization: Bearer delayed\r\n\r\n";
    let mut becomes_ready = BecomesReady {
        would_block: true,
        bytes: std::io::Cursor::new(complete.to_vec()),
    };
    assert_eq!(
        read_recorded_http_header(&mut becomes_ready, Duration::from_millis(50)).unwrap(),
        complete
    );

    let mut eof = std::io::Cursor::new(b"GET /items HTTP/1.1\r\nhost: local\r\n".as_slice());
    let error = read_recorded_http_header(&mut eof, Duration::from_millis(50)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
    assert!(error.to_string().contains("before the header terminator"));

    let mut oversized = std::io::Cursor::new(vec![b'x'; RECORDED_HTTP_HEADER_CAP]);
    let error = read_recorded_http_header(&mut oversized, Duration::from_millis(50)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(error.to_string().contains("8192-byte cap"));

    struct NeverReady;
    impl Read for NeverReady {
        fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::from(ErrorKind::WouldBlock))
        }
    }

    let mut never_ready = NeverReady;
    let error = read_recorded_http_header(&mut never_ready, Duration::from_millis(5)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    assert!(error.to_string().contains("incomplete after 5 ms"));
}

impl RecordedHttpServer {
    fn new<I, S>(bodies: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let thread_requests = Arc::clone(&requests);
        let failure = Arc::new(Mutex::new(None));
        let thread_failure = Arc::clone(&failure);
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let bodies = bodies.into_iter().map(Into::into).collect::<VecDeque<_>>();
        let thread = thread::spawn(move || {
            if let Err(message) =
                run_recorded_http_server(listener, thread_requests, thread_stop, bodies)
            {
                store_recorded_http_failure(&thread_failure, message);
            }
        });
        Self {
            base_url: format!("http://{address}"),
            requests,
            failure,
            stop,
            thread: Some(thread),
        }
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn requests(&self) -> std::result::Result<Vec<String>, String> {
        if let Some(failure) = self
            .failure
            .lock()
            .map_err(|_| "recorded HTTP fixture failure state was poisoned".to_owned())?
            .clone()
        {
            return Err(failure);
        }
        self.requests
            .lock()
            .map_err(|_| "recorded HTTP fixture request log was poisoned".to_owned())
            .map(|requests| requests.clone())
    }
}

impl Drop for RecordedHttpServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            store_recorded_http_failure(
                &self.failure,
                "recorded HTTP fixture worker panicked".to_owned(),
            );
        }
    }
}
