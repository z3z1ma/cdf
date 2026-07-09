use cdf_dest_duckdb::DuckDbDestination;
use cdf_kernel::{
    Batch, BatchStream, CheckpointStore, CursorValue, PipelineId, ResourceId, ResourceStream,
    Result, ScopeKey, SourcePosition, WriteDisposition, source_name,
};
use cdf_package::PackageReader;
use cdf_state_sqlite::SqliteCheckpointStore;
use futures_util::StreamExt;
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    ffi::OsString,
    fs,
    io::{ErrorKind, Read, Write},
    net::TcpListener,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use super::{
    MatrixDestination, MatrixDisposition, RunMatrixCell, SourceArchetype, core, file_fixture,
    local_postgres::LivePostgres, plan_json, rest_fixture, sql_fixture,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CoverageStatus {
    Covered,
    Excluded,
    Pending,
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

const WS_I: &str = ".10x/tickets/2026-07-08-p2-ws-i-conformance-parity-friction-suite.md";
const WS_A: &str = ".10x/tickets/2026-07-08-p2-ws-a-discovery-compiler-stage.md";
const WS_B: &str = ".10x/tickets/2026-07-08-p2-ws-b-schema-reconciliation-arrow-vocabulary.md";
const WS_C: &str = ".10x/tickets/2026-07-08-p2-ws-c-source-identity-normalization.md";
const WS_D: &str = ".10x/tickets/2026-07-08-p2-ws-d-file-source-globs-manifest-compression.md";
const WS_E: &str = ".10x/tickets/2026-07-08-p2-ws-e-remote-transports.md";
const WS_G: &str = ".10x/tickets/2026-07-08-p2-ws-g-source-diagnostics-deep-validate.md";
const WS_H: &str = ".10x/tickets/2026-07-08-p2-ws-h-scaffolding-id-model-two-minute-path.md";

const P2_SCENARIOS: &[P2Scenario] = &[
    P2Scenario {
        id: "S1",
        title: "Public HTTPS Parquet single file, zero typed schema fields, through cdf add and run",
        status: CoverageStatus::Pending,
        rationale: "deterministic HTTPS Parquet discovery/run and cdf add are covered primitives; the public TLC first-attempt flow and recorded live session remain pending",
        tests: &[
            "crates/cdf-project/src/tests.rs::http_parquet_schema_discovery_uses_bounded_ranges_without_artifacts",
            "crates/cdf-project/src/tests.rs::http_parquet_auto_pin_plan_preview_and_run_use_file_runtime",
            "crates/cdf-cli/src/tests.rs::add_http_parquet_pins_schema_with_bounded_fixture_requests",
        ],
        tickets: &[WS_E, WS_H, WS_I],
    },
    P2Scenario {
        id: "S2",
        title: "Public HTTPS Parquet monthly glob with default FileManifest incrementality and no-change no-op rerun",
        status: CoverageStatus::Pending,
        rationale: "local manifest incrementality and no-op reruns are covered; HTTP template/glob enumeration and public monthly-file conformance remain pending",
        tests: &[
            "crates/cdf-declarative/src/tests.rs::file_glob_plans_deterministic_partition_per_match",
            "crates/cdf-project/src/runtime_tests.rs::file_manifest_append_run_skips_unchanged_files_and_loads_only_changes",
        ],
        tickets: &[WS_D, WS_E, WS_I],
    },
    P2Scenario {
        id: "S3",
        title: "S3 compressed NDJSON recursive glob with transparent gzip and drift governed by contract policy",
        status: CoverageStatus::Pending,
        rationale: "local gzip/zstd NDJSON decode and drift quarantine are covered primitives; S3 transport, recursive remote globs, remote compression, and per-file variance conformance remain pending",
        tests: &[
            "crates/cdf-declarative/src/tests.rs::file_runtime_auto_compression_decodes_gzip_and_zstd_ndjson",
            "crates/cdf-conformance/src/live_run/drift_quarantine/mod.rs::drift_quarantine_duckdb_conformance_asserts_unsupported_mirror_exclusion",
        ],
        tickets: &[WS_D, WS_E, WS_I],
    },
    P2Scenario {
        id: "S4",
        title: "Postgres table discovery with optional schema block and cursor candidates",
        status: CoverageStatus::Pending,
        rationale: "Postgres catalog discover/preview/run primitives are covered; cdf add, cursor-candidate suggestions, and final S4 conformance remain pending",
        tests: &[
            "crates/cdf-cli/src/tests.rs::schema_discover_postgres_catalog_uses_project_secret_without_writes_or_secret_leak",
            "crates/cdf-cli/src/tests.rs::postgres_discover_mode_plan_preview_run_autopins_through_file_secret_without_leaks",
        ],
        tickets: &[WS_A, WS_H, WS_I],
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
        status: CoverageStatus::Pending,
        rationale: "accepted-stream quarantine and deep-validate foundations exist; incompatible per-file schema verdicts plus file/column remediation rendering remain pending",
        tests: &[
            "crates/cdf-conformance/src/live_run/drift_quarantine/mod.rs::drift_quarantine_duckdb_conformance_asserts_unsupported_mirror_exclusion",
            "crates/cdf-conformance/src/live_run/drift_quarantine/mod.rs::drift_quarantine_postgres_conformance_asserts_supported_mirror",
        ],
        tickets: &[WS_D, WS_G, WS_I],
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
        status: CoverageStatus::Pending,
        rationale: "local file, REST fixture, and Postgres table row/schema fingerprints are partial evidence only; every required archetype must still share resolution, decode, discovery, reconciliation, normalization, and the full compiler front end",
        tests: &[
            "crates/cdf-conformance/src/run_matrix/data_onramp.rs::p2_preview_run_parity_law_covers_supported_archetypes",
        ],
        tickets: &[WS_A, WS_B, WS_C, WS_D, WS_E, WS_I],
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
        open_tickets: &[WS_D, WS_E, WS_H, WS_I],
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
        open_tickets: &[WS_A, WS_H, WS_I],
    },
    P2FrictionRow {
        id: 3,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::declarative_arrow_type_strings_compile_from_toml",
            "crates/cdf-declarative/src/tests.rs::declarative_arrow_type_strings_compile_from_yaml",
        ],
        open_tickets: &[WS_B, WS_I],
    },
    P2FrictionRow {
        id: 4,
        closed_tests: &[
            "crates/cdf-contract/src/tests.rs::schema_reconciliation_records_lossless_widenings_and_physical_type",
            "crates/cdf-formats/src/tests.rs::declared_parquet_int32_declared_int64_materializes_lossless_widening",
            "crates/cdf-formats/src/tests.rs::declared_parquet_float32_declared_float64_materializes_lossless_widening",
        ],
        open_tickets: &[WS_B, WS_I],
    },
    P2FrictionRow {
        id: 5,
        closed_tests: &[
            "crates/cdf-formats/src/tests.rs::declared_parquet_projection_renames_by_source_name_and_drops_extra_fields",
            "crates/cdf-formats/src/tests.rs::declared_parquet_lossy_narrowing_fails_before_batches_are_emitted",
            "crates/cdf-formats/src/tests.rs::undeclared_parquet_read_preserves_physical_schema_after_declared_path_added",
        ],
        open_tickets: &[WS_B, WS_I],
    },
    P2FrictionRow {
        id: 6,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::declarative_schema_normalizes_field_names_and_records_source_names",
            "crates/cdf-contract/src/tests.rs::destination_identifier_policy_preserves_postgres_max_length",
            "crates/cdf-cli/src/tests.rs::duckdb_destination_policy_normalizes_plan_preview_package_and_commit",
            "crates/cdf-cli/src/tests.rs::destination_normalization_collision_fails_before_writes",
            "crates/cdf-project/src/runtime_tests.rs::postgres_destination_policy_truncates_package_and_committed_column_identically",
            "crates/cdf-project/src/runtime_tests.rs::stale_long_name_column_program_cannot_spoof_destination_policy_before_writes",
        ],
        open_tickets: &[WS_C, WS_I],
    },
    P2FrictionRow {
        id: 7,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::declarative_schema_normalizes_field_names_and_records_source_names",
            "crates/cdf-cli/src/tests.rs::duckdb_destination_policy_normalizes_plan_preview_package_and_commit",
            "crates/cdf-project/src/runtime_tests.rs::postgres_destination_policy_truncates_package_and_committed_column_identically",
        ],
        open_tickets: &[WS_C, WS_I],
    },
    P2FrictionRow {
        id: 8,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::file_glob_plans_deterministic_partition_per_match",
            "crates/cdf-project/src/runtime_tests.rs::general_project_run_commits_multi_file_resource_manifest_checkpoint",
            "crates/cdf-project/src/runtime_tests.rs::file_manifest_append_run_skips_unchanged_files_and_loads_only_changes",
        ],
        open_tickets: &[WS_D, WS_E, WS_I],
    },
    P2FrictionRow {
        id: 9,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::file_glob_run_and_preview_open_the_requested_partition",
            "crates/cdf-conformance/src/run_matrix/data_onramp.rs::p2_preview_run_parity_law_covers_supported_archetypes",
        ],
        open_tickets: &[WS_E, WS_I],
    },
    P2FrictionRow {
        id: 10,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::source_and_resource_names_form_canonical_compiled_id",
            "crates/cdf-project/src/tests.rs::declarative_resource_mapping_pattern_must_match_compiled_id",
            "crates/cdf-cli/src/tests.rs::resource_mapping_pattern_mismatch_reports_validate_and_plan_commands",
        ],
        open_tickets: &[WS_H],
    },
    P2FrictionRow {
        id: 11,
        closed_tests: &[
            "crates/cdf-cli/src/tests.rs::resource_not_compiled_error_names_compiled_ids_origins_and_fix",
        ],
        open_tickets: &[WS_G, WS_I],
    },
    P2FrictionRow {
        id: 12,
        closed_tests: &[
            "crates/cdf-cli/src/scan_command.rs::tests::plan_error_wording_uses_plan_command_name",
            "crates/cdf-cli/src/tests.rs::resource_mapping_pattern_mismatch_reports_validate_and_plan_commands",
        ],
        open_tickets: &[WS_G, WS_I],
    },
    P2FrictionRow {
        id: 13,
        closed_tests: &[
            "crates/cdf-cli/src/tests.rs::resource_not_compiled_error_names_compiled_ids_origins_and_fix",
        ],
        open_tickets: &[WS_G, WS_I],
    },
    P2FrictionRow {
        id: 14,
        closed_tests: &[
            "crates/cdf-cli/src/tests.rs::validate_deep_reports_source_front_end_checks_without_writes",
        ],
        open_tickets: &[WS_G, WS_I],
    },
    P2FrictionRow {
        id: 15,
        closed_tests: &[
            "crates/cdf-declarative/src/file_transport.rs::tests::file_transport_http_metadata_and_bounded_range_use_http_client",
            "crates/cdf-declarative/src/file_transport.rs::tests::file_transport_http_range_rejects_unbounded_or_ignored_range",
            "crates/cdf-project/src/tests.rs::http_parquet_schema_discovery_uses_bounded_ranges_without_artifacts",
            "crates/cdf-project/src/tests.rs::http_parquet_auto_pin_plan_preview_and_run_use_file_runtime",
        ],
        open_tickets: &[WS_D, WS_E, WS_I],
    },
    P2FrictionRow {
        id: 16,
        closed_tests: &[
            "crates/cdf-formats/src/tests.rs::compression_ndjson_file_sources_decode_and_preserve_compressed_identity",
            "crates/cdf-declarative/src/tests.rs::file_runtime_auto_compression_decodes_gzip_and_zstd_ndjson",
            "crates/cdf-declarative/src/tests.rs::file_runtime_explicit_compression_mismatch_names_file_and_signals",
        ],
        open_tickets: &[WS_D, WS_E, WS_I],
    },
    P2FrictionRow {
        id: 17,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::disposition_append_default_and_explicit_forms_are_keyless",
            "crates/cdf-declarative/src/tests.rs::disposition_merge_requires_explicit_merge_key_with_remediation",
            "crates/cdf-declarative/src/tests.rs::disposition_merge_with_explicit_merge_key_compiles",
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
            "crates/cdf-formats/src/tests.rs::parquet_file_source_produces_descriptor_batches_and_file_manifest",
            "crates/cdf-formats/src/tests.rs::declared_parquet_int32_declared_int64_materializes_lossless_widening",
            "crates/cdf-cli/src/tests.rs::run_local_parquet_discover_autopins_and_commits_pinned_schema",
            "crates/cdf-project/src/tests.rs::http_parquet_auto_pin_plan_preview_and_run_use_file_runtime",
            "crates/cdf-cli/src/tests.rs::add_http_parquet_pins_schema_with_bounded_fixture_requests",
        ],
        open_tickets: &[WS_D, WS_E, WS_H, WS_I],
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
            CoverageStatus::Pending => assert_active_tickets(scenario.id, scenario.tickets),
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

    assert_eq!(scenario("S1").status, CoverageStatus::Pending);
    assert_eq!(scenario("S2").status, CoverageStatus::Pending);
    assert_eq!(scenario("S3").status, CoverageStatus::Pending);
    assert_eq!(scenario("S4").status, CoverageStatus::Pending);
    assert_eq!(scenario("S5").status, CoverageStatus::Covered);
    assert_eq!(scenario("S6").status, CoverageStatus::Pending);
    assert_eq!(scenario("S7").status, CoverageStatus::Covered);
    assert_eq!(scenario("S8").status, CoverageStatus::Pending);
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
        test.contains("declared_parquet_int32_declared_int64_materializes_lossless_widening")
    }));
    assert!(
        friction(5)
            .closed_tests
            .iter()
            .any(|test| test.contains("declared_parquet_projection_renames_by_source_name"))
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
    assert!(friction(16).closed_tests.iter().any(|test| {
        test.contains("file_runtime_auto_compression_decodes_gzip_and_zstd_ndjson")
    }));
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

    for pending in ["S1", "S2", "S3", "S4", "S6", "S8"] {
        assert_eq!(scenario(pending).status, CoverageStatus::Pending);
    }
}

#[test]
fn p2_s5_rest_discover_pin_preview_run_package_checkpoint_conformance() {
    const SECRET: &str = "s5-recorded-rest-secret";
    const PACKAGE_ID: &str = "p2-s5-rest-discover";
    const CHECKPOINT_ID: &str = "checkpoint-p2-s5-rest-discover";
    const PIPELINE_ID: &str = "pipeline-p2-s5-rest-discover";
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
    assert_eq!(first["snapshot_metadata"]["probe"], "rest-sample-page");
    assert_eq!(first["snapshot_metadata"]["source_kind"], "rest");
    assert_eq!(first["source_identity"]["sample_pages"], "1");
    assert_eq!(first["source_identity"]["sample_records"], "2");
    assert_eq!(first["writes"]["schema_snapshot"], true);
    assert_eq!(first["writes"]["lockfile"], true);

    let pinned_hash = first["schema_hash"].as_str().unwrap().to_owned();
    let snapshot_path = first["schema_snapshot_path"].as_str().unwrap().to_owned();
    let snapshot_bytes = fs::read(temp.path().join(&snapshot_path)).unwrap();
    let lock_bytes = fs::read(temp.path().join("cdf.lock")).unwrap();
    let snapshot: Value = serde_json::from_slice(&snapshot_bytes).unwrap();
    assert_eq!(snapshot["metadata"]["probe"], "rest-sample-page");
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

    let run = invoke_success_json(
        temp.path(),
        &[
            "run",
            "api.items",
            "--pipeline",
            PIPELINE_ID,
            "--target",
            "items",
            "--package-id",
            PACKAGE_ID,
            "--checkpoint-id",
            CHECKPOINT_ID,
        ],
        Some(SECRET),
    );
    let report = &run["result"];
    assert_eq!(report["resource_id"], "api.items");
    assert_eq!(report["schema_hash"], pinned_hash);
    assert_eq!(report["schema_snapshot"]["outcome"], "unchanged");
    assert_eq!(report["row_count"], 2);
    assert_eq!(report["checkpoint"]["status"], "committed");

    let package_dir = temp.path().join(".cdf/packages").join(PACKAGE_ID);
    let reader = PackageReader::open(&package_dir).unwrap();
    reader.verify().unwrap();
    let receipts = reader.receipts().unwrap();
    assert_eq!(receipts.len(), 1);
    let receipt = &receipts[0];
    assert_eq!(receipt.schema_hash.as_str(), pinned_hash);
    assert_eq!(receipt.disposition, WriteDisposition::Append);
    assert_eq!(receipt.counts.rows_written, 2);

    let destination = DuckDbDestination::new(temp.path().join(".cdf/s5.duckdb")).unwrap();
    assert!(destination.verify_receipt(receipt).unwrap().verified);

    let segments = reader.read_all_segments().unwrap();
    let output_schema = segments[0].1[0].schema();
    let vendor = output_schema.field_with_name("vendor_id").unwrap();
    assert_eq!(source_name(vendor), Some("VendorID"));

    let store = SqliteCheckpointStore::open(temp.path().join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new(PIPELINE_ID).unwrap(),
            &ResourceId::new("api.items").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("S5 committed checkpoint head");
    assert_eq!(head.delta.checkpoint_id.as_str(), CHECKPOINT_ID);
    assert_eq!(head.delta.schema_hash.as_str(), pinned_hash);
    assert!(receipt.covers_state_delta(&head.delta));
    let SourcePosition::Cursor(cursor) = &head.delta.output_position else {
        panic!("S5 checkpoint must carry the declared REST cursor");
    };
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));

    assert_generated_artifacts_do_not_contain(temp.path(), SECRET);
    let requests = server.requests();
    assert_eq!(requests.len(), 4);
    assert!(requests.iter().all(|request| {
        request.contains("GET /items HTTP/1.1")
            && request.contains(&format!("authorization: Bearer {SECRET}"))
    }));
}

#[test]
fn p2_s7_keyless_append_and_precontact_merge_failure_conformance() {
    const PACKAGE_ID: &str = "p2-s7-keyless-append";
    const CHECKPOINT_ID: &str = "checkpoint-p2-s7-keyless-append";
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
    let run = invoke_cli(
        append.path(),
        &[
            "run",
            "local.events",
            "--target",
            "events",
            "--package-id",
            PACKAGE_ID,
            "--checkpoint-id",
            CHECKPOINT_ID,
        ],
    );
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
    assert_eq!(merge_server.requests(), Vec::<String>::new());
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
fn p2_active_owner_validation_reads_status_and_rejects_invalid_owners() {
    let status = ticket_owner_status(WS_I).expect("WS-I must remain a nonterminal owner");
    assert!(matches!(status.as_str(), "open" | "active" | "blocked"));

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
    let postgres = LivePostgres::start().expect(
        "P2 S8 parity conformance requires Postgres coverage; set TEST_DATABASE_URL or install initdb/pg_ctl",
    );
    let cases = [
        RunMatrixCell::file(MatrixDestination::DuckDb, MatrixDisposition::Append),
        RunMatrixCell::rest(MatrixDestination::DuckDb, MatrixDisposition::Append),
        RunMatrixCell::sql(MatrixDestination::DuckDb, MatrixDisposition::Append),
    ];

    for cell in cases {
        let preview = preview_fingerprint(cell, &postgres).unwrap();
        let executed = core::execute_cell(cell, &postgres).unwrap();

        assert_eq!(preview.source, cell.source_archetype);
        assert_eq!(
            preview.row_count,
            executed.row_count,
            "{} preview row count must match package-producing run",
            cell.source_archetype.as_str()
        );
        assert_eq!(preview.row_count, core::ROW_COUNT);
        assert_eq!(preview.partition_count, core::SEGMENT_COUNT);
        assert!(
            preview.field_names.iter().any(|name| name == "id"),
            "{} preview schema should expose the id column consumed by run",
            cell.source_archetype.as_str()
        );
    }
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

fn preview_fingerprint(cell: RunMatrixCell, postgres: &LivePostgres) -> Result<PreviewFingerprint> {
    let temp = tempfile::tempdir().map_err(|error| {
        cdf_kernel::CdfError::data(format!("create P2 parity preview tempdir: {error}"))
    })?;
    let package_id = format!(
        "p2-preview-parity-{}-{}",
        cell.source_archetype.as_str(),
        cell.disposition.as_str()
    );

    let (batches, partition_count) = match cell.source_archetype {
        SourceArchetype::File => {
            let resource = file_fixture::resource(temp.path(), cell.disposition)?;
            let plan = plan_json::file_engine_plan(&resource, &package_id, cell.disposition, None)?;
            let partitions = resource.plan_partitions(&plan.scan.request)?;
            assert_file_partitions_match_plan_identity(&partitions, &plan.scan.partitions);
            let stream = futures_executor::block_on(resource.open_preview(partitions[0].clone()))?;
            (drain_batches(stream)?, partitions.len())
        }
        SourceArchetype::Rest => {
            let (resource, _) = rest_fixture::resource(cell.disposition)?;
            let plan = plan_json::planned_engine_plan(&resource, &package_id, None)?;
            let partitions = resource.plan_partitions(&plan.scan.request)?;
            assert_eq!(partitions, plan.scan.partitions);
            let stream = futures_executor::block_on(resource.open(partitions[0].clone()))?;
            (drain_batches(stream)?, partitions.len())
        }
        SourceArchetype::Sql => {
            let resource = sql_fixture::resource(cell, postgres)?;
            let plan = plan_json::planned_engine_plan(&resource, &package_id, None)?;
            let partitions = resource.plan_partitions(&plan.scan.request)?;
            assert_eq!(partitions, plan.scan.partitions);
            let stream = futures_executor::block_on(resource.open(partitions[0].clone()))?;
            (drain_batches(stream)?, partitions.len())
        }
    };

    Ok(PreviewFingerprint {
        source: cell.source_archetype,
        row_count: batches.iter().map(|batch| batch.header.row_count).sum(),
        field_names: batches
            .first()
            .and_then(|batch| {
                batch.record_batch().map(|record_batch| {
                    record_batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| field.name().clone())
                        .collect()
                })
            })
            .unwrap_or_default(),
        partition_count,
    })
}

fn assert_file_partitions_match_plan_identity(
    actual: &[cdf_kernel::PartitionPlan],
    planned: &[cdf_kernel::PartitionPlan],
) {
    assert_eq!(actual.len(), planned.len());
    for (actual, planned) in actual.iter().zip(planned) {
        assert_eq!(actual.partition_id, planned.partition_id);
        assert_eq!(actual.scope, planned.scope);
        for key in ["kind", "glob", "path", "resource_id", "bytes", "sha256"] {
            assert_eq!(
                actual.metadata.get(key),
                planned.metadata.get(key),
                "file partition metadata `{key}` should match the run plan"
            );
        }
    }
}

fn drain_batches(stream: BatchStream) -> Result<Vec<Batch>> {
    futures_executor::block_on(async {
        futures_util::pin_mut!(stream);
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        Ok(batches)
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

fn invoke_cli(root: &Path, args: &[&str]) -> cdf_cli::InvocationResult {
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

fn success_json(result: &cdf_cli::InvocationResult) -> Value {
    serde_json::from_str(&result.stdout).unwrap()
}

fn assert_success_without_key_nudge(result: &cdf_cli::InvocationResult) {
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
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
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
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let mut bodies = bodies.into_iter().map(Into::into).collect::<VecDeque<_>>();
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) && !bodies.is_empty() {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut request = Vec::new();
                        while request.len() < 8192
                            && !request.windows(4).any(|window| window == b"\r\n\r\n")
                        {
                            let mut chunk = [0_u8; 1024];
                            let bytes_read = stream.read(&mut chunk).unwrap_or(0);
                            if bytes_read == 0 {
                                break;
                            }
                            request.extend_from_slice(&chunk[..bytes_read]);
                        }
                        thread_requests
                            .lock()
                            .unwrap()
                            .push(String::from_utf8_lossy(&request).into_owned());
                        let body = bodies.pop_front().unwrap();
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        stream.write_all(response.as_bytes()).unwrap();
                        stream.flush().unwrap();
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("recorded HTTP fixture accept failed: {error}"),
                }
            }
        });
        Self {
            base_url: format!("http://{address}"),
            requests,
            stop,
            thread: Some(thread),
        }
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for RecordedHttpServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}
