use cdf_kernel::{Batch, BatchStream, ResourceStream, Result};
use futures_util::StreamExt;

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
const WS_F: &str = ".10x/tickets/2026-07-08-p2-ws-f-keys-dispositions.md";
const WS_G: &str = ".10x/tickets/2026-07-08-p2-ws-g-source-diagnostics-deep-validate.md";
const WS_H: &str = ".10x/tickets/2026-07-08-p2-ws-h-scaffolding-id-model-two-minute-path.md";
const A7: &str = ".10x/tickets/2026-07-09-p2-ws-a7-schema-pin-show-diff-cli.md";
const D3: &str = ".10x/tickets/2026-07-09-p2-ws-d3-file-manifest-incremental-noop.md";
const E2: &str = ".10x/tickets/2026-07-09-p2-ws-e2-http-file-runtime-and-discovery.md";
const G1: &str =
    ".10x/tickets/2026-07-09-p2-ws-g1-source-diagnostics-and-deep-validate-foundation.md";
const H2: &str = ".10x/tickets/2026-07-09-p2-ws-h2-cdf-add-single-file-parquet.md";

const P2_SCENARIOS: &[P2Scenario] = &[
    P2Scenario {
        id: "S1",
        title: "Public HTTPS Parquet single file, zero typed schema fields, through cdf add and run",
        status: CoverageStatus::Pending,
        rationale: "single-file local Parquet discovery exists, but HTTPS runtime and cdf add remain active work",
        tests: &[
            "crates/cdf-cli/src/tests.rs::schema_discover_local_parquet_reports_schema_without_project_writes",
            "crates/cdf-cli/src/tests.rs::run_local_parquet_discover_autopins_and_commits_pinned_schema",
        ],
        tickets: &[E2, H2, WS_I],
    },
    P2Scenario {
        id: "S2",
        title: "Public HTTPS Parquet monthly glob with default FileManifest incrementality and no-change no-op rerun",
        status: CoverageStatus::Pending,
        rationale: "local manifest aggregation exists, but incrementality, no-op rerun, and HTTPS glob coverage remain open",
        tests: &[
            "crates/cdf-declarative/src/tests.rs::file_glob_plans_deterministic_partition_per_match",
            "crates/cdf-project/src/runtime_tests.rs::general_project_run_commits_multi_file_resource_manifest_checkpoint",
        ],
        tickets: &[D3, E2, WS_I],
    },
    P2Scenario {
        id: "S3",
        title: "S3 compressed NDJSON recursive glob with transparent gzip and drift governed by contract policy",
        status: CoverageStatus::Pending,
        rationale: "drift quarantine is covered locally; S3 transport, recursive globs, and transparent compression remain open",
        tests: &[
            "crates/cdf-conformance/src/live_run/drift_quarantine/mod.rs::drift_quarantine_duckdb_conformance_asserts_unsupported_mirror_exclusion",
        ],
        tickets: &[WS_D, WS_E, WS_I],
    },
    P2Scenario {
        id: "S4",
        title: "Postgres table discovery with optional schema block and cursor candidates",
        status: CoverageStatus::Pending,
        rationale: "Postgres discover/preview/run primitives are covered; final S4 conformance ownership remains open",
        tests: &[
            "crates/cdf-cli/src/tests.rs::schema_discover_postgres_catalog_uses_project_secret_without_writes_or_secret_leak",
            "crates/cdf-cli/src/tests.rs::postgres_discover_mode_plan_preview_run_autopins_through_file_secret_without_leaks",
        ],
        tickets: &[WS_A, WS_I],
    },
    P2Scenario {
        id: "S5",
        title: "REST API in discover mode with a recorded sample page and pinned snapshot",
        status: CoverageStatus::Pending,
        rationale: "one-page REST discovery and auto-pin are covered; final S5 conformance closure remains open",
        tests: &[
            "crates/cdf-project/src/tests.rs::generic_schema_discovery_dispatch_samples_rest_without_snapshot_write",
            "crates/cdf-cli/src/tests.rs::rest_discover_mode_plan_preview_run_autopins_through_file_secret_without_leaks",
        ],
        tickets: &[WS_A, WS_I],
    },
    P2Scenario {
        id: "S6",
        title: "Drift quarantines with accepted stream unblocked and file/column remediation rendered",
        status: CoverageStatus::Pending,
        rationale: "accepted-stream quarantine conformance exists; source remediation rendering and deep validation remain open",
        tests: &[
            "crates/cdf-conformance/src/live_run/drift_quarantine/mod.rs::drift_quarantine_duckdb_conformance_asserts_unsupported_mirror_exclusion",
            "crates/cdf-conformance/src/live_run/drift_quarantine/mod.rs::drift_quarantine_postgres_conformance_asserts_supported_mirror",
        ],
        tickets: &[G1, WS_G, WS_I],
    },
    P2Scenario {
        id: "S7",
        title: "Append requires no key; merge without key fails with precise remediation",
        status: CoverageStatus::Pending,
        rationale: "compiler/scaffold key behavior is covered; CLI rendering and S7 conformance remain open",
        tests: &[
            "crates/cdf-declarative/src/tests.rs::disposition_append_default_and_explicit_forms_are_keyless",
            "crates/cdf-declarative/src/tests.rs::disposition_merge_requires_explicit_merge_key_with_remediation",
        ],
        tickets: &[WS_F, WS_H, WS_I],
    },
    P2Scenario {
        id: "S8",
        title: "Preview/run parity per source archetype",
        status: CoverageStatus::Covered,
        rationale: "current local file, REST fixture, and Postgres table direct-preview streams match package-producing run row counts",
        tests: &[
            "crates/cdf-conformance/src/run_matrix/data_onramp.rs::p2_preview_run_parity_law_covers_supported_archetypes",
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
        ],
        open_tickets: &[E2, H2, WS_I],
    },
    P2FrictionRow {
        id: 2,
        closed_tests: &[
            "crates/cdf-cli/src/tests.rs::schema_discover_local_parquet_reports_schema_without_project_writes",
            "crates/cdf-cli/src/tests.rs::schema_discover_rest_reports_sample_schema_without_project_writes_or_secret_leak",
            "crates/cdf-cli/src/tests.rs::schema_discover_postgres_catalog_uses_project_secret_without_writes_or_secret_leak",
        ],
        open_tickets: &[A7, WS_H, WS_I],
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
        ],
        open_tickets: &[WS_C, WS_I],
    },
    P2FrictionRow {
        id: 7,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::declarative_schema_normalizes_field_names_and_records_source_names",
        ],
        open_tickets: &[WS_C, WS_I],
    },
    P2FrictionRow {
        id: 8,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::file_glob_plans_deterministic_partition_per_match",
            "crates/cdf-project/src/runtime_tests.rs::general_project_run_commits_multi_file_resource_manifest_checkpoint",
        ],
        open_tickets: &[D3, E2, WS_I],
    },
    P2FrictionRow {
        id: 9,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::file_glob_run_and_preview_open_the_requested_partition",
            "crates/cdf-conformance/src/run_matrix/data_onramp.rs::p2_preview_run_parity_law_covers_supported_archetypes",
        ],
        open_tickets: &[D3, E2],
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
        closed_tests: &[],
        open_tickets: &[G1, WS_G],
    },
    P2FrictionRow {
        id: 12,
        closed_tests: &[],
        open_tickets: &[G1, WS_G],
    },
    P2FrictionRow {
        id: 13,
        closed_tests: &[],
        open_tickets: &[G1, WS_G],
    },
    P2FrictionRow {
        id: 14,
        closed_tests: &[],
        open_tickets: &[G1, WS_G],
    },
    P2FrictionRow {
        id: 15,
        closed_tests: &[
            "crates/cdf-declarative/src/file_transport.rs::tests::file_transport_http_metadata_and_bounded_range_use_http_client",
            "crates/cdf-declarative/src/file_transport.rs::tests::file_transport_http_range_rejects_unbounded_or_ignored_range",
        ],
        open_tickets: &[E2, WS_I],
    },
    P2FrictionRow {
        id: 16,
        closed_tests: &[],
        open_tickets: &[WS_D, WS_I],
    },
    P2FrictionRow {
        id: 17,
        closed_tests: &[
            "crates/cdf-declarative/src/tests.rs::disposition_append_default_and_explicit_forms_are_keyless",
            "crates/cdf-declarative/src/tests.rs::disposition_merge_requires_explicit_merge_key_with_remediation",
            "crates/cdf-declarative/src/tests.rs::disposition_merge_with_explicit_merge_key_compiles",
            "crates/cdf-project/src/tests.rs::local_project_scaffold_writes_valid_project_without_runtime_artifacts",
        ],
        open_tickets: &[WS_F, WS_H, WS_I],
    },
    P2FrictionRow {
        id: 18,
        closed_tests: &[
            "crates/cdf-formats/src/tests.rs::parquet_file_source_produces_descriptor_batches_and_file_manifest",
            "crates/cdf-formats/src/tests.rs::declared_parquet_int32_declared_int64_materializes_lossless_widening",
            "crates/cdf-cli/src/tests.rs::run_local_parquet_discover_autopins_and_commits_pinned_schema",
        ],
        open_tickets: &[E2, H2, D3, WS_I],
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
    assert!(
        !friction(11).open_tickets.is_empty(),
        "open diagnostics row stays tied to G tickets"
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
        assert!(
            ticket.starts_with(".10x/tickets/"),
            "{label} ticket must live under .10x/tickets/: {ticket}"
        );
        assert!(
            !ticket.starts_with(".10x/tickets/done/"),
            "{label} must not use terminal tickets as active owners: {ticket}"
        );
    }
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
            let plan = plan_json::file_engine_plan(&package_id, cell.disposition)?;
            let partitions = resource.plan_partitions(&plan.scan.request)?;
            assert_file_partitions_match_plan_identity(&partitions, &plan.scan.partitions);
            let stream = futures_executor::block_on(resource.open_preview(partitions[0].clone()))?;
            (drain_batches(stream)?, partitions.len())
        }
        SourceArchetype::Rest => {
            let (resource, _) = rest_fixture::resource(cell.disposition)?;
            let plan = plan_json::planned_engine_plan(&resource, &package_id)?;
            let partitions = resource.plan_partitions(&plan.scan.request)?;
            assert_eq!(partitions, plan.scan.partitions);
            let stream = futures_executor::block_on(resource.open(partitions[0].clone()))?;
            (drain_batches(stream)?, partitions.len())
        }
        SourceArchetype::Sql => {
            let resource = sql_fixture::resource(cell, postgres)?;
            let plan = plan_json::planned_engine_plan(&resource, &package_id)?;
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
