use super::{
    BTreeMap, DependencyTuple, DestinationProtocolCapabilities, DestinationSheetArtifact,
    SemanticCatalog, TypeMappingFidelity, freeze_contract_snapshots,
    generate_lockfile_with_destination_artifacts, parse_cdf_toml,
    support::{
        BOOK_PROJECT, GITHUB_RESOURCE, compile_declarative_fixture, destination_sheet,
        test_source_registry,
    },
    test_contract_snapshots,
};

#[test]
fn contract_freeze_preserves_existing_dependency_and_destination_data() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resources = compile_declarative_fixture(&test_source_registry(), GITHUB_RESOURCE).unwrap();
    let sheet = destination_sheet("duckdb", TypeMappingFidelity::Lossless);
    let sheet_artifact =
        DestinationSheetArtifact::new(sheet, DestinationProtocolCapabilities::default()).unwrap();
    let dependency_tuple = DependencyTuple {
        cdf: "0.1.0-old".to_owned(),
        arrow_rs: "58.3.0-old".to_owned(),
        datafusion: Some("pinned-datafusion".to_owned()),
        object_store: Some("pinned-object-store".to_owned()),
        duckdb_rs: Some("pinned-duckdb".to_owned()),
        rust: Some("pinned-rust".to_owned()),
    };
    let existing = generate_lockfile_with_destination_artifacts(
        &config,
        &resources,
        dependency_tuple.clone(),
        std::slice::from_ref(&sheet_artifact),
        BTreeMap::new(),
        &SemanticCatalog::builtins().unwrap(),
    )
    .unwrap();

    let (lock, report) = freeze_contract_snapshots(
        &config,
        &resources,
        Some(&existing),
        &[],
        Some("github.issues"),
        &SemanticCatalog::builtins().unwrap(),
    )
    .unwrap();

    assert_eq!(
        lock.resources["github.issues"].compiler.dependency_tuple,
        dependency_tuple
    );
    assert_eq!(lock.destinations, existing.destinations);
    assert_eq!(report.resource_ids, vec!["github.issues"]);
    let snapshot = lock.resources["github.issues"].contract.as_ref().unwrap();
    assert!(
        snapshot
            .policy_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(
        snapshot
            .validation_program_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
}

#[test]
fn contract_test_reports_field_level_snapshot_drift() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resources = compile_declarative_fixture(&test_source_registry(), GITHUB_RESOURCE).unwrap();
    let artifact = cdf_kernel::DestinationSheetArtifact::new(
        destination_sheet("duckdb", TypeMappingFidelity::Lossless),
        cdf_kernel::DestinationProtocolCapabilities::default(),
    )
    .unwrap();
    let (lock, _) = freeze_contract_snapshots(
        &config,
        &resources,
        None,
        &[artifact],
        None,
        &SemanticCatalog::builtins().unwrap(),
    )
    .unwrap();
    let changed_resource = GITHUB_RESOURCE.replace(
        "  { name = \"updated_at\", type = \"timestamp_micros\", nullable = false, timezone = \"UTC\" },",
        concat!(
            "  { name = \"updated_at\", type = \"timestamp_micros\", nullable = false, timezone = \"UTC\" },\n",
            "  { name = \"ingested_at\", type = \"int64\", nullable = true },"
        ),
    );
    let changed_resources =
        compile_declarative_fixture(&test_source_registry(), &changed_resource).unwrap();

    let report = test_contract_snapshots(&lock, &changed_resources, Some("github.issues")).unwrap();

    assert_eq!(report.counts.passed, 0);
    assert_eq!(report.counts.drifted, 1);
    let fields = report
        .drift_details
        .iter()
        .map(|detail| detail.field.as_str())
        .collect::<Vec<_>>();
    assert!(fields.contains(&"schema_hash"));
    assert!(fields.contains(&"validation_program_hash"));
}
