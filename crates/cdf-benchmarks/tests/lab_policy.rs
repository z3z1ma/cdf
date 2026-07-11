use std::fs;

use cdf_benchmarks::{
    BenchmarkReport, ComparisonVerdict, EnvelopeSpec, IoMode, ReferenceIdentity, canonical_sha256,
    compare_reports, comparison_fails, generate_envelope, install_baseline, report_fixture,
    summarize_samples,
};

fn current_report(percent: u64) -> BenchmarkReport {
    let mut report = report_fixture().unwrap();
    for observation in &mut report.observations {
        observation.comparability.cdf_revision = "current-revision".to_owned();
        if observation.samples.is_empty() {
            continue;
        }
        for sample in &mut observation.samples {
            sample.wall_time_ns = sample.wall_time_ns * percent / 100;
        }
        observation.summary = Some(summarize_samples(&observation.samples).unwrap());
    }
    report
}

fn observed_verdict(report: &cdf_benchmarks::ComparisonReport) -> &ComparisonVerdict {
    &report
        .cells
        .iter()
        .find(|cell| cell.workload_id == "legacy_medium_ndjson_package")
        .unwrap()
        .verdict
}

#[test]
fn comparison_threshold_and_variance_boundaries_are_exact() {
    let baseline = report_fixture().unwrap();
    let exactly_ten = compare_reports(&baseline, &current_report(110)).unwrap();
    assert!(matches!(
        observed_verdict(&exactly_ten),
        ComparisonVerdict::Pass { .. }
    ));
    assert!(!comparison_fails(&exactly_ten));

    let over_ten = compare_reports(&baseline, &current_report(111)).unwrap();
    assert!(matches!(
        observed_verdict(&over_ten),
        ComparisonVerdict::Regression { .. }
    ));
    assert!(comparison_fails(&over_ten));

    let mut exact_variance = current_report(100);
    let observation = &mut exact_variance.observations[0];
    for (sample, wall) in observation
        .samples
        .iter_mut()
        .zip([990_000, 1_100_000, 1_210_000])
    {
        sample.wall_time_ns = wall;
    }
    observation.summary = Some(summarize_samples(&observation.samples).unwrap());
    let exact_variance = compare_reports(&baseline, &exact_variance).unwrap();
    assert!(matches!(
        observed_verdict(&exact_variance),
        ComparisonVerdict::Pass { .. }
    ));

    let mut high_variance = current_report(100);
    let observation = &mut high_variance.observations[0];
    for (sample, wall) in observation
        .samples
        .iter_mut()
        .zip([900_000, 1_100_000, 1_300_000])
    {
        sample.wall_time_ns = wall;
    }
    observation.summary = Some(summarize_samples(&observation.samples).unwrap());
    let high_variance = compare_reports(&baseline, &high_variance).unwrap();
    assert!(matches!(
        observed_verdict(&high_variance),
        ComparisonVerdict::Inconclusive { .. }
    ));
}

#[test]
fn comparison_refuses_host_mode_schema_and_reference_drift() {
    let baseline = report_fixture().unwrap();
    let mutations: [fn(&mut BenchmarkReport); 4] = [
        |report: &mut BenchmarkReport| {
            report.observations[0].comparability.dataset_id = "different-fixture".to_owned();
        },
        |report: &mut BenchmarkReport| {
            report.observations[0].comparability.host_class = "different-host-class".to_owned();
        },
        |report: &mut BenchmarkReport| {
            report.observations[0].comparability.io_mode = IoMode::Cold;
        },
        |report: &mut BenchmarkReport| {
            report.observations[0].reference = Some(ReferenceIdentity {
                kind: "internal".to_owned(),
                name: "raw-arrow".to_owned(),
                version: "different-version".to_owned(),
                semantic_work: "decode only".to_owned(),
            });
        },
    ];
    for mutate in mutations {
        let mut current = current_report(100);
        mutate(&mut current);
        let comparison = compare_reports(&baseline, &current).unwrap();
        assert!(matches!(
            observed_verdict(&comparison),
            ComparisonVerdict::Inconclusive { .. }
        ));
    }

    let mut byte_drift = current_report(100);
    for sample in &mut byte_drift.observations[0].samples {
        sample.physical_bytes += 1;
    }
    byte_drift.observations[0].summary =
        Some(summarize_samples(&byte_drift.observations[0].samples).unwrap());
    let comparison = compare_reports(&baseline, &byte_drift).unwrap();
    assert!(matches!(
        observed_verdict(&comparison),
        ComparisonVerdict::Inconclusive { .. }
    ));

    let mut incompatible = current_report(100);
    incompatible.schema_version += 1;
    assert!(compare_reports(&baseline, &incompatible).is_err());
}

#[test]
fn baseline_install_is_content_addressed_evidence_backed_and_preserves_history() {
    let repository = tempfile::tempdir().unwrap();
    let evidence_dir = repository.path().join(".10x/evidence");
    fs::create_dir_all(&evidence_dir).unwrap();
    fs::write(evidence_dir.join("baseline-one.md"), "recorded").unwrap();
    fs::write(evidence_dir.join("baseline-two.md"), "recorded").unwrap();
    let root = repository.path().join("baselines");
    let baseline = report_fixture().unwrap();
    let first = install_baseline(
        &root,
        repository.path(),
        &baseline,
        ".10x/evidence/baseline-one.md",
    )
    .unwrap();
    assert_eq!(first.entries.len(), 1);

    let current = current_report(105);
    let second = install_baseline(
        &root,
        repository.path(),
        &current,
        ".10x/evidence/baseline-two.md",
    )
    .unwrap();
    assert_eq!(second.entries.len(), 2);
    assert_eq!(
        second.current_report_sha256,
        canonical_sha256(&current).unwrap()
    );
    assert!(
        second
            .entries
            .iter()
            .all(|entry| root.join(&entry.report_file).is_file())
    );
    assert!(
        install_baseline(
            &root,
            repository.path(),
            &current,
            ".10x/evidence/missing.md",
        )
        .is_err()
    );
    fs::write(root.join(&second.entries[0].report_file), b"{}").unwrap();
    assert!(
        install_baseline(
            &root,
            repository.path(),
            &current,
            ".10x/evidence/baseline-two.md",
        )
        .is_err()
    );
}

#[test]
fn generated_envelope_matches_committed_golden() {
    let report: BenchmarkReport = serde_json::from_str(include_str!(
        "../../../.10x/evidence/.storage/p3-baseline-macos-ef3d84f6.json"
    ))
    .unwrap();
    let spec: EnvelopeSpec =
        serde_json::from_str(include_str!("../fixtures/p3-envelope-spec.json")).unwrap();
    let generated = generate_envelope(&report, &spec).unwrap();
    assert_eq!(
        generated,
        include_str!("../../../docs/performance-envelope.md")
    );
}
