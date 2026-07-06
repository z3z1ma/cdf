use std::panic::{AssertUnwindSafe, catch_unwind};

use super::*;

#[test]
fn prepared_orders_v1_matches_committed_golden_across_100_rebuilds() {
    let expected = prepared_orders_v1_expected_evidence().unwrap();

    for run in 0..100 {
        let temp = tempfile::tempdir().unwrap();
        let package_dir = temp.path().join(format!("prepared-orders-run-{run:03}"));
        build_prepared_orders_golden_package(GoldenPackageFixtureSpec::prepared_orders_v1(
            &package_dir,
        ))
        .unwrap();

        assert_verified_package_matches_golden(&package_dir, &expected)
            .unwrap_or_else(|error| panic!("run {run} golden comparison failed: {error}"));
    }
}

#[test]
fn prepared_orders_v1_expected_fixture_contains_required_evidence() {
    let expected = prepared_orders_v1_expected_evidence().unwrap();

    assert_eq!(expected.manifest_version, 1);
    assert_eq!(expected.identity_manifest_version, 1);
    assert_eq!(expected.package_status, "packaged");
    assert_eq!(expected.signature_signing_input, expected.package_hash);
    assert_eq!(expected.signature_value, None);
    assert!(expected.identity_layout.contains(&"data/".to_owned()));
    assert!(expected.identity_layout.contains(&"trace.jsonl".to_owned()));
    assert!(
        expected
            .identity_files
            .iter()
            .any(|file| file.path == "data/seg-000001.arrow"
                && file.byte_count > 0
                && file.sha256.len() == 64)
    );
    assert_eq!(expected.segments.len(), 1);
    assert_eq!(expected.segments[0].segment_id, "seg-000001");
    assert_eq!(expected.segments[0].path, "data/seg-000001.arrow");
    assert_eq!(expected.segments[0].row_count, 3);
    assert_eq!(
        expected.segments[0].byte_count,
        file(&expected, "data/seg-000001.arrow").byte_count
    );
    assert_eq!(
        expected.segments[0].sha256,
        file(&expected, "data/seg-000001.arrow").sha256
    );
}

#[test]
fn negative_self_tests_catch_corrupted_expected_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("prepared-orders-negative");
    let fixture = build_prepared_orders_golden_package(
        GoldenPackageFixtureSpec::prepared_orders_v1(&package_dir),
    )
    .unwrap();
    let expected = fixture.evidence;
    let actual = read_verified_golden_package_evidence(&package_dir).unwrap();
    assert!(compare_golden_package_evidence(&expected, &actual).is_empty());

    let mut wrong_hash = expected.clone();
    wrong_hash.package_hash = "sha256:wrong".to_owned();
    assert_compare_fails(&wrong_hash, &actual, "package hash mismatch");

    let mut missing_file = expected.clone();
    missing_file
        .identity_files
        .retain(|file| file.path != "trace.jsonl");
    assert_compare_fails(&missing_file, &actual, "extra identity file trace.jsonl");

    let mut extra_file = expected.clone();
    extra_file.identity_files.push(GoldenIdentityFileEvidence {
        path: "plan/missing.json".to_owned(),
        byte_count: 1,
        sha256: "missing".to_owned(),
    });
    assert_compare_fails(
        &extra_file,
        &actual,
        "missing identity file plan/missing.json",
    );

    let mut wrong_file_hash = expected.clone();
    file_mut(&mut wrong_file_hash, "data/seg-000001.arrow").sha256 = "bad-file-hash".to_owned();
    assert_compare_fails(
        &wrong_file_hash,
        &actual,
        "identity file data/seg-000001.arrow sha256 mismatch",
    );

    let mut wrong_file_bytes = expected.clone();
    file_mut(&mut wrong_file_bytes, "data/seg-000001.arrow").byte_count += 1;
    assert_compare_fails(
        &wrong_file_bytes,
        &actual,
        "identity file data/seg-000001.arrow byte count mismatch",
    );

    let mut wrong_segment_hash = expected.clone();
    wrong_segment_hash.segments[0].sha256 = "bad-segment-hash".to_owned();
    assert_compare_fails(
        &wrong_segment_hash,
        &actual,
        "segment seg-000001 sha256 mismatch",
    );

    let mut wrong_segment_bytes = expected.clone();
    wrong_segment_bytes.segments[0].byte_count += 1;
    assert_compare_fails(
        &wrong_segment_bytes,
        &actual,
        "segment seg-000001 byte count mismatch",
    );

    let mut wrong_segment_rows = expected.clone();
    wrong_segment_rows.segments[0].row_count += 1;
    assert_compare_fails(
        &wrong_segment_rows,
        &actual,
        "segment seg-000001 row count mismatch",
    );

    let mut wrong_status = expected.clone();
    wrong_status.package_status = "loading".to_owned();
    assert_compare_fails(&wrong_status, &actual, "package lifecycle status mismatch");

    let mut wrong_signing_input = expected.clone();
    wrong_signing_input.signature_signing_input = "sha256:other".to_owned();
    assert_compare_fails(
        &wrong_signing_input,
        &actual,
        "signature signing input mismatch",
    );

    let mut wrong_layout = expected.clone();
    wrong_layout.identity_layout.pop();
    assert_compare_fails(&wrong_layout, &actual, "identity layout mismatch");
}

#[test]
fn assert_verified_package_matches_golden_verifies_package_before_comparison() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("prepared-orders-tamper");
    let fixture = build_prepared_orders_golden_package(
        GoldenPackageFixtureSpec::prepared_orders_v1(&package_dir),
    )
    .unwrap();

    std::fs::write(package_dir.join("data").join("seg-000001.arrow"), b"tamper").unwrap();

    let error =
        assert_verified_package_matches_golden(&package_dir, &fixture.evidence).unwrap_err();
    assert!(error.to_string().contains("package verification failed"));
    assert!(error.to_string().contains("tampered identity file"));
}

fn assert_compare_fails(
    expected: &GoldenPackageEvidence,
    actual: &GoldenPackageEvidence,
    expected_message: &str,
) {
    let mismatches = compare_golden_package_evidence(expected, actual);
    assert!(
        mismatches
            .iter()
            .any(|mismatch| mismatch.contains(expected_message)),
        "missing {expected_message:?} in {mismatches:?}"
    );

    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            assert_golden_package_evidence_matches(expected, actual);
        }))
        .is_err(),
        "corrupted expected evidence passed assertion path"
    );
}

fn file<'a>(evidence: &'a GoldenPackageEvidence, path: &str) -> &'a GoldenIdentityFileEvidence {
    evidence
        .identity_files
        .iter()
        .find(|file| file.path == path)
        .expect("identity file")
}

fn file_mut<'a>(
    evidence: &'a mut GoldenPackageEvidence,
    path: &str,
) -> &'a mut GoldenIdentityFileEvidence {
    evidence
        .identity_files
        .iter_mut()
        .find(|file| file.path == path)
        .expect("identity file")
}
