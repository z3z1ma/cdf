#!/usr/bin/env python3

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("certify-connector.py")
SPEC = importlib.util.spec_from_file_location("certify_connector", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
certify = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = certify
SPEC.loader.exec_module(certify)


class ChangeSurfaceTests(unittest.TestCase):
    def test_source_leaf_catalog_fixture_and_docs_are_connector_only(self):
        paths = [
            "crates/cdf-source-nebula/src/lib.rs",
            "crates/cdf-builtin-drivers/src/lib.rs",
            "crates/cdf-conformance/run-matrix-shards.json",
            "crates/cdf-conformance/src/run_matrix/nebula_task_fixture.rs",
            "docs/connectors/nebula.md",
            ".10x/evidence/2026-07-31-nebula.md",
        ]
        accepted, core = certify.classify_changes("source", "nebula", paths)
        self.assertEqual(core, [])
        self.assertEqual([entry["path"] for entry in accepted], paths)

    def test_destination_leaf_and_catalog_fixture_are_connector_only(self):
        paths = [
            "crates/cdf-dest-quasar/src/lib.rs",
            "crates/cdf-conformance/src/destination_catalog.rs",
            "crates/cdf-conformance/src/destination_catalog/quasar.rs",
            "crates/cdf-conformance/runtime-chaos-shards.json",
        ]
        accepted, core = certify.classify_changes("destination", "quasar", paths)
        self.assertEqual(core, [])
        self.assertEqual(len(accepted), len(paths))

    def test_generic_runtime_and_orchestration_require_core_impact(self):
        paths = [
            "Cargo.toml",
            "Cargo.lock",
            "deny.toml",
            "crates/cdf-engine/src/execution.rs",
            "crates/cdf-project/src/runtime.rs",
            "crates/cdf-runtime/src/source.rs",
            "crates/cdf-cli/src/run_command.rs",
            ".github/workflows/fast-quality.yml",
        ]
        accepted, core = certify.classify_changes("source", "nebula", paths)
        self.assertEqual(accepted, [])
        self.assertEqual(core, paths)

    def test_connector_identifier_maps_underscores_to_crate_hyphens(self):
        self.assertEqual(
            certify.classify_path(
                "source", "cloud_events", "crates/cdf-source-cloud-events/src/lib.rs"
            ),
            "connector_leaf",
        )


class ProfileTests(unittest.TestCase):
    def test_source_profile_selects_matrix_and_graph_laws(self):
        checks = certify.certification_checks("source", "nebula", False, True)
        names = [check.name for check in checks]
        self.assertIn("selected-source-matrix", names)
        self.assertIn("source-extension-graph", names)
        self.assertNotIn("workspace-clippy", names)

    def test_destination_profile_selects_matrix_chaos_product_and_static_laws(self):
        checks = certify.certification_checks("destination", "quasar", False, True)
        names = [check.name for check in checks]
        self.assertIn("selected-destination-matrix", names)
        self.assertIn("destination-runtime-chaos", names)
        self.assertIn("fixture-identity-laws", names)
        self.assertIn("destination-extension-boundaries", names)

    def test_core_impact_adds_broader_checks_instead_of_bypassing_connector_laws(self):
        connector = certify.certification_checks("source", "nebula", False, True)
        core = certify.certification_checks("source", "nebula", True, True)
        self.assertEqual(core[: len(connector)], connector)
        self.assertEqual(
            [check.name for check in core[-2:]],
            ["core-regression-profile", "workspace-clippy"],
        )
        self.assertIn("--workspace", core[-2].command)

    def test_fixture_filters_are_direction_specific_and_count_exact_laws(self):
        source = certify.certification_checks("source", "nebula", False, True)[1]
        destination = certify.certification_checks("destination", "quasar", False, True)[1]
        self.assertEqual(source.command[-1], "nebula_source_inherits_")
        self.assertIs(source.required_output, certify.TWO_TESTS_PASSED)
        self.assertEqual(destination.command[-1], "injected_quasar_destination_")
        self.assertIs(destination.required_output, certify.THREE_TESTS_PASSED)


class ReportIdentityTests(unittest.TestCase):
    def test_change_set_digest_binds_head_and_worktree_contents(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tracked = root / "tracked.txt"
            tracked.write_text("one", encoding="utf-8")
            first = certify.change_set_sha256(root, "base", "head-one", ["tracked.txt"])
            tracked.write_text("two", encoding="utf-8")
            second = certify.change_set_sha256(root, "base", "head-one", ["tracked.txt"])
            third = certify.change_set_sha256(root, "base", "head-two", ["tracked.txt"])
        self.assertNotEqual(first, second)
        self.assertNotEqual(second, third)

    def test_builtin_catalog_preflight_rejects_synthetic_fixture_as_admissible(self):
        root = SCRIPT.parent.parent
        self.assertIsNone(certify.catalog_enrollment_error(root, "source", "files"))
        self.assertIn(
            "absent",
            certify.catalog_enrollment_error(root, "destination", "quasar"),
        )


if __name__ == "__main__":
    unittest.main()
