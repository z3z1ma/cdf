#!/usr/bin/env python3

import importlib.util
from pathlib import Path
import sys
import unittest


SCRIPT = Path(__file__).with_name("certify-connector.py")
SPEC = importlib.util.spec_from_file_location("certify_connector", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
certify = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = certify
SPEC.loader.exec_module(certify)


class ChangeSurfaceTests(unittest.TestCase):
    def test_source_leaf_catalog_fixture_manifest_and_docs_are_connector_only(self):
        paths = [
            "crates/cdf-source-nebula/src/lib.rs",
            "crates/cdf-builtin-drivers/src/lib.rs",
            "crates/cdf-conformance/run-matrix-shards.json",
            "crates/cdf-conformance/src/run_matrix/nebula_task_fixture.rs",
            "Cargo.lock",
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
        checks = certify.certification_checks("source", "nebula", False)
        names = [check.name for check in checks]
        self.assertIn("selected-source-matrix", names)
        self.assertIn("source-extension-graph", names)
        self.assertNotIn("workspace-clippy", names)

    def test_destination_profile_selects_matrix_chaos_product_and_static_laws(self):
        checks = certify.certification_checks("destination", "quasar", False)
        names = [check.name for check in checks]
        self.assertIn("selected-destination-matrix", names)
        self.assertIn("destination-runtime-chaos", names)
        self.assertIn("destination-product-laws", names)
        self.assertIn("destination-extension-boundaries", names)

    def test_core_impact_adds_broader_checks_instead_of_bypassing_connector_laws(self):
        connector = certify.certification_checks("source", "nebula", False)
        core = certify.certification_checks("source", "nebula", True)
        self.assertEqual(core[: len(connector)], connector)
        self.assertEqual(
            [check.name for check in core[-2:]],
            ["core-regression-profile", "workspace-clippy"],
        )


if __name__ == "__main__":
    unittest.main()
