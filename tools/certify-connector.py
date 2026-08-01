#!/usr/bin/env python3
"""Run CDF's existing connector laws and emit one machine-readable report."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field


REPORT_VERSION = 2
INTEGRATION_BASE = "origin/main"
IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
TESTS_PASSED = re.compile(r"test result: ok\. [1-9][0-9]* passed;")
TWO_TESTS_PASSED = re.compile(r"test result: ok\. 2 passed;")
THREE_TESTS_PASSED = re.compile(r"test result: ok\. 3 passed;")


@dataclass(frozen=True)
class Check:
    name: str
    command: tuple[str, ...]
    timeout_seconds: int
    environment: dict[str, str] = field(default_factory=dict)
    required_output: re.Pattern[str] | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Certify one CDF source or destination through existing repository laws."
    )
    parser.add_argument("--kind", choices=("source", "destination"), required=True)
    parser.add_argument("--id", required=True, help="conformance catalog identity")
    parser.add_argument(
        "--fixture",
        action="store_true",
        help="exercise Nebula/Quasar synthetic laws and emit a non-admissible proof report",
    )
    parser.add_argument(
        "--core-impact",
        action="store_true",
        help="acknowledge generic-core edits and activate the broader core profile",
    )
    parser.add_argument("--report", type=Path, help="also write the JSON report to this path")
    return parser.parse_args()


def repository_root() -> Path:
    return Path(__file__).resolve().parent.parent


def git(root: Path, *arguments: str) -> str:
    result = subprocess.run(
        ("git", *arguments),
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "unknown git failure"
        raise RuntimeError(f"git {' '.join(arguments)} failed: {detail}")
    return result.stdout.strip()


def change_set_sha256(root: Path, merge_base: str, head: str, paths: list[str]) -> str:
    digest = hashlib.sha256()
    for label, value in (("merge_base", merge_base), ("head", head)):
        digest.update(label.encode())
        digest.update(b"\0")
        digest.update(value.encode())
        digest.update(b"\0")
    for path in paths:
        digest.update(path.encode())
        digest.update(b"\0")
        candidate = root / path
        if candidate.is_symlink():
            digest.update(b"symlink\0")
            digest.update(os.readlink(candidate).encode())
        elif candidate.is_file():
            digest.update(b"file\0")
            digest.update(str(candidate.stat().st_mode & 0o7777).encode())
            digest.update(b"\0")
            with candidate.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
        else:
            digest.update(b"deleted\0")
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


def changed_files(root: Path) -> tuple[str, str, list[str], str]:
    git(root, "rev-parse", "--verify", f"{INTEGRATION_BASE}^{{commit}}")
    head = git(root, "rev-parse", "HEAD")
    merge_base = git(root, "merge-base", INTEGRATION_BASE, "HEAD")
    commands = (
        ("diff", "--name-only", "--diff-filter=ACMRD", f"{merge_base}...HEAD"),
        ("diff", "--name-only", "--diff-filter=ACMRD"),
        ("diff", "--cached", "--name-only", "--diff-filter=ACMRD"),
        ("ls-files", "--others", "--exclude-standard"),
    )
    paths: set[str] = set()
    for command in commands:
        paths.update(line for line in git(root, *command).splitlines() if line)
    sorted_paths = sorted(paths)
    return merge_base, head, sorted_paths, change_set_sha256(
        root, merge_base, head, sorted_paths
    )


def classify_path(kind: str, connector_id: str, path: str) -> str | None:
    crate_id = connector_id.replace("_", "-")
    crate_role = "source" if kind == "source" else "dest"
    connector_root = f"crates/cdf-{crate_role}-{crate_id}/"
    if path.startswith(connector_root):
        return "connector_leaf"
    if path.startswith("docs/"):
        return "documentation"
    if path.startswith((".10x/tickets/", ".10x/evidence/")):
        return "execution_record"
    if path in {
        "crates/cdf-builtin-drivers/Cargo.toml",
        "crates/cdf-builtin-drivers/src/lib.rs",
        "crates/cdf-builtin-drivers/fixtures/catalog.json",
        "crates/cdf-conformance/Cargo.toml",
    }:
        return "catalog"
    if kind == "source":
        if path in {
            "crates/cdf-conformance/run-matrix-shards.json",
            "crates/cdf-conformance/src/run_matrix/source_catalog.rs",
        }:
            return "fixture_catalog"
        fixture_root = "crates/cdf-conformance/src/run_matrix/"
        if path.startswith(fixture_root):
            filename = path.removeprefix(fixture_root)
            if "/" not in filename and (
                filename == f"{connector_id}.rs"
                or filename.startswith(f"{connector_id}_")
            ):
                return "connector_fixture"
    else:
        if path in {
            "crates/cdf-conformance/runtime-chaos-shards.json",
            "crates/cdf-conformance/src/destination_catalog.rs",
        }:
            return "fixture_catalog"
        if path == f"crates/cdf-conformance/src/destination_catalog/{connector_id}.rs":
            return "connector_fixture"
    return None


def classify_changes(
    kind: str, connector_id: str, paths: list[str]
) -> tuple[list[dict[str, str]], list[str]]:
    accepted: list[dict[str, str]] = []
    core_paths: list[str] = []
    for path in paths:
        category = classify_path(kind, connector_id, path)
        if category is None:
            core_paths.append(path)
        else:
            accepted.append({"path": path, "category": category})
    return accepted, core_paths


def connector_checks(kind: str, connector_id: str, fixture: bool) -> list[Check]:
    checks = [Check("format", ("cargo", "fmt", "--all", "--", "--check"), 300)]
    if fixture:
        fixture_filter = (
            "nebula_source_inherits_"
            if kind == "source"
            else "injected_quasar_destination_"
        )
        checks.append(
            Check(
                "fixture-identity-laws",
                (
                    "cargo",
                    "test",
                    "-p",
                    "cdf-conformance" if kind == "source" else "cdf-cli",
                    "--locked",
                    fixture_filter,
                ),
                1800 if kind == "source" else 3600,
                required_output=TWO_TESTS_PASSED if kind == "source" else THREE_TESTS_PASSED,
            )
        )
    else:
        crate_role = "source" if kind == "source" else "dest"
        checks.extend(
            [
                Check(
                    "connector-leaf-laws",
                    (
                        "cargo",
                        "test",
                        "-p",
                        f"cdf-{crate_role}-{connector_id.replace('_', '-')}",
                        "--locked",
                    ),
                    3600,
                    required_output=TESTS_PASSED,
                ),
                Check(
                    "builtin-catalog-integrity",
                    (
                        "cargo",
                        "test",
                        "-p",
                        "cdf-builtin-drivers",
                        "--locked",
                        "tests::catalog_matches_the_data_driven_first_party_fixture",
                        "--",
                        "--exact",
                    ),
                    1800,
                    required_output=TESTS_PASSED,
                ),
            ]
        )
    checks.append(
        Check(
            "general-conformance",
            ("cargo", "nextest", "run", "-p", "cdf-conformance", "--locked"),
            7200,
        )
    )
    if kind == "source":
        checks.extend(
            [
                Check(
                    "selected-source-matrix",
                    (
                        "cargo",
                        "test",
                        "-p",
                        "cdf-conformance",
                        "run_matrix::tests::registered_source_shard_cells_persist_output",
                        "--locked",
                        "--",
                        "--ignored",
                        "--exact",
                        "--nocapture",
                        "--test-threads=1",
                    ),
                    2700,
                    {"CDF_RUN_MATRIX_SOURCE": connector_id},
                ),
                Check(
                    "source-extension-graph",
                    (
                        "cargo",
                        "test",
                        "-p",
                        "cdf-runtime",
                        "--test",
                        "build_graph",
                        "--locked",
                        "generic_source_compiler_graphs_exclude_concrete_drivers",
                    ),
                    1200,
                    required_output=TESTS_PASSED,
                ),
            ]
        )
    else:
        destination_checks = [
                Check(
                    "selected-destination-matrix",
                    (
                        "cargo",
                        "test",
                        "-p",
                        "cdf-conformance",
                        "run_matrix::tests::registered_destination_shard_cells_persist_output",
                        "--locked",
                        "--",
                        "--ignored",
                        "--exact",
                        "--nocapture",
                        "--test-threads=1",
                    ),
                    2700,
                    {"CDF_RUN_MATRIX_DESTINATION": connector_id},
                ),
                Check(
                    "destination-runtime-chaos",
                    (
                        "cargo",
                        "test",
                        "-p",
                        "cdf-conformance",
                        "runtime_chaos::tests::registered_destination_shard_runtime_stage_chaos_persists_output",
                        "--locked",
                        "--",
                        "--ignored",
                        "--exact",
                        "--nocapture",
                        "--test-threads=1",
                    ),
                    2700,
                    {"CDF_RUNTIME_CHAOS_DESTINATION": connector_id},
                ),
                Check(
                    "destination-extension-boundaries",
                    ("cargo", "test", "-p", "cdf-conformance", "--locked", "generic_"),
                    1200,
                    required_output=TESTS_PASSED,
                ),
            ]
        if not fixture:
            destination_checks.insert(
                2,
                Check(
                    "destination-product-laws",
                    ("cargo", "nextest", "run", "-p", "cdf-cli", "--locked"),
                    7200,
                ),
            )
        checks.extend(destination_checks)
    return checks


def certification_checks(
    kind: str, connector_id: str, core_impact: bool, fixture: bool = False
) -> list[Check]:
    checks = connector_checks(kind, connector_id, fixture)
    if core_impact:
        checks.extend(
            [
                Check(
                    "core-regression-profile",
                    (
                        "cargo",
                        "nextest",
                        "run",
                        "--workspace",
                        "--locked",
                    ),
                    7200,
                ),
                Check(
                    "workspace-clippy",
                    (
                        "cargo",
                        "clippy",
                        "--workspace",
                        "--all-targets",
                        "--all-features",
                        "--locked",
                        "--",
                        "-D",
                        "warnings",
                    ),
                    5400,
                ),
            ]
        )
    return checks


def catalog_enrollment_error(root: Path, kind: str, connector_id: str) -> str | None:
    catalog = json.loads(
        (root / "crates/cdf-builtin-drivers/fixtures/catalog.json").read_text(encoding="utf-8")
    )
    section = "sources" if kind == "source" else "destinations"
    identities = {entry["id"] for entry in catalog[section]}
    if connector_id not in identities:
        return f"{kind} `{connector_id}` is absent from the shipped built-in catalog fixture"
    return None


def command_environment(root: Path) -> dict[str, str]:
    environment = os.environ.copy()
    environment["DUCKDB_DOWNLOAD_LIB"] = "1"
    host = subprocess.run(
        ("rustc", "-vV"), capture_output=True, text=True, check=False
    ).stdout
    host_match = re.search(r"^host: (.+)$", host, re.MULTILINE)
    if host_match:
        candidates = sorted(
            (root / "target" / "duckdb-download" / host_match.group(1)).glob(
                "*/libduckdb.*"
            )
        )
        if candidates:
            library_dir = str(candidates[-1].parent)
            for name in ("LIBRARY_PATH", "LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH"):
                existing = environment.get(name)
                environment[name] = (
                    f"{library_dir}{os.pathsep}{existing}" if existing else library_dir
                )
    dependency_dir = str(root / "target" / "debug" / "deps")
    existing_dyld = environment.get("DYLD_LIBRARY_PATH")
    environment["DYLD_LIBRARY_PATH"] = (
        f"{dependency_dir}{os.pathsep}{existing_dyld}"
        if existing_dyld
        else dependency_dir
    )
    return environment


def run_check(root: Path, base_environment: dict[str, str], check: Check) -> dict[str, object]:
    environment = base_environment.copy()
    environment.update(check.environment)
    started = time.monotonic()
    print(f"CDF_CONNECTOR_CHECK_START={check.name}", file=sys.stderr, flush=True)
    try:
        result = subprocess.run(
            check.command,
            cwd=root,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=check.timeout_seconds,
        )
        combined = result.stdout + result.stderr
        if combined:
            sys.stderr.write(combined)
            if not combined.endswith("\n"):
                sys.stderr.write("\n")
        output_requirement_met = (
            check.required_output is None or check.required_output.search(combined) is not None
        )
        passed = result.returncode == 0 and output_requirement_met
        detail = None
        if result.returncode == 0 and not output_requirement_met:
            detail = "command ran zero identity-specific tests"
        return_code = result.returncode
    except subprocess.TimeoutExpired as error:
        for output in (error.stdout, error.stderr):
            if output:
                sys.stderr.write(output if isinstance(output, str) else output.decode())
        passed = False
        detail = f"timed out after {check.timeout_seconds} seconds"
        return_code = None
    duration_ms = round((time.monotonic() - started) * 1000)
    marker = "PASS" if passed else "FAIL"
    print(f"CDF_CONNECTOR_CHECK_{marker}={check.name}", file=sys.stderr, flush=True)
    return {
        "name": check.name,
        "command": shlex.join(check.command),
        "environment": dict(sorted(check.environment.items())),
        "timeout_seconds": check.timeout_seconds,
        "duration_ms": duration_ms,
        "return_code": return_code,
        "status": "passed" if passed else "failed",
        "detail": detail,
    }


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def emit_report(report: dict[str, object], path: Path | None) -> None:
    serialized = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(serialized, encoding="utf-8")
    sys.stdout.write(serialized)


def main() -> int:
    args = parse_args()
    if not IDENTIFIER.fullmatch(args.id):
        raise SystemExit("--id must use lowercase ASCII letters, digits, and underscores")
    expected_fixture = ("source", "nebula") if args.kind == "source" else (
        "destination",
        "quasar",
    )
    if args.fixture and (args.kind, args.id) != expected_fixture:
        raise SystemExit("--fixture is limited to source nebula or destination quasar")
    root = repository_root()
    started_at = utc_now()
    try:
        merge_base, head, paths, change_digest = changed_files(root)
    except RuntimeError as error:
        report = {
            "version": REPORT_VERSION,
            "started_at": started_at,
            "finished_at": utc_now(),
            "connector": {"kind": args.kind, "id": args.id},
            "verdict": "failed",
            "error": str(error),
            "checks": [],
        }
        emit_report(report, args.report)
        return 2

    accepted, core_paths = classify_changes(args.kind, args.id, paths)
    profile = "core-impact" if args.core_impact else "connector-only"
    report: dict[str, object] = {
        "version": REPORT_VERSION,
        "started_at": started_at,
        "connector": {"kind": args.kind, "id": args.id},
        "admissible": not args.fixture,
        "fixture_proof": args.fixture,
        "change_surface": {
            "integration_base": INTEGRATION_BASE,
            "merge_base": merge_base,
            "head": head,
            "change_set_sha256": change_digest,
            "changed_files": paths,
            "accepted_files": accepted,
            "generic_core_files": core_paths,
            "core_impact_acknowledged": args.core_impact,
        },
        "profile": profile,
        "checks": [],
    }
    enrollment_error = None if args.fixture else catalog_enrollment_error(root, args.kind, args.id)
    if enrollment_error is not None:
        report.update(
            {
                "finished_at": utc_now(),
                "verdict": "failed",
                "error": enrollment_error,
            }
        )
        emit_report(report, args.report)
        return 2
    if core_paths and not args.core_impact:
        report.update(
            {
                "finished_at": utc_now(),
                "verdict": "failed",
                "error": (
                    "generic core ownership changed without acknowledgement; rerun with "
                    "--core-impact to activate, not bypass, the broader profile"
                ),
            }
        )
        emit_report(report, args.report)
        return 2

    checks = certification_checks(args.kind, args.id, args.core_impact, args.fixture)
    environment = command_environment(root)
    check_reports: list[dict[str, object]] = []
    for check in checks:
        check_report = run_check(root, environment, check)
        check_reports.append(check_report)
        if check_report["status"] != "passed":
            break
    report["checks"] = check_reports
    passed = len(check_reports) == len(checks) and all(
        check["status"] == "passed" for check in check_reports
    )
    report["finished_at"] = utc_now()
    report["verdict"] = "passed" if passed else "failed"
    emit_report(report, args.report)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
