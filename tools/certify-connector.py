#!/usr/bin/env python3
"""Run CDF's existing connector laws and emit one machine-readable report."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field


REPORT_VERSION = 1
IDENTIFIER = re.compile(r"^[a-z][a-z0-9_-]*$")
TESTS_PASSED = re.compile(r"test result: ok\. [1-9][0-9]* passed;")


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
        "--base",
        default=os.environ.get("CDF_CONNECTOR_BASE", "origin/main"),
        help="Git revision used to classify the complete change set (default: origin/main)",
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


def changed_files(root: Path, base: str) -> tuple[str, list[str]]:
    git(root, "rev-parse", "--verify", f"{base}^{{commit}}")
    merge_base = git(root, "merge-base", base, "HEAD")
    commands = (
        ("diff", "--name-only", "--diff-filter=ACMRD", f"{merge_base}...HEAD"),
        ("diff", "--name-only", "--diff-filter=ACMRD"),
        ("diff", "--cached", "--name-only", "--diff-filter=ACMRD"),
        ("ls-files", "--others", "--exclude-standard"),
    )
    paths: set[str] = set()
    for command in commands:
        paths.update(line for line in git(root, *command).splitlines() if line)
    return merge_base, sorted(paths)


def classify_path(kind: str, connector_id: str, path: str) -> str | None:
    crate_id = connector_id.replace("_", "-")
    crate_role = "source" if kind == "source" else "dest"
    connector_root = f"crates/cdf-{crate_role}-{crate_id}/"
    if path.startswith(connector_root):
        return "connector_leaf"
    if path in {"Cargo.toml", "Cargo.lock", "deny.toml"}:
        return "manifest"
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


def connector_checks(kind: str, connector_id: str) -> list[Check]:
    checks = [
        Check("format", ("cargo", "fmt", "--all", "--", "--check"), 300),
        Check(
            "identity-specific-laws",
            ("cargo", "test", "-p", "cdf-conformance", "--locked", connector_id),
            1800,
            required_output=TESTS_PASSED,
        ),
        Check(
            "general-conformance",
            ("cargo", "nextest", "run", "-p", "cdf-conformance", "--locked"),
            7200,
        ),
    ]
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
        checks.extend(
            [
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
                    "destination-product-laws",
                    ("cargo", "test", "-p", "cdf-cli", "--locked", connector_id),
                    3600,
                    required_output=TESTS_PASSED,
                ),
                Check(
                    "destination-extension-boundaries",
                    ("cargo", "test", "-p", "cdf-conformance", "--locked", "generic_"),
                    1200,
                    required_output=TESTS_PASSED,
                ),
            ]
        )
    return checks


def certification_checks(kind: str, connector_id: str, core_impact: bool) -> list[Check]:
    checks = connector_checks(kind, connector_id)
    if core_impact:
        checks.extend(
            [
                Check(
                    "core-regression-profile",
                    (
                        "cargo",
                        "nextest",
                        "run",
                        "-p",
                        "cdf-engine",
                        "-p",
                        "cdf-runtime",
                        "-p",
                        "cdf-project",
                        "-p",
                        "cdf-cli",
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
        raise SystemExit("--id must be a lowercase ASCII connector identifier")
    root = repository_root()
    started_at = utc_now()
    try:
        merge_base, paths = changed_files(root, args.base)
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
        "change_surface": {
            "requested_base": args.base,
            "merge_base": merge_base,
            "changed_files": paths,
            "accepted_files": accepted,
            "generic_core_files": core_paths,
            "core_impact_acknowledged": args.core_impact,
        },
        "profile": profile,
        "checks": [],
    }
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

    checks = certification_checks(args.kind, args.id, args.core_impact)
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
