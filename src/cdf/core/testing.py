# pyright: reportUnknownMemberType=false
from __future__ import annotations

import logging
import typing as t
import unittest
from abc import ABC, abstractmethod
from collections.abc import Mapping
from pathlib import Path
from types import TracebackType

import pytest

from cdf.core.models import (
    DbtTestAdapterConfig,
    PytestAdapterConfig,
    TestConfig,
    UnittestAdapterConfig,
)

__all__ = ["TestAdapterBase", "PytestAdapter", "UnittestAdapter", "DbtTestAdapter"]

logger = logging.getLogger(__name__)

T = t.TypeVar("T")
TConfig = t.TypeVar("TConfig", bound=TestConfig)


@t.overload
def test_adapter_factory(
    package_path: Path, adapter_conf: PytestAdapterConfig
) -> PytestAdapter: ...


@t.overload
def test_adapter_factory(
    package_path: Path, adapter_conf: UnittestAdapterConfig
) -> UnittestAdapter: ...


@t.overload
def test_adapter_factory(
    package_path: Path, adapter_conf: DbtTestAdapterConfig
) -> DbtTestAdapter: ...


def test_adapter_factory(
    package_path: Path, adapter_conf: TestConfig
) -> TestAdapterBase[t.Any, t.Any]:
    match adapter_conf.adapter:
        case "pytest":
            return PytestAdapter(package_path, adapter_conf)
        case "unittest":
            return UnittestAdapter(package_path, adapter_conf)
        case "dbt":
            return DbtTestAdapter(package_path, adapter_conf)


class TestAdapterBase(ABC, t.Generic[T, TConfig]):
    """Abstract base class for test adapters."""

    def __init__(self, package_path: Path, adapter_conf: TConfig) -> None:
        self.package_path: Path = package_path
        self.adapter_conf: TConfig = adapter_conf

    @abstractmethod
    def discover_tests(self) -> list[t.Any]:
        """Discover available test files."""
        pass

    @abstractmethod
    def __call__(self) -> tuple[Mapping[str, T], bool]:
        """Run tests for the package."""
        pass


class PytestAdapter(TestAdapterBase[pytest.TestReport, PytestAdapterConfig]):
    """Adapter for running pytest programmatically."""

    def discover_tests(self) -> list[pytest.Item]:
        """Discover test cases in the package."""
        collected_items: list[pytest.Item] = []

        class CaseCollector:
            def pytest_collection_finish(self, session: pytest.Session) -> None:
                collected_items.extend(session.items)

        _ = pytest.main([str(self.package_path), "--collect-only"], plugins=[CaseCollector()])
        return collected_items

    def __call__(self) -> tuple[Mapping[str, pytest.TestReport], bool]:
        """Run pytest tests programmatically."""
        results: dict[str, pytest.TestReport] = {}

        class ReportCollector:
            def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
                if report.when == "call":
                    results[report.nodeid] = report

        _ = pytest.main([str(self.package_path)], plugins=[ReportCollector()])
        return results, any(report.outcome == "failed" for report in results.values())


_OptExcInfo = tuple[type[BaseException], BaseException, TracebackType] | tuple[None, None, None]


class _CollectingTestResult(unittest.TestResult):
    """A TestResult class that collects test results per test."""

    def __init__(self):
        super().__init__()
        self.test_results: dict[str, str] = {}

    def addError(self, test: unittest.TestCase, err: _OptExcInfo) -> None:
        super().addError(test, err)
        self.test_results[str(test)] = "error"

    def addFailure(self, test: unittest.TestCase, err: _OptExcInfo) -> None:
        super().addFailure(test, err)
        self.test_results[str(test)] = "failed"

    def addSkip(self, test: unittest.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self.test_results[str(test)] = "skipped"

    def addExpectedFailure(self, test: unittest.TestCase, err: _OptExcInfo) -> None:
        super().addExpectedFailure(test, err)
        self.test_results[str(test)] = "expectedFailure"

    def addUnexpectedSuccess(self, test: unittest.TestCase) -> None:
        super().addUnexpectedSuccess(test)
        self.test_results[str(test)] = "unexpectedSuccess"


class UnittestAdapter(TestAdapterBase[str, UnittestAdapterConfig]):
    """Adapter for running built-in unittest module tests."""

    def discover_tests(self) -> list[str]:
        """Discover test cases in the package."""
        loader = unittest.TestLoader()

        suite = loader.discover(start_dir=str(self.package_path))
        test_names: list[str] = []

        def _flatten_suite(suite: unittest.TestSuite) -> None:
            for test in suite:
                if isinstance(test, unittest.TestSuite):
                    _flatten_suite(test)
                else:
                    test_names.append(str(test))

        _flatten_suite(suite)
        return test_names

    def __call__(self) -> tuple[Mapping[str, str], bool]:
        """Run unittest tests programmatically."""
        loader = unittest.TestLoader()
        suite = loader.discover(start_dir=str(self.package_path))

        result = _CollectingTestResult()
        _ = suite.run(result)

        results = result.test_results
        return results, not result.wasSuccessful()


class _DbtRunResult(t.Protocol):
    """Protocol for dbt test results."""

    status: t.Any
    timing: list[t.Any]
    thread_id: str
    execution_time: float
    adapter_response: dict[str, t.Any]
    message: str | None
    failures: int | None


class DbtTestAdapter(TestAdapterBase[_DbtRunResult, DbtTestAdapterConfig]):
    """Adapter for running dbt tests."""

    def discover_tests(self) -> list[str]:
        """Discover dbt tests in the project."""
        from dbt.cli.main import dbtRunner

        runner = dbtRunner()
        args = [
            "ls",
            "--resource-type",
            "test",
            "--project-dir",
            str(self.package_path),
            "--select",
            "test_type:unit",
        ]

        logger.debug("Running dbt command: %s", " ".join(args))

        invocation_info = runner.invoke(args)
        if invocation_info.success:
            return t.cast(list[str], invocation_info.result)
        else:
            raise RuntimeError from invocation_info.exception

    def __call__(self) -> tuple[Mapping[str, _DbtRunResult], bool]:
        """Run dbt tests and collect results."""
        from dbt.artifacts.schemas.run import RunExecutionResult
        from dbt.cli.main import dbtRunner

        runner = dbtRunner()
        args = ["test", "--project-dir", str(self.package_path), "--select", "test_type:unit"]

        logger.debug("Running dbt command: %s", " ".join(args))

        invocation_info = runner.invoke(args)
        if invocation_info.exception:
            logger.error("dbt test command raised an exception: %s", invocation_info.exception)
            raise RuntimeError(f"dbt test command failed: {invocation_info.exception}")

        run_execution = t.cast(RunExecutionResult, invocation_info.result)
        logger.debug("dbt test results generated at: %s", run_execution.generated_at)

        return {r.node.unique_id: r for r in run_execution.results}, any(
            r.failures for r in run_execution.results
        )
