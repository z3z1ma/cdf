# pyright: reportUnknownMemberType=false
"""Test adapters for running pytest, unittest, and dbt tests."""

from __future__ import annotations

import logging
import typing as t
import unittest
from abc import ABC, abstractmethod
from collections.abc import Mapping
from pathlib import Path
from types import TracebackType

import pytest

import cdf.legacy.interface as I

__all__ = ["TestAdapterBase", "PytestAdapter", "UnittestAdapter", "DbtTestAdapter"]

logger = logging.getLogger(__name__)

T = t.TypeVar("T")


@t.overload
def test_adapter_factory(package_path: Path, conf: I.PytestAdapterConfig) -> PytestAdapter: ...


@t.overload
def test_adapter_factory(package_path: Path, conf: I.UnittestAdapterConfig) -> UnittestAdapter: ...


@t.overload
def test_adapter_factory(package_path: Path, conf: I.DbtTestAdapterConfig) -> DbtTestAdapter: ...


def test_adapter_factory(package_path: Path, conf: I.TestConfig) -> TestAdapterBase[t.Any]:
    """Factory function to create a test adapter based on the provided configuration.

    Args:
        package_path (Path): The path to the package containing the tests.
        conf (M.TestConfig): The configuration object specifying the adapter type and its settings.

    Returns:
        TestAdapterBase: An instance of a test adapter (PytestAdapter, UnittestAdapter, or DbtTestAdapter).

    Raises:
        ValueError: If the adapter type specified in the configuration is unknown.
    """
    match conf.adapter:
        case "pytest":
            return PytestAdapter(
                package_path,
                pytest_args=conf.pytest_args,
            )
        case "unittest":
            return UnittestAdapter(
                package_path,
                test_pattern=conf.test_pattern,
            )
        case "dbt":
            return DbtTestAdapter(
                package_path,
                project_dir=conf.project_dir,
                profiles_dir=conf.profiles_dir,
                target=conf.target,
                vars=conf.vars,
                models=conf.models,
                exclude=conf.exclude,
                threads=conf.threads,
            )
    raise ValueError(f"Unknown test adapter: {conf.adapter}")  # pyright: ignore[reportUnreachable]


class TestAdapterBase(ABC, t.Generic[T]):
    """Abstract base class for test adapters."""

    def __init__(self, package_path: Path, **kwargs: t.Any) -> None:
        self.package_path: Path = package_path

    @abstractmethod
    def discover_tests(self) -> list[t.Any]:
        """Discover available test files."""
        pass

    @abstractmethod
    def __call__(self) -> tuple[Mapping[str, T], bool]:
        """Run tests for the package."""
        pass


@t.final
class PytestAdapter(TestAdapterBase[pytest.TestReport]):
    """Adapter for running pytest programmatically."""

    def __init__(
        self,
        package_path: Path,
        pytest_args: list[str] | None = None,
    ) -> None:
        """Initialize the PytestAdapter.

        Args:
            package_path (Path): The path to the package containing the tests.
            pytest_args (list[str] | None, optional): Additional arguments to pass to pytest. Defaults to None.
        """
        super().__init__(package_path)
        self.pytest_args = pytest_args or []

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


@t.final
class UnittestAdapter(TestAdapterBase[str]):
    """Adapter for running built-in unittest module tests."""

    def __init__(
        self,
        package_path: Path,
        test_pattern: str = "test*.py",
    ) -> None:
        """Initialize the UnittestAdapter.

        Args:
            package_path (Path): The path to the package containing the tests.
            test_pattern (str, optional): The pattern to match test files. Defaults to "test*.py".
        """
        super().__init__(package_path)
        self.test_pattern = test_pattern

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
        suite = loader.discover(start_dir=str(self.package_path), pattern=self.test_pattern)

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


@t.final
class DbtTestAdapter(TestAdapterBase[_DbtRunResult]):
    """Adapter for running dbt tests."""

    def __init__(
        self,
        package_path: Path,
        project_dir: str | None = None,
        profiles_dir: str | None = None,
        target: str | None = None,
        vars: dict[str, t.Any] | None = None,
        models: list[str] | None = None,
        exclude: list[str] | None = None,
        threads: int | None = None,
    ) -> None:
        """Initialize the DbtTestAdapter.

        Args:
            package_path (Path): The path to the package containing the dbt project.
            project_dir (str | None, optional): The directory of the dbt project. Defaults to None.
            profiles_dir (str | None, optional): The directory of the dbt profiles. Defaults to None.
            target (str | None, optional): The target profile to use. Defaults to None.
            vars (dict[str, t.Any] | None, optional): Variables to pass to dbt. Defaults to None.
            models (list[str] | None, optional): Models to include in the test run. Defaults to None.
            exclude (list[str] | None, optional): Models to exclude from the test run. Defaults to None.
            threads (int | None, optional): Number of threads to use for dbt. Defaults to 1.
        """
        super().__init__(package_path)
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.target = target
        self.vars = vars or {}
        self.models = models or []
        self.exclude = exclude or []
        self.threads = threads or 1

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
