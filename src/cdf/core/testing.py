# pyright: reportUnknownMemberType=false
import typing as t
from abc import ABC, abstractmethod
from pathlib import Path

import pytest

from cdf.core.configuration import ConfigBox

T = t.TypeVar("T")


class TestAdapterBase(ABC, t.Generic[T]):
    """Abstract base class for test adapters."""

    def __init__(self, package_path: Path, config: ConfigBox) -> None:
        self.package_path: Path = package_path
        self.config: ConfigBox = config

    @abstractmethod
    def discover_tests(self) -> list[t.Any]:
        """Discover available test files."""
        pass

    @abstractmethod
    def run_tests(self) -> tuple[bool, dict[str, T]]:
        """Run tests for the package."""
        pass


class PytestAdapter(TestAdapterBase[pytest.TestReport]):
    """Adapter for running pytest programmatically."""

    def discover_tests(self) -> list[pytest.Item]:
        """Discover test cases in the package."""
        collected_items: list[pytest.Item] = []

        class CaseCollector:
            def pytest_collection_finish(self, session: pytest.Session) -> None:
                collected_items.extend(session.items)

        _ = pytest.main([str(self.package_path), "--collect-only"], plugins=[CaseCollector()])
        return collected_items

    def run_tests(self) -> tuple[bool, dict[str, pytest.TestReport]]:
        """Run pytest tests programmatically."""
        results: dict[str, pytest.TestReport] = {}

        class ReportCollector:
            def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
                if report.when == "call":
                    results[report.nodeid] = report

        _ = pytest.main([str(self.package_path)], plugins=[ReportCollector()])
        return not any(report.outcome == "failed" for report in results.values()), results
