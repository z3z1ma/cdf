# pyright: reportUnknownMemberType=false
"""Transformation adapters for dbt, Jinja templated SQL, and SqlMesh."""

from __future__ import annotations

import logging
import typing as t
from abc import ABC, abstractmethod
from collections.abc import Mapping
from pathlib import Path

from cdf.core.models import (
    DbtTransformAdapterConfig,
    JinjaSqlAdapterConfig,
    SqlMeshAdapterConfig,
    TransformConfig,
)

if t.TYPE_CHECKING:
    from sqlmesh import Context as SQLMeshContext

__all__ = ["SqlMeshAdapter", "DbtAdapter", "JinjaSqlAdapter", "TransformationAdapterBase"]

logger = logging.getLogger(__name__)

T = t.TypeVar("T")
TConfig = t.TypeVar("TConfig", bound=TransformConfig)


@t.overload
def transform_adapter_factory(
    package_path: Path, adapter_conf: DbtTransformAdapterConfig
) -> DbtAdapter: ...


@t.overload
def transform_adapter_factory(
    package_path: Path, adapter_conf: SqlMeshAdapterConfig
) -> SqlMeshAdapter: ...


@t.overload
def transform_adapter_factory(
    package_path: Path, adapter_conf: JinjaSqlAdapterConfig
) -> JinjaSqlAdapter: ...


def transform_adapter_factory(
    package_path: Path, adapter_conf: TransformConfig
) -> TransformationAdapterBase[t.Any]:
    match adapter_conf.adapter:
        case "sqlmesh":
            return DbtAdapter(package_path, adapter_conf)
        case "dbt":
            return SqlMeshAdapter(package_path, adapter_conf)
        case "jinja_sql":
            return JinjaSqlAdapter(package_path, adapter_conf)
    raise ValueError(f"Unknown transform adapter: {adapter_conf.adapter}")  # pyright: ignore[reportUnreachable]


class TransformationAdapterBase(ABC, t.Generic[TConfig]):
    """Abstract base class for all transformation adapters."""

    def __init__(self, package_path: Path, adapter_conf: t.Any) -> None:
        self.package_path: Path = package_path
        self.adapter_conf: TConfig = adapter_conf
        self._transformations: Mapping[str, t.Any] = {}

    @abstractmethod
    def _discover_transformations(self) -> Mapping[str, t.Any]:
        """Discover available transformations."""
        pass

    def discover_transformations(self) -> Mapping[str, t.Any]:
        """Discover available transformations."""
        if not self._transformations:
            self._transformations = self._discover_transformations()
        return self._transformations

    @abstractmethod
    def __call__(self, **kwargs: t.Any) -> None:
        """Run a specific transformation."""
        pass

    def __getitem__(self, name: str) -> t.Any:
        return (self._transformations or self.discover_transformations())[name]

    def __getattr__(self, name: str) -> t.Any:
        try:
            return (self._transformations or self.discover_transformations())[name]
        except KeyError as e:
            raise AttributeError(f"No such transformation: {name}") from e


class SqlMeshAdapter(TransformationAdapterBase[SqlMeshAdapterConfig]):
    """Adapter for SqlMesh transformations."""

    def __init__(self, package_path: Path, config: t.Any) -> None:
        super().__init__(package_path, config)
        self._context: SQLMeshContext | None = None

    @property
    def context(self) -> SQLMeshContext:
        from sqlmesh import Context

        if not self._context:
            self._context = Context(paths=[self.package_path])
        return self._context

    def _discover_transformations(self) -> Mapping[str, t.Any]:
        """Discover SqlMesh models."""
        return self.context.models

    def __call__(self, *, environment: str = "prod", **kwargs: t.Any) -> None:
        """Run SqlMesh plan and run commands using the sqlmesh API."""
        logger.info("Running 'sqlmesh plan' in '%s'", self.package_path)
        try:
            plan_builder = self.context.plan_builder(environment)
            self.context.apply(plan_builder.build())
        except Exception as e:
            logger.error("SqlMesh plan failed: %s", e)
            raise

        logger.info("Running 'sqlmesh run' in '%s'", self.package_path)
        if not self.context.run(environment):
            logger.error("SqlMesh run failed")


class DbtAdapter(TransformationAdapterBase[DbtTransformAdapterConfig]):
    """Adapter for dbt transformations."""

    def _discover_transformations(self) -> Mapping[str, Path]:
        """Discover dbt models."""
        models_dir = self.package_path / "models"
        if not models_dir.exists():
            logger.warning("No 'models' directory found in package '%s'", self.package_path.name)
            return {}
        models = {
            model_file.stem: model_file
            for model_file in models_dir.glob("**/*.sql")
            if model_file.is_file()
        }
        return models

    def __call__(self, **kwargs: t.Any) -> None:
        """Run dbt commands using the dbt-core Python API."""
        from dbt.cli.main import dbtRunner, dbtRunnerResult

        dbt = dbtRunner()

        project_dir = str(self.package_path)
        profiles_dir = self.adapter_conf.profiles_dir or "~/.dbt"

        args_list = ["run", "--project-dir", project_dir, "--profiles-dir", profiles_dir]
        logger.info("Running dbt with arguments: %s", args_list)

        try:
            result: dbtRunnerResult = dbt.invoke(args_list)
            if not result.success:
                raise RuntimeError from result.exception
        except Exception as e:
            logger.error("dbt run failed: %s", e)
            raise


class JinjaSqlAdapter(TransformationAdapterBase[JinjaSqlAdapterConfig]):
    """Adapter for simple Jinja templated SQL DDL/DML."""

    def _discover_transformations(self) -> dict[str, Path]:
        """Discover Jinja templated SQL scripts."""
        sql_dir = self.package_path / "sql"
        if not sql_dir.exists():
            logger.warning("No 'sql' directory found in package '%s'", self.package_path.name)
            return {}
        transformations: dict[str, Path] = {}
        for subdir_name in ["ddl", "dml"]:
            subdir = sql_dir / subdir_name
            if subdir.exists():
                for sql_file in sorted(subdir.glob("*.sql")):
                    transformation_name = f"{subdir_name}/{sql_file.name}"
                    transformations[transformation_name] = sql_file
        return transformations

    def __call__(self, **kwargs: t.Any) -> None:
        """Execute Jinja templated SQL scripts."""
        from jinja2 import Environment, FileSystemLoader
        from sqlalchemy import create_engine, text

        db_config = self.adapter_conf.model_dump()
        if not db_config:
            raise ValueError("Database configuration is missing in config")

        engine = create_engine(db_config["url"])

        sql_dir = self.package_path / "sql"
        env = Environment(loader=FileSystemLoader(str(sql_dir)))

        transformations = self.discover_transformations()
        sorted_statements = sorted(transformations.items(), key=lambda x: x[0])

        with engine.connect() as conn:
            for name, sql_file in sorted_statements:
                logger.info("Executing SQL script: %s", name)
                template = env.get_template(str(sql_file.relative_to(sql_dir)))
                sql_content = template.render()
                try:
                    _ = conn.execute(text(sql_content))
                    logger.info("Successfully executed '%s'", name)
                except Exception as e:
                    logger.error("Failed to execute '%s': %s", name, e)
                    raise
