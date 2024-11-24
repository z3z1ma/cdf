# pyright: reportUnknownMemberType=false
"""Transformation adapters for dbt, Jinja templated SQL, and SqlMesh."""

from __future__ import annotations

import logging
import typing as t
from abc import ABC, abstractmethod
from collections.abc import Mapping
from pathlib import Path

import cdf.core.interface as I

if t.TYPE_CHECKING:
    from sqlmesh import Context as SQLMeshContext


__all__ = ["SqlMeshAdapter", "DbtAdapter", "JinjaSqlAdapter", "TransformationAdapterBase"]

logger = logging.getLogger(__name__)

T = t.TypeVar("T")


@t.overload
def transform_adapter_factory(
    package_path: Path, conf: I.DbtTransformAdapterConfig
) -> DbtAdapter: ...


@t.overload
def transform_adapter_factory(
    package_path: Path, conf: I.SqlMeshAdapterConfig
) -> SqlMeshAdapter: ...


@t.overload
def transform_adapter_factory(
    package_path: Path, conf: I.JinjaSqlAdapterConfig
) -> JinjaSqlAdapter: ...


def transform_adapter_factory(
    package_path: Path, conf: I.TransformConfig
) -> TransformationAdapterBase:
    """Factory function to create a transformation adapter based on the provided configuration.

    Args:
        package_path (Path): The path to the package containing the transformations.
        conf (M.TransformConfig): The configuration object specifying the adapter type and its settings.

    Returns:
        TransformationAdapterBase: An instance of a transformation adapter.

    Raises:
        ValueError: If the adapter type specified in the configuration is unknown.
    """
    match conf.adapter:
        case "sqlmesh":
            return SqlMeshAdapter(
                package_path,
                environment=conf.environment,
            )
        case "dbt":
            return DbtAdapter(
                package_path,
                project_dir=conf.project_dir,
                profiles_dir=conf.profiles_dir,
                target=conf.target,
                vars=conf.vars,
                models=conf.models,
                exclude=conf.exclude,
                threads=conf.threads,
            )
        case "jinja_sql":
            return JinjaSqlAdapter(
                package_path,
                connection_str=conf.connection_str,
                template_dir=conf.template_dir,
                variables=conf.variables,
            )
    raise ValueError(f"Unknown transform adapter: {conf.adapter}")  # pyright: ignore[reportUnreachable]


class TransformationAdapterBase(ABC):
    """Abstract base class for all transformation adapters."""

    def __init__(self, package_path: Path, **kwargs: t.Any) -> None:
        self.package_path: Path = package_path
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


@t.final
class SqlMeshAdapter(TransformationAdapterBase):
    """Adapter for SqlMesh transformations."""

    def __init__(
        self,
        package_path: Path,
        environment: str = "prod",
    ) -> None:
        """Initialize the SqlMeshAdapter.

        Args:
            package_path (Path): The path to the package containing the transformations.
            environment (str, optional): The environment in which to run the transformations. Defaults to "prod".
        """
        super().__init__(package_path)
        self.environment = environment
        self._context: SQLMeshContext | None = None

    @property
    def context(self) -> SQLMeshContext:
        """A lazy-loaded SqlMesh context."""
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


@t.final
class DbtAdapter(TransformationAdapterBase):
    """Adapter for dbt transformations."""

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


@t.final
class JinjaSqlAdapter(TransformationAdapterBase):
    """Adapter for simple Jinja templated SQL DDL/DML."""

    def __init__(
        self,
        package_path: Path,
        connection_str: str,
        template_dir: str = "sql",
        variables: dict[str, t.Any] | None = None,
    ) -> None:
        """Initialize the JinjaSqlAdapter.

        Args:
            package_path (Path): The path to the package containing the SQL templates.
            connection_str (str): The database connection string.
            template_dir (str, optional): The directory containing the SQL templates. Defaults to "sql".
            variables (dict[str, t.Any] | None, optional): Variables to pass to the SQL templates. Defaults to None.
        """
        super().__init__(package_path)
        self.connection_str = connection_str
        self.template_dir = template_dir
        self.variables = variables or {}

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

        engine = create_engine(self.connection_str)

        sql_dir = self.package_path / self.template_dir
        env = Environment(loader=FileSystemLoader(str(sql_dir)))

        transformations = self.discover_transformations()
        sorted_statements = sorted(transformations.items(), key=lambda x: x[0])

        with engine.connect() as conn:
            for name, sql_file in sorted_statements:
                logger.info("Executing SQL script: %s", name)
                template = env.get_template(str(sql_file.relative_to(sql_dir)))
                sql_content = template.render(self.variables)
                try:
                    _ = conn.execute(text(sql_content))
                    logger.info("Successfully executed '%s'", name)
                except Exception as e:
                    logger.error("Failed to execute '%s': %s", name, e)
                    raise
