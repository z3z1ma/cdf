# pyright: reportUnknownMemberType=false
"""Transformation adapters for dbt, Jinja templated SQL, and SqlMesh."""

from __future__ import annotations

import logging
import os
import shlex
import subprocess  # nosec
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


# TODO: while the entrypoint pattern here gives us maximum flexibility to truly expose the entire
# API of the underlying tools (a must for cdf) -- we should complement this with at least a single
# `run` method that tries to take the happy path for the most common use case with a given tool. So
# for sqlmesh, it is `plan --run`, for dbt it is `build`, for jinja_sql it just executes entrypoint
# This confers the benefit of having a uniform interface to fallback on when needed that can simply
# 'transform' data without needing to know the underlying tool.
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
    def get_entrypoint(self) -> str | list[str] | t.Callable[..., t.Any]:
        """Get the command with which to execute the transformation adapter."""

    def __call__(self, *args: t.Any) -> None:
        """Run the transformation adapter.

        The base command to execute is based on the implementation's `get_entrypoint` method. Given
        a string or list of strings, the command is assumed to be a shell command, otherwise it is a
        python callable. Usage would be as follows:

        cdf transform --package=salesforce ... <sqlmesh args>
        cdf transform --package=salesforce ... <dbt args>
        cdf transform --package=salesforce ... <jinja sql command> <args>

        More for my own reference, temporarily:
        cdf el --package=salesforce ... <pipeline kwargs>
        cdf extract-load --package=salesforce ... <pipeline kwargs>
        cdf test --package=salesforce ... <pytest args>

        Consider if --package flag should be top-level, IE cdf --package ... transform
        or if it should be part of the subcommands...

        Or if it is a posarg and the "project" level is signified by a sentinel value like 'project'
        cdf transform __main__ <sqlmesh args>
        cdf transform main <sqlmesh args>
        cdf transform project <sqlmesh args>
        cdf transform salesforce
        cdf transform _ <sqlmesh args>
        cdf transform . <sqlmesh args>
        """
        cmd = self.get_entrypoint()
        if isinstance(cmd, str):
            cmd = shlex.split(cmd)
        if isinstance(cmd, list):
            _ = subprocess.run([*cmd, *args])
        else:
            _ = cmd(*args)

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

    def get_entrypoint(self) -> list[str]:
        return ["sqlmesh", "-p", str(self.package_path)]


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

    def get_entrypoint(self):
        """Run dbt commands using the dbt-core Python API."""

        def entrypoint(*args: t.Any):
            from dbt.cli.main import dbtRunner, dbtRunnerResult

            dbt = dbtRunner()

            os.environ["DBT_PROJECT_DIR"] = str(self.package_path)
            os.environ["DBT_PROFILES_DIR"] = str(self.profiles_dir or os.path.expanduser("~/.dbt"))

            logger.info("Running dbt with arguments: %s", args)

            try:
                result: dbtRunnerResult = dbt.invoke(list(args))
                if not result.success:
                    raise RuntimeError from result.exception
            except Exception as e:
                logger.error("dbt run failed: %s", e)
                raise

            return result

        return entrypoint


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
        sql_dir = self.package_path / self.template_dir
        if not sql_dir.exists():
            logger.warning(
                "No '%s' directory found in package '%s'", self.template_dir, self.package_path.name
            )
            return {}
        transformations: dict[str, Path] = {}
        if sql_dir.exists():
            for sql_file in sorted(sql_dir.rglob("*.sql")):
                transformation_name = (
                    str(sql_file.relative_to(sql_dir))
                    .replace(".sql", "")
                    .replace(os.path.sep, "__")
                )
                transformations[transformation_name] = sql_file
        return transformations

    def get_entrypoint(self) -> str | list[str] | t.Callable[..., t.Any]:
        """Generate the transform command for the package"""

        def entrypoint(*args: t.Any) -> t.Any:
            from jinja2 import Environment, FileSystemLoader
            from sqlalchemy import create_engine, text

            from cdf.core.container import active_container

            _ = args  # TODO: use args to dispatch to other functions or control internal behavior
            engine = create_engine(self.connection_str)

            sql_dir = self.package_path / self.template_dir
            env = Environment(loader=FileSystemLoader(str(sql_dir)))
            container = active_container.get()

            transformations = self.discover_transformations()
            sorted_statements = sorted(transformations.items(), key=lambda x: x[0])

            with engine.connect() as conn:
                context = {
                    **self.variables,
                    "run_query": conn.exec_driver_sql,
                    "cdf": container.cfg,
                }
                for name, sql_file in sorted_statements:
                    logger.info("Executing SQL script: %s", name)
                    template = env.get_template(str(sql_file.relative_to(sql_dir)))
                    sql_content = template.render(context)
                    try:
                        _ = conn.execute(text(sql_content))
                        logger.info("Successfully executed '%s'", name)
                    except Exception as e:
                        logger.error("Failed to execute '%s': %s", name, e)
                        raise

        return entrypoint
