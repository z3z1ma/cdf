"""The spec classes and custom loader for continuous data framework models"""
import fnmatch
import os
import typing as t
from pathlib import Path

import pydantic
import sqlmesh.core.constants as sqlmesh_constants
from dlt.common.schema.typing import TTableSchema
from ruamel import yaml
from sqlglot import exp, parse_one
from sqlmesh import Config
from sqlmesh.core.loader import SqlMeshLoader
from sqlmesh.core.macros import MacroRegistry
from sqlmesh.core.model import Model, create_external_model, create_sql_model
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.jinja import JinjaMacroRegistry

import cdf.core.constants as c
import cdf.core.logger as logger

YAML = yaml.YAML(typ="rt")
EXT = "yaml"


class _DataTypeDict(dict):
    """A mapping of data types which fallsback to unknown."""

    def __missing__(self, _) -> exp.DataType:
        return exp.DataType.build("unknown")


DLT_TO_SQLGLOT = _DataTypeDict(
    {
        "complex": exp.DataType.build("json"),
        "text": exp.DataType.build("text"),
        "double": exp.DataType.build("double"),
        "bool": exp.DataType.build("boolean"),
        "date": exp.DataType.build("date"),
        "bigint": exp.DataType.build("bigint"),
        "binary": exp.DataType.build("binary"),
        "timestamp": exp.DataType.build("timestamptz"),
        "time": exp.DataType.build("time"),
        "decimal": exp.DataType.build("decimal"),
        "wei": exp.DataType.build("numeric"),
    }
)
"""Converts DLT data types to SQLGlot data types."""


class CDFStagingSpecification(pydantic.BaseModel):
    """Staging specification/DSL for cdf."""

    input: str
    """The input table."""
    prefix: str = ""
    """The prefix to apply to all columns."""
    suffix: str = ""
    """The suffix to apply to all columns."""
    excludes: t.List[str] = []
    """Columns to exclude."""
    exclude_patterns: t.List[str] = []
    """Column patterns to exclude."""
    includes: t.List[str] = []
    """Columns to include."""
    include_patterns: t.List[str] = []
    """Column patterns to include."""
    predicate: str = ""
    """The predicate to apply to the input table."""
    computed_columns: t.List[str] = []
    """Computed columns to add."""

    def to_query(self, cdf_metadata: t.Dict[str, TTableSchema]) -> exp.Select:
        """Converts the staging specification to a query by applying rules.

        Args:
            cdf_metadata (t.Dict[str, TTableSchema]): Metadata from a cdf yaml file.

        Raises:
            ValueError: If no columns are selected.

        Returns:
            exp.Select: The query.
        """
        input_table = parse_one(self.input, into=exp.Table)
        base_projection = [
            exp.column(c).as_(f"{self.prefix}{c}{self.suffix}")
            for c in cdf_metadata[input_table.sql()].get("columns", [])
            if c not in self.excludes
            and not any(fnmatch.fnmatch(c, p) for p in self.exclude_patterns)
            and (not self.includes or c in self.includes)
            and (
                not self.include_patterns
                or any(fnmatch.fnmatch(c, p) for p in self.include_patterns)
            )
        ]
        projection = [
            *base_projection,
            *[parse_one(c) for c in self.computed_columns],
        ]
        if not projection:
            raise ValueError(f"No columns selected when staging {input_table.sql()}")
        select = exp.select(*projection).from_(input_table)
        if self.predicate:
            select = select.where(self.predicate)

        return select


class CDFModelLoader(SqlMeshLoader):
    """Custom SQLMesh loader for cdf."""

    def __init__(self, sink: str) -> None:
        super().__init__()
        self._sink = sink

    def _process_cdf_unmanaged(
        self,
        models: UniqueKeyDict,
        /,
        config: Config,
        path: Path,
    ) -> UniqueKeyDict[str, Model]:
        """Processes an unmanaged cdf yaml file."""
        for schema in YAML.load(path):
            model = create_external_model(
                **schema,
                dialect=config.model_defaults.dialect,
                path=path,
                project=config.project,
                default_catalog=self._context.default_catalog,
            )
            models[model.fqn] = model
        return models

    def _process_cdf_managed(
        self,
        models: UniqueKeyDict,
        /,
        config: Config,
        path: Path,
    ) -> UniqueKeyDict[str, Model]:
        """Processes a managed cdf yaml file."""
        for name, meta in YAML.load(path).items():
            model = create_external_model(
                name,
                columns={
                    c["name"]: DLT_TO_SQLGLOT[c.get("data_type", "unknown")]
                    for c in meta["columns"].values()
                },
                dialect=config.model_defaults.dialect,
                path=path,
                project=config.project,
                default_catalog=self._context.default_catalog,
            )
            models[model.fqn] = model
        return models

    # Overrides

    def _load_external_models(self) -> UniqueKeyDict[str, Model]:
        """Adds behavior to load cdf source models."""
        models: UniqueKeyDict = UniqueKeyDict("models")
        for context_path, config in self._context.configs.items():
            base_path = Path(context_path / c.METADATA / self._sink)
            base_path.mkdir(parents=True, exist_ok=True)
            cdf_unmanaged = base_path / c.SQLMESH_METADATA_FILE
            cdf_unmanaged.touch()
            for path in base_path.glob(f"*.{EXT}"):
                if not os.path.getsize(path):
                    continue
                self._track_file(path)
                if path.samefile(cdf_unmanaged):
                    models = self._process_cdf_unmanaged(models, config, path)
                else:
                    models = self._process_cdf_managed(models, config, path)
        return models

    def _load_models(
        self, macros: MacroRegistry, jinja_macros: JinjaMacroRegistry
    ) -> UniqueKeyDict[str, Model]:
        """Adds behavior to load cdf staging models."""
        models = super()._load_models(macros, jinja_macros)

        for context_path, config in self._context.configs.items():
            data = []
            for path in self._glob_paths(
                context_path / sqlmesh_constants.MODELS,
                config=config,
                extension=f".{EXT}",
            ):
                if not os.path.getsize(path):
                    continue
                with path.open() as f:
                    specs = YAML.load(f)
                    if not isinstance(specs, list):
                        specs = [specs]
                    data.extend((s, Path(path)) for s in specs)

            for raw_spec, path in data:
                try:
                    staging_spec = CDFStagingSpecification.model_validate(raw_spec)
                except pydantic.ValidationError as e:
                    logger.warning("Invalid staging spec %s: %s", path, e)
                    continue

                input_table = parse_one(staging_spec.input, into=exp.Table)
                meta_path = (
                    context_path / c.METADATA / self._sink / f"{input_table.db}.{EXT}"
                )

                if not meta_path.exists():
                    logger.warning("Missing metadata file %s", meta_path)
                    continue

                with meta_path.open() as f:
                    cdf_metadata = YAML.load(f)

                select = staging_spec.to_query(cdf_metadata)
                select.add_comments(
                    [f"Source: {meta_path.relative_to(self._context.path)}"]
                )

                # TODO: get audits + grain ascertained from dlt in add to model

                model = create_sql_model(
                    f"cdf_staging.stg_{input_table.db}__{input_table.name}",
                    select,
                    path=path.absolute(),
                    module_path=context_path,
                    dialect=config.dialect,
                    macros=macros,
                    jinja_macros=jinja_macros,
                    physical_schema_override=config.physical_schema_override,
                    time_column_format=config.time_column_format,
                    project=config.project,
                )
                models[getattr(model, "fqn", model.name)] = model

        return models
