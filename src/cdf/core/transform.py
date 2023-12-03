import fnmatch
import os
import typing as t
from dataclasses import dataclass, field
from pathlib import Path

import sqlmesh.core.constants as sqlmesh_constants
from ruamel import yaml
from sqlglot import exp, parse_one
from sqlmesh.core.loader import SqlMeshLoader
from sqlmesh.core.macros import MacroRegistry
from sqlmesh.core.model import Model, create_external_model, create_sql_model
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.jinja import JinjaMacroRegistry

YAML = yaml.YAML(typ="safe")


DLT_TO_SQLGLOT = {
    "complex": exp.DataType.build("json"),
    "text": exp.DataType.build("text"),
    "double": exp.DataType.build("double"),
    "bool": exp.DataType.build("boolean"),
    "date": exp.DataType.build("date"),
    "bigint": exp.DataType.build("bigint"),
    "binary": exp.DataType.build("binary"),
    "timestamp": exp.DataType.build("timestamp"),
    "time": exp.DataType.build("time"),
    "decimal": exp.DataType.build("decimal"),
    "wei": exp.DataType.build("numeric"),
}
"""Converts DLT data types to SQLGlot data types."""


@dataclass
class CDFStagingSpec:
    """Staging specification/DSL for cdf."""

    input: str
    """The input table."""
    prefix: str = ""
    """The prefix to apply to all columns."""
    suffix: str = ""
    """The suffix to apply to all columns."""
    excludes: t.List[str] = field(default_factory=list)
    """Columns to exclude."""
    exclude_patterns: t.List[str] = field(default_factory=list)
    """Column patterns to exclude."""
    includes: t.List[str] = field(default_factory=list)
    """Columns to include."""
    include_patterns: t.List[str] = field(default_factory=list)
    """Column patterns to include."""
    predicate: str = ""
    """The predicate to apply to the input table."""
    computed_columns: t.List[str] = field(default_factory=list)
    """Computed columns to add."""


class CDFTransformLoader(SqlMeshLoader):
    """Custom SQLMesh loader for cdf."""

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
                extension=".yaml",
            ):
                if not os.path.getsize(path):
                    continue
                self._track_file(path)
                with path.open() as f:
                    specs = YAML.load(f)
                    if not isinstance(specs, list):
                        specs = [specs]
                    data.extend((s, Path(path)) for s in specs)

            for raw_spec, path in data:
                spec = CDFStagingSpec(**raw_spec)

                input_table = parse_one(spec.input, into=exp.Table)
                candidates = sorted(
                    [
                        candidate
                        for candidate in context_path.glob(
                            f"metadata/*/{input_table.db}.yaml"
                        )
                        if candidate.is_file()
                    ],
                    key=lambda c: c.stat().st_mtime,
                    reverse=True,
                )

                meta_path = next(iter(candidates), None)

                if meta_path is None or not meta_path.exists():
                    raise Exception(
                        f"Metadata file not found: {meta_path}, run cdf metadata"
                    )

                with meta_path.open() as f:
                    meta = YAML.load(f)

                base_projection = [
                    exp.column(c).as_(f"{spec.prefix}{c}{spec.suffix}")
                    for c in meta[input_table.name]["columns"]
                    if c not in spec.excludes
                    and not any(fnmatch.fnmatch(c, p) for p in spec.exclude_patterns)
                    and (not spec.includes or c in spec.includes)
                    and (
                        not spec.include_patterns
                        or any(fnmatch.fnmatch(c, p) for p in spec.include_patterns)
                    )
                ]
                projection = [
                    *base_projection,
                    *[parse_one(c) for c in spec.computed_columns],
                ]
                select = exp.select(*projection).from_(input_table)
                if spec.predicate:
                    select = select.where(spec.predicate)

                # TODO: get audits + grain ascertained from dlt

                select.add_comments([f"Source: {path.relative_to(self._context.path)}"])
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
                models[model.name] = model

                parent_model = create_external_model(
                    name=spec.input,
                    columns={
                        c["name"]: DLT_TO_SQLGLOT[c["data_type"]]
                        for c in meta[input_table.name]["columns"].values()
                    },
                    dialect=config.model_defaults.dialect,
                    path=path,
                    project=config.project,
                )
                models[parent_model.name] = parent_model

        return models
