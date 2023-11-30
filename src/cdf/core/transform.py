import os
import re
import typing as t
from dataclasses import dataclass, field

import sqlmesh.core.constants as sqlmesh_constants
from ruamel import yaml
from sqlglot import exp, parse_one
from sqlmesh.core.loader import SqlMeshLoader
from sqlmesh.core.macros import MacroRegistry
from sqlmesh.core.model import Model, create_external_model, create_sql_model
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.jinja import JinjaMacroRegistry

YAML = yaml.YAML(typ="safe")


@dataclass
class CDFStagingSpec:
    input: str
    prefix: str = ""
    suffix: str = ""
    excludes: t.List[str] = field(default_factory=list)
    exclude_patterns: t.List[str] = field(default_factory=list)
    includes: t.List[str] = field(default_factory=list)
    include_patterns: t.List[str] = field(default_factory=list)
    predicate: str = ""
    computed_columns: t.List[str] = field(default_factory=list)


class CDFTransformLoader(SqlMeshLoader):
    def _load_models(
        self, macros: MacroRegistry, jinja_macros: JinjaMacroRegistry
    ) -> UniqueKeyDict[str, Model]:
        models = super()._load_models(macros, jinja_macros)

        for context_path, config in self._context.configs.items():
            for path in self._glob_paths(
                context_path / sqlmesh_constants.MODELS,
                config=config,
                extension=".yaml",
            ):
                if not os.path.getsize(path):
                    continue
                self._track_file(path)
                with path.open() as f:
                    spec = CDFStagingSpec(**YAML.load(f))

                input_table = parse_one(spec.input, into=exp.Table)
                meta_path = context_path / "metadata" / f"{input_table.db}.yaml"

                if not meta_path.exists():
                    raise Exception(
                        f"Metadata file not found: {meta_path}, run cdf metadata"
                    )

                with meta_path.open() as f:
                    meta = YAML.load(f)

                base_projection = [
                    exp.column(c).as_(f"{spec.prefix}{c}{spec.suffix}")
                    for c in meta[input_table.name]["columns"]
                    if c not in spec.excludes
                    and not any(re.match(p, c) for p in spec.exclude_patterns)
                    and (not spec.includes or c in spec.includes)
                    and (
                        not spec.include_patterns
                        or any(re.match(p, c) for p in spec.include_patterns)
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
                        c["name"]: exp.DataType.build(c["data_type"])
                        for c in meta[input_table.name]["columns"].values()
                    },
                    dialect=config.model_defaults.dialect,
                    path=path,
                    project=config.project,
                )
                models[parent_model.name] = parent_model

        return models
