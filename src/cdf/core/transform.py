import itertools
import os

import sqlmesh.core.constants as sqlmesh_constants
from sqlglot import parse_one
from sqlmesh.core.loader import SqlMeshLoader
from sqlmesh.core.macros import MacroRegistry
from sqlmesh.core.model import Model, create_sql_model
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.jinja import JinjaMacroRegistry
from sqlmesh.utils.yaml import YAML


class CDFTransformLoader(SqlMeshLoader):
    def _load_models(
        self, macros: MacroRegistry, jinja_macros: JinjaMacroRegistry
    ) -> UniqueKeyDict[str, Model]:
        models = super()._load_models(macros, jinja_macros)
        # inject YAML based models
        for context_path, config in self._context.configs.items():
            for path in itertools.chain(
                *[
                    self._glob_paths(
                        context_path / sqlmesh_constants.MODELS,
                        config=config,
                        extension=ext,
                    )
                    for ext in (".yml", ".yaml")
                ]
            ):
                if not os.path.getsize(path):
                    continue
                self._track_file(path)
                with open(path, "r", encoding="utf-8") as file:
                    spec = YAML().load(file.read())

                # DSL
                # extends the idea of a centralized DRY metadata layer and is founded upon
                # the notion that a staging model is 99% of the time a copy of a source model
                # with some rules applied to it such as filtering, renaming, etc.
                # Most of these rules are repetitive and can be expressed in a declarative way
                # using a simple DSL to drastically reduce repreated code and improve
                # maintainability whilst still having the benefits of a staging layer of views.
                #
                # Case-in-point may be a Salesforce Opp table with 300+ fields, but all we
                # need to do is apply a few rules like prefixing, filtering deleted, adding
                # a few convenience columns, and excluding a few fields.
                # We can convert probably 350 lines of code into 10 lines of DSL.
                #
                # link: 'metadata/salesforce/opportunity.yml'
                # prefix: opportunity_
                # exclude: [acv_temp_c, arr_cd_legacy_c]
                # predicate: is_deleted = false
                # add_columns:
                #  - case when is_closed = true then 'closed' else 'open' end as status
                #  - current_date() as load_date

                # NOTE: these dsl files can be autoscaffolded by cdf generate-staging but we should
                # ensure we check if a model already exists for a given source table with our
                # opinionated (configurable?) naming convention (stg_<source_name>__<table_name>) and
                # if so, we should not overwrite because it indicates that the user has opted for more customization

                model = create_sql_model(
                    spec["name"],
                    parse_one("SELECT 1"),
                    path=path.absolute(),
                    module_path=context_path,
                    dialect="default",
                    macros=macros,
                    jinja_macros=jinja_macros,
                    physical_schema_override=config.physical_schema_override,
                    time_column_format=config.time_column_format,
                    project=config.project,
                )
                models[model.name] = model

        return models
