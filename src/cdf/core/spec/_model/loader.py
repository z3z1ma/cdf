"""The spec classes and custom loader for continuous data framework models"""
import os
import pickle
from pathlib import Path

from ruamel import yaml
from sqlglot import exp
from sqlmesh import Config
from sqlmesh import __version__ as sqlmesh_version
from sqlmesh.core.loader import SqlMeshLoader
from sqlmesh.core.macros import MacroRegistry
from sqlmesh.core.model import Model, create_external_model
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.jinja import JinjaMacroRegistry

import cdf.core.constants as c

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


class CDFModelLoader(SqlMeshLoader):
    """Custom SQLMesh loader for cdf."""

    def __init__(self, sink: str) -> None:
        super().__init__()
        self._sink = sink
        self.__mutated = False

    def _process_cdf_unmanaged(
        self,
        models: UniqueKeyDict,
        /,
        config: Config,
        path: Path,
    ) -> UniqueKeyDict[str, Model]:
        """Processes an unmanaged cdf yaml file."""
        path_key = f"{path.as_posix()}@{path.stat().st_mtime}"
        if path_key in self.__cache:
            return self.__cache[path_key]
        for schema in YAML.load(path):
            model = create_external_model(
                **schema,
                dialect=config.model_defaults.dialect,
                path=path,
                project=config.project,
                default_catalog=self._context.default_catalog,
            )
            # We do our best to avoid conflicts, but if there is any
            # duplication across schema files -- prefer managed metadata
            if model.fqn in models:
                continue
            models[model.fqn] = model
        self.__cache[path_key] = models
        self.__mutated = True
        return models

    def _process_cdf_managed(
        self,
        models: UniqueKeyDict,
        /,
        config: Config,
        path: Path,
    ) -> UniqueKeyDict[str, Model]:
        """Processes a managed cdf yaml file."""
        path_key = f"{path.as_posix()}@{path.stat().st_mtime}"
        if path_key in self.__cache:
            return self.__cache[path_key]
        for name, schema in YAML.load(path).items():
            model = create_external_model(
                name,
                columns={
                    c["name"]: DLT_TO_SQLGLOT[c.get("data_type", "unknown")]
                    for c in schema["columns"].values()
                },
                dialect=config.model_defaults.dialect,
                path=path,
                project=config.project,
                default_catalog=self._context.default_catalog,
            )
            models[model.fqn] = model
        self.__cache[path_key] = models
        self.__mutated = True
        return models

    # Overrides

    def _load_external_models(self) -> UniqueKeyDict[str, Model]:
        """Adds behavior to load cdf source models."""
        models: UniqueKeyDict = UniqueKeyDict("models")
        for context_path, config in self._context.configs.items():
            base_path = Path(context_path / c.METADATA / self._sink)
            base_path.mkdir(parents=True, exist_ok=True)
            cdf_unmanaged = base_path / c.SQLMESH_METADATA_FILE
            for path in base_path.glob(f"*.{EXT}"):
                if not os.path.getsize(path) or path.samefile(cdf_unmanaged):
                    continue
                self._track_file(path)
                models = self._process_cdf_managed(models, config, path)
            if cdf_unmanaged.exists():
                self._track_file(cdf_unmanaged)
                models = self._process_cdf_unmanaged(models, config, cdf_unmanaged)
        return models

    def _load_models(
        self, macros: MacroRegistry, jinja_macros: JinjaMacroRegistry
    ) -> UniqueKeyDict[str, Model]:
        """Adds behavior to load cdf staging models."""
        self.__cache_path = (
            self._context.path / ".cache" / f"external.{sqlmesh_version}"
        )
        self.__cache_path.parent.mkdir(parents=True, exist_ok=True)
        if self.__cache_path.exists():
            with self.__cache_path.open("rb") as cache_contents:
                self.__cache = pickle.load(cache_contents)
        else:
            self.__cache = {}
        models = super()._load_models(macros, jinja_macros)
        if self.__mutated:
            with self.__cache_path.open("wb") as cache_file:
                pickle.dump(self.__cache, cache_file)
        return models
