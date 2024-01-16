"""The spec classes and custom loader for continuous data framework models"""
import os
from pathlib import Path

from ruamel import yaml
from sqlglot import exp
from sqlmesh import Config
from sqlmesh.core.loader import SqlMeshLoader
from sqlmesh.core.model import Model, create_external_model
from sqlmesh.utils import UniqueKeyDict

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


class CDFModelLoader(SqlMeshLoader):
    """Custom SQLMesh loader for cdf."""

    def __init__(self, sink: str) -> None:
        super().__init__()
        self._sink = sink
        self._mutated = False

    def _process_cdf_unmanaged(
        self,
        models: UniqueKeyDict,
        /,
        config: Config,
        path: Path,
    ) -> UniqueKeyDict[str, Model]:
        """Processes an unmanaged cdf yaml file."""
        try:
            data = YAML.load(path)
            if not data:
                return models
            # TODO: replace with pydantic validator?
            assert isinstance(data, list), "Expected a list of schemas"
        except Exception as e:
            logger.error("Failed to parse %s", path)
            raise e
        for schema in data:
            model = create_external_model(
                **schema,
                dialect=config.model_defaults.dialect,
                path=path,
                project=config.project,
                default_catalog=self._context.default_catalog,
            )
            # We do our best to avoid conflicts, but if there is any duplication
            # across managed -> unmanaged schema files -- prefer existing managed metadata
            if model.fqn in models:
                logger.warn(
                    "Duplicate external model definition %s found while parsing %s",
                    model.fqn,
                    path,
                )
                continue
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
        try:
            data = YAML.load(path)
            if not data:
                return models
            # TODO: replace with pydantic validator?
            assert isinstance(data, dict), "Expected a dict of model names to schemas"
        except Exception as e:
            logger.error("Failed to parse %s", path)
            raise e
        for name, schema in data.items():
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
        return models

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
