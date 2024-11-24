"""Defines state storage interfaces for persisting JSON objects"""

from __future__ import annotations

import atexit
import os
import threading
import typing as t
from collections.abc import Iterator, MutableMapping
from pathlib import Path

import sqlalchemy
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

import cdf.core.interface as I
from cdf.commons.file import json

__all__ = ["FileStateBackend", "SqlAlchemyStateBackend", "state_backend_factory"]

JSON = str | int | float | bool | None | dict[str, "JSON"] | list["JSON"]


@t.overload
def state_backend_factory(
    package_path: Path, conf: I.FileStateBackendConfig
) -> FileStateBackend: ...


@t.overload
def state_backend_factory(
    package_path: Path, conf: I.SqlAlchemyStateBackendConfig
) -> SqlAlchemyStateBackend: ...


def state_backend_factory(package_path: Path, conf: I.StateBackendConfig) -> StateBackend:
    """Factory function to create a state backend instance based on the provided configuration.

    Args:
        package_path: The path to the package directory.
        backend_conf: The configuration for the state backend.

    Returns:
        StateBackend: An instance of FileStateBackend or SqlAlchemyStateBackend based on the adapter specified in the configuration.

    Raises:
        ValueError: If the adapter specified in the configuration is invalid.
    """
    match conf.adapter:
        case "file":
            if not Path(conf.file_path).is_absolute():
                conf.file_path = package_path / conf.file_path
            return FileStateBackend(**conf.model_dump(exclude={"adapter"}, exclude_none=True))
        case "sqlalchemy":
            return SqlAlchemyStateBackend(**conf.model_dump(exclude={"adapter"}, exclude_none=True))
    raise ValueError(f"Invalid state backend adapter: {conf.adapter}")  # pyright: ignore[reportUnreachable]


def _dumper(obj: JSON) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


class SqlAlchemyStateBackend(MutableMapping[str, JSON]):
    """Store JSON objects persistently using SQLAlchemy with dict-like interface"""

    def __init__(
        self,
        table_name: str,
        schema_name: str | None = None,
        *,
        connection_str: str,
        dumper: t.Callable[[JSON], str] = _dumper,
        loader: t.Callable[[str], JSON] = json.loads,
    ) -> None:
        """Initialize the storage interface creating the target table if needed

        Args:
            table_name: The name of the target table to store objects in
            schema_name: The name of the target schema to store objects in
            connection_str: The database URI to connect to
            dumper: A function to serialize a python object to str
            loader: A function to deserialize str to python object
        """
        self._engine: sqlalchemy.Engine = sqlalchemy.create_engine(connection_str)
        self._metadata: sqlalchemy.MetaData = sqlalchemy.MetaData(schema=schema_name)
        self._table: sqlalchemy.Table = sqlalchemy.Table(
            table_name,
            self._metadata,
            sqlalchemy.Column("key", sqlalchemy.String, primary_key=True),
            sqlalchemy.Column("value", sqlalchemy.Text),
            extend_existing=True,
        )
        self._metadata.create_all(self._engine)
        self._Session: sessionmaker[Session] = sessionmaker(bind=self._engine)
        self._dumper: t.Callable[[JSON], str] = dumper
        self._loader: t.Callable[[str], JSON] = loader

    def __getitem__(self, key: str) -> JSON:
        """Get the JSON object stored under the given key

        Args:
            key: The key to look up the JSON object with

        Returns:
            JSON: The JSON object stored under the given key
        """
        session = self._Session()
        try:
            stmt = sqlalchemy.select(self._table.c.value).where(self._table.c.key == key)
            result = session.execute(stmt).fetchone()
            if result is None:
                raise KeyError(key)
            return self._loader(result[0])
        finally:
            session.close()

    def __setitem__(self, key: str, value: JSON) -> None:
        """Set the JSON object under the given key, updating if it already exists

        Args:
            key: The key to store the JSON object under
            value: The JSON object to store
        """
        session = self._Session()
        try:
            stmt = insert(self._table).values(key=key, value=self._dumper(value))
            stmt = stmt.on_conflict_do_update(
                index_elements=["key"],
                set_={"value": self._dumper(value)},
            )
            _ = session.execute(stmt)
            session.commit()
        finally:
            session.close()

    def __delitem__(self, key: str) -> None:
        """Delete the JSON object stored under the given key

        Args:
            key: The key to delete the JSON object for

        Raises:
            KeyError: If the key does not exist
        """
        session = self._Session()
        try:
            stmt = self._table.delete().where(self._table.c.key == key)
            result = session.execute(stmt)
            if result.rowcount == 0:
                raise KeyError(key)
            session.commit()
        finally:
            session.close()

    def __iter__(self) -> Iterator[str]:
        """Iterate over the keys stored in the database

        Returns:
            Iterator[str]: An iterator over the keys stored in the database
        """
        session = self._Session()
        try:
            stmt = sqlalchemy.select(self._table.c.key)
            result = session.execute(stmt)
            for row in result:
                yield row[0]
        finally:
            session.close()

    def __len__(self) -> int:
        """Return the number of JSON objects stored in the database

        Returns:
            int: The number of JSON objects stored in the database
        """
        session = self._Session()
        try:
            stmt = sqlalchemy.select(sqlalchemy.func.count()).select_from(self._table)
            result = session.execute(stmt).scalar()
            return result or 0
        finally:
            session.close()

    def scope(self, namespace: str) -> ScopedMapping:
        """Scope the mapping to a namespace"""
        return ScopedMapping(self, namespace)


class FileStateBackend(MutableMapping[str, JSON]):
    """Store JSON objects persistently in a local file with dict-like interface"""

    def __init__(
        self,
        file_path: Path | str,
        *,
        dumper: t.Callable[[JSON], str] = _dumper,
        loader: t.Callable[[str], JSON] = json.loads,
        buffered: bool = False,
    ) -> None:
        """Initialize the storage interface loading data from the file if it exists

        Args:
            file_path: The path to the file to store the data
            dumper: A function to serialize a python object to str
            loader: A function to deserialize str to python object
            buffered: If True, buffer writes and write to file on exit
        """
        self._file_path: Path = Path(file_path).resolve()
        self._buffered: bool = buffered
        self._lock: threading.Lock = threading.Lock()
        self._data: dict[str, JSON] = {}
        self._dumper: t.Callable[[JSON], str] = dumper
        self._loader: t.Callable[[str], JSON] = loader
        if self._file_path.exists():
            self._data = t.cast(dict[str, JSON], self._loader(self._file_path.read_text()))
        if buffered:
            _ = atexit.register(self._flush)

    def _flush(self) -> None:
        """Flush the buffered data to the file"""
        with self._lock:
            self._write_data()

    def _write_data(self) -> None:
        """Write data to a temporary file and replace the target file"""
        temp_file_path = str(self._file_path) + ".tmp"
        with open(temp_file_path, "w") as f:
            _ = f.write(self._dumper(self._data))
        os.replace(temp_file_path, self._file_path)

    def __getitem__(self, key: str) -> JSON:
        """Get the JSON object stored under the given key

        Args:
            key: The key to look up the JSON object with

        Returns:
            JSON: The JSON object stored under the given key

        Raises:
            KeyError: If the key does not exist
        """
        try:
            return self._data[key]
        except KeyError:
            raise KeyError(key)

    def __setitem__(self, key: str, value: JSON) -> None:
        """Set the JSON object under the given key, updating if it already exists

        Args:
            key: The key to store the JSON object under
            value: The JSON object to store
        """
        self._data[key] = value
        if not self._buffered:
            self._flush()

    def __delitem__(self, key: str) -> None:
        """Delete the JSON object stored under the given key

        Args:
            key: The key to delete the JSON object for

        Raises:
            KeyError: If the key does not exist
        """
        try:
            del self._data[key]
        except KeyError:
            raise KeyError(key)
        if not self._buffered:
            self._flush()

    def __iter__(self) -> Iterator[str]:
        """Iterate over the keys stored in the file

        Returns:
            Iterator[str]: An iterator over the keys stored in the file
        """
        return iter(self._data.copy())

    def __len__(self) -> int:
        """Return the number of JSON objects stored in the file

        Returns:
            int: The number of JSON objects stored in the file
        """
        return len(self._data)

    def scope(self, namespace: str) -> ScopedMapping:
        """Scope the mapping to a namespace"""
        return ScopedMapping(self, namespace)


StateBackend = FileStateBackend | SqlAlchemyStateBackend


class ScopedMapping(MutableMapping[str, JSON]):
    """A mapping that prefixes all keys with a namespace and a delimiter"""

    def __init__(
        self, mapping: MutableMapping[str, JSON], namespace: str, delimiter: str = ":"
    ) -> None:
        """Initialize the NamespaceMapping with a mapping and a namespace

        Args:
            mapping: The underlying mapping to store the data
            namespace: The namespace to prefix all keys with
            delimiter: The delimiter to use between the namespace and the key
        """
        self._mapping: MutableMapping[str, JSON] = mapping
        self._namespace: str = namespace
        self._delimiter: str = delimiter

    def _prefixed_key(self, key: str) -> str:
        """Return the key prefixed with the namespace and delimiter

        Args:
            key: The key to prefix

        Returns:
            str: The prefixed key
        """
        return f"{self._namespace}{self._delimiter}{key}"

    def __getitem__(self, key: str) -> JSON:
        """Get the JSON object stored under the given key

        Args:
            key: The key to look up the JSON object with

        Returns:
            JSON: The JSON object stored under the given key
        """
        return self._mapping[self._prefixed_key(key)]

    def __setitem__(self, key: str, value: JSON) -> None:
        """Set the JSON object under the given key, updating if it already exists

        Args:
            key: The key to store the JSON object under
            value: The JSON object to store
        """
        self._mapping[self._prefixed_key(key)] = value

    def __delitem__(self, key: str) -> None:
        """Delete the JSON object stored under the given key

        Args:
            key: The key to delete the JSON object for

        Raises:
            KeyError: If the key does not exist
        """
        del self._mapping[self._prefixed_key(key)]

    def __iter__(self) -> Iterator[str]:
        """Iterate over the keys stored in the mapping

        Returns:
            Iterator[str]: An iterator over the keys stored in the mapping
        """
        ns_prefix = f"{self._namespace}{self._delimiter}"
        return (key[len(ns_prefix) :] for key in self._mapping if key.startswith(ns_prefix))

    def __len__(self) -> int:
        """Return the number of JSON objects stored in the mapping

        Returns:
            int: The number of JSON objects stored in the mapping
        """
        ns_prefix = f"{self._namespace}{self._delimiter}"
        return sum(1 for key in self._mapping if key.startswith(ns_prefix))
