import json
import os
import tempfile
import threading
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from cdf.legacy.adapter.state import (
    FileStateBackend,
    SqlAlchemyStateBackend,
    _ScopedMapping,
    state_backend_factory,
)
from cdf.legacy.interface import (
    FileStateBackendConfig,
    SqlAlchemyStateBackendConfig,
)

JSON = str | int | float | bool | None | dict[str, "JSON"] | list["JSON"]


def create_in_memory_engine():
    return create_engine("sqlite:///:memory:", echo=False)


def test_file_state_backend_basic_operations():
    # Create a temporary directory to store the state file
    with tempfile.TemporaryDirectory() as temp_dir:
        state_file = Path(temp_dir) / "state.json"
        backend = FileStateBackend(path=state_file)

        # Test setting an item
        backend["key1"] = {"data": 123}
        assert backend["key1"] == {"data": 123}

        # Test updating an item
        backend["key1"] = {"data": 456}
        assert backend["key1"] == {"data": 456}

        # Test deleting an item
        del backend["key1"]
        with pytest.raises(KeyError):
            _ = backend["key1"]

        # Test len and iter
        backend["key2"] = "value2"
        backend["key3"] = "value3"
        assert len(backend) == 2
        keys = list(backend)
        assert set(keys) == {"key2", "key3"}

        # Test persistence
        backend = FileStateBackend(path=state_file)
        assert len(backend) == 2
        assert backend["key2"] == "value2"


def test_file_state_backend_thread_safety():
    # Create a temporary directory to store the state file
    with tempfile.TemporaryDirectory() as temp_dir:
        state_file = Path(temp_dir) / "state.json"
        backend = FileStateBackend(path=state_file)

        def worker():
            for i in range(100):
                backend[f"key{i}"] = i

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(backend) == 100
        for i in range(100):
            assert backend[f"key{i}"] == i


def test_file_state_backend_missing_key():
    # Create a temporary directory to store the state file
    with tempfile.TemporaryDirectory() as temp_dir:
        state_file = Path(temp_dir) / "state.json"
        backend = FileStateBackend(path=state_file)

        with pytest.raises(KeyError):
            _ = backend["nonexistent_key"]


def test_sqlalchemy_state_backend_basic_operations():
    # Use an in-memory SQLite database for testing
    backend = SqlAlchemyStateBackend(
        table_name="state_table",
        schema_name=None,
        connection_str="sqlite:///:memory:",
        dumper=json.dumps,
        loader=json.loads,
    )

    # Test setting an item
    backend["key1"] = {"data": 123}
    assert backend["key1"] == {"data": 123}

    # Test updating an item
    backend["key1"] = {"data": 456}
    assert backend["key1"] == {"data": 456}

    # Test deleting an item
    del backend["key1"]
    with pytest.raises(KeyError):
        _ = backend["key1"]

    # Test len and iter
    backend["key2"] = "value2"
    backend["key3"] = "value3"
    assert len(backend) == 2
    keys = list(backend)
    assert set(keys) == {"key2", "key3"}


def test_sqlalchemy_state_backend_missing_key():
    backend = SqlAlchemyStateBackend(
        table_name="state_table",
        schema_name=None,
        connection_str="sqlite:///:memory:",
    )

    with pytest.raises(KeyError):
        _ = backend["nonexistent_key"]


def test_sqlalchemy_state_backend_thread_safety():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = os.path.join(temp_dir, "test.db")
        connection_str = f"sqlite:///{db_file}"
        backend = SqlAlchemyStateBackend(
            table_name="state_table",
            schema_name=None,
            connection_str=connection_str,
        )

        def worker(thread_id):
            for i in range(100):
                backend[f"key{thread_id}_{i}"] = i

        threads = [threading.Thread(target=worker, args=(tid,)) for tid in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(backend) == 500
        for tid in range(5):
            for i in range(100):
                assert backend[f"key{tid}_{i}"] == i


def test_sqlalchemy_state_backend_conflict_resolution():
    # ... (adjusted to use file-based database)
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = os.path.join(temp_dir, "test.db")
        connection_str = f"sqlite:///{db_file}"
        backend = SqlAlchemyStateBackend(
            table_name="state_table",
            schema_name=None,
            connection_str=connection_str,
        )

        # Insert initial value
        backend["key1"] = {"data": 123}
        assert backend["key1"] == {"data": 123}

        # Insert again to test upsert
        backend["key1"] = {"data": 456}
        assert backend["key1"] == {"data": 456}


def test_state_backend_factory_file_backend_absolute_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        state_file = Path(temp_dir) / "state.json"
        config = FileStateBackendConfig(
            adapter="file",
            path=str(state_file),
            buffered=False,
        )
        backend = state_backend_factory(Path(temp_dir), config)
        assert isinstance(backend, FileStateBackend)
        backend["key"] = "value"
        assert backend["key"] == "value"


def test_state_backend_factory_file_backend_relative_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        state_file = "state.json"
        config = FileStateBackendConfig(
            adapter="file",
            path=state_file,
            buffered=False,
        )
        backend = state_backend_factory(Path(temp_dir), config)
        assert isinstance(backend, FileStateBackend)
        backend["key"] = "value"
        assert backend["key"] == "value"
        assert (Path(temp_dir) / state_file).exists()


def test_state_backend_factory_sqlalchemy_backend():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = os.path.join(temp_dir, "test.db")
        connection_str = f"sqlite:///{db_file}"
        config = SqlAlchemyStateBackendConfig(
            adapter="sqlalchemy",
            connection_str=connection_str,
            table_name="state_table",
        )
        backend = state_backend_factory(Path(temp_dir), config)
        assert isinstance(backend, SqlAlchemyStateBackend)
        backend["key"] = "value"
        assert backend["key"] == "value"


def test_scoped_mapping_basic_operations():
    base_mapping = {}
    mapping = _ScopedMapping(base_mapping, "namespace")

    # Test setting an item
    mapping["key1"] = "value1"
    assert mapping["key1"] == "value1"
    assert base_mapping["namespace:key1"] == "value1"

    # Test updating an item
    mapping["key1"] = "value2"
    assert mapping["key1"] == "value2"
    assert base_mapping["namespace:key1"] == "value2"

    # Test deleting an item
    del mapping["key1"]
    assert "namespace:key1" not in base_mapping
    with pytest.raises(KeyError):
        _ = mapping["key1"]

    # Test len and iter
    mapping["key2"] = "value2"
    mapping["key3"] = "value3"
    assert len(mapping) == 2
    keys = list(mapping)
    assert set(keys) == {"key2", "key3"}


def test_scoped_mapping_with_custom_delimiter():
    base_mapping = {}
    mapping = _ScopedMapping(base_mapping, "namespace", delimiter="::")

    mapping["key"] = "value"
    assert base_mapping["namespace::key"] == "value"
    assert mapping["key"] == "value"


def test_scoped_mapping_multiple_namespaces():
    base_mapping = {}
    mapping1 = _ScopedMapping(base_mapping, "namespace1")
    mapping2 = _ScopedMapping(base_mapping, "namespace2")

    mapping1["key"] = "value1"
    mapping2["key"] = "value2"

    assert mapping1["key"] == "value1"
    assert mapping2["key"] == "value2"
    assert base_mapping["namespace1:key"] == "value1"
    assert base_mapping["namespace2:key"] == "value2"


def test_scoped_mapping_missing_key():
    base_mapping = {}
    mapping = _ScopedMapping(base_mapping, "namespace")

    with pytest.raises(KeyError):
        _ = mapping["nonexistent_key"]


def test_file_state_backend_custom_dumper_loader():
    with tempfile.TemporaryDirectory() as temp_dir:
        state_file = Path(temp_dir) / "state.json"

        def custom_dumper(obj):
            return json.dumps(obj).upper()

        def custom_loader(s):
            return json.loads(s.lower())

        backend = FileStateBackend(
            path=state_file,
            dumper=custom_dumper,
            loader=custom_loader,
        )

        backend["key"] = {"Data": 123}
        assert backend["key"] == {"Data": 123}
        assert state_file.exists()

        # Check that the file contents are uppercased
        with open(state_file, "r") as f:
            content = f.read()
            assert content.isupper()


def test_sqlalchemy_state_backend_custom_dumper_loader():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = os.path.join(temp_dir, "test.db")
        connection_str = f"sqlite:///{db_file}"

        def custom_dumper(obj):
            return json.dumps(obj).upper()

        def custom_loader(s):
            return json.loads(s.lower())

        backend = SqlAlchemyStateBackend(
            table_name="state_table",
            connection_str=connection_str,
            dumper=custom_dumper,
            loader=custom_loader,
        )

        backend["key"] = {"Data": 123}
        assert backend["key"] == {"data": 123}


def test_file_state_backend_persistence():
    with tempfile.TemporaryDirectory() as temp_dir:
        state_file = Path(temp_dir) / "state.json"
        backend = FileStateBackend(path=state_file)
        backend["key"] = "value"

        # Reload the backend
        backend = FileStateBackend(path=state_file)
        assert backend["key"] == "value"


def test_file_state_backend_nonexistent_file():
    with tempfile.TemporaryDirectory() as temp_dir:
        state_file = Path(temp_dir) / "nonexistent.json"
        backend = FileStateBackend(path=state_file)
        assert len(backend) == 0


def test_sqlalchemy_state_backend_empty():
    backend = SqlAlchemyStateBackend(
        table_name="state_table",
        schema_name=None,
        connection_str="sqlite:///:memory:",
    )
    assert len(backend) == 0


def test_state_backend_factory_invalid_adapter():
    class InvalidConfig:
        adapter = "invalid"

    with pytest.raises(ValueError):
        _ = state_backend_factory(Path("."), InvalidConfig())
