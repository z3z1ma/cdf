"""The state module is responible for providing an adapter through which we can persist data"""

import json
import time
import typing as t
from datetime import timedelta, timezone

import pandas as pd
import pydantic
from dlt.common.pipeline import ExtractInfo, LoadInfo, NormalizeInfo
from sqlglot import exp
from sqlmesh.core.config.connection import (
    DuckDBConnectionConfig,
    MySQLConnectionConfig,
    PostgresConnectionConfig,
)
from sqlmesh.core.engine_adapter import EngineAdapter

import cdf.core.logger as logger
from cdf.core.context import active_project, execution_id
from cdf.types import M, P

T = t.TypeVar("T")
JSON = t.Union[bool, int, float, str, t.List["JSON"], t.Dict[str, "JSON"]]

KV_SCHEMA = {"key": exp.DataType.build("TEXT"), "value": exp.DataType.build("TEXT")}
"""The schema for the key-value store"""

_PIPELINE_SCHEMA = {
    "load_id": exp.DataType.build("TEXT"),
    "timestamp": exp.DataType.build("INT64"),
    "pipeline": exp.DataType.build("TEXT"),
    "dataset": exp.DataType.build("TEXT"),
    "destination_name": exp.DataType.build("TEXT"),
    "destination_type": exp.DataType.build("TEXT"),
    "data": exp.DataType.build("TEXT"),
    "success": exp.DataType.build("BOOLEAN"),
    "elapsed": exp.DataType.build("FLOAT"),
    "execution_id": exp.DataType.build("TEXT"),
}

EXTRACT_SCHEMA = _PIPELINE_SCHEMA.copy()
"""The schema for the extract store"""
NORMALIZE_SCHEMA = _PIPELINE_SCHEMA.copy()
"""The schema for the normalize store"""
LOAD_SCHEMA = _PIPELINE_SCHEMA.copy()
"""The schema for the load store"""

AUDIT_SCHEMA = {
    "event": exp.DataType.build("TEXT"),
    "timestamp": exp.DataType.build("INT64"),
    "elapsed": exp.DataType.build("FLOAT"),
    "success": exp.DataType.build("BOOLEAN"),
    "properties": exp.DataType.build("TEXT"),
    "execution_id": exp.DataType.build("TEXT"),
}
"""The schema for the audit store"""


def _no_props(*args: t.Any, **kwargs: t.Any) -> t.Dict[str, JSON]:
    """Empty properties"""
    return {}


class StateStore(pydantic.BaseModel):
    """The state store is responsible for persisting data"""

    model_config = {"frozen": True, "from_attributes": True}

    schema_: t.Annotated[str, pydantic.Field(alias="schema")] = "cdf_state"
    """The schema in which to store data"""
    protected: bool = True
    """Whether the state store is protected, i.e. should never be torn down

    A safety measure to prevent accidental data loss when users are consuming the cdf API
    directly. This should be set to False when running tests or you know what you're doing.
    """

    connection: t.Union[
        DuckDBConnectionConfig,
        MySQLConnectionConfig,
        PostgresConnectionConfig,
    ] = DuckDBConnectionConfig(database=".cdf.db")
    """The connection configuration to the state store"""

    _adapter: t.Optional[EngineAdapter] = None
    """Lazy loaded adapter to the state store"""

    @property
    def kv_table(self) -> str:
        """The key-value table name"""
        return f"{self.schema_}.json_store"

    @property
    def extract_table(self) -> str:
        """The extract table name"""
        return f"{self.schema_}.extract_store"

    @property
    def normalize_table(self) -> str:
        """The normalize table name"""
        return f"{self.schema_}.normalize_store"

    @property
    def load_table(self) -> str:
        """The load table name"""
        return f"{self.schema_}.load_store"

    @property
    def audit_table(self) -> str:
        """The audit table name"""
        return f"{self.schema_}.audit_store"

    @property
    def adapter(self) -> EngineAdapter:
        """The adapter to the state store"""
        if self._adapter is None:
            adapter = self.connection.create_engine_adapter()
            adapter.create_schema(self.schema_)
            adapter.create_state_table(self.kv_table, KV_SCHEMA)
            adapter.create_state_table(self.extract_table, EXTRACT_SCHEMA)
            adapter.create_state_table(self.normalize_table, NORMALIZE_SCHEMA)
            adapter.create_state_table(self.load_table, LOAD_SCHEMA)
            adapter.create_state_table(self.audit_table, AUDIT_SCHEMA)
            self._adapter = adapter
        return self._adapter

    def _execute(self, sql: str) -> None:
        """Execute a SQL statement"""
        self.adapter.execute(sql)

    def store_json(self, key: str, value: JSON) -> None:
        """Store a JSON value"""
        with self.adapter.transaction(value is not None), logger.suppress_and_warn():
            self.adapter.delete_from(self.kv_table, f"key = '{key}'")
            if value is not None:
                self.adapter.insert_append(
                    self.kv_table,
                    pd.DataFrame([{"key": key, "value": json.dumps(value)}]),
                )

    def load_json(self, key: str) -> JSON:
        """Load a JSON value"""
        return json.loads(
            self.adapter.fetchone(
                exp.select("value").from_(self.kv_table).where(f"key = '{key}'")
            )[0]
        )

    __getitem__ = load_json
    __setitem__ = store_json

    def __enter__(self, condition: bool = True) -> "StateStore":
        """Proxies to the transaction context manager"""
        self.__trans = self.adapter.transaction(condition)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Proxies to the transaction context manager"""
        self.__trans.__exit__(exc_type, exc_value, traceback)

    def __del__(self) -> None:
        """Close the connection to the state store"""
        if self._adapter is not None:
            self.adapter.close()

    def with_audit(
        self,
        event: str,
        input_props: t.Union[t.Callable[P, JSON], t.Dict[str, JSON]] = _no_props,
        output_props: t.Callable[[T], t.Dict[str, JSON]] = _no_props,
    ) -> t.Callable[[t.Callable[P, T]], t.Callable[P, T]]:
        """Decorator to add audit logging to a function

        Args:
            event (str): The event name
            input_props (Union[Callable[[P], JSON], Dict[str, JSON], optional): A callable that takes the function arguments
                and returns a dictionary of properties to log. Alternatively, static props are accepted as a dictionary.
            output_props (Callable[[T], Dict[str, JSON], optional): A callable that takes the function return value
                and returns a dictionary of properties to log.
        """

        def decorator(func: t.Callable[P, T]) -> t.Callable[P, T]:
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                audit_event = {
                    "event": event,
                    "timestamp": time.time(),
                    "elapsed": 0,
                    "success": False,
                    "properties": (
                        input_props(*args, **kwargs)
                        if callable(input_props)
                        else input_props
                    ),
                    "execution_id": execution_id.get(),
                }
                start = time.perf_counter()
                try:
                    rv = func(*args, **kwargs)
                except Exception as e:
                    audit_event["elapsed"] = time.perf_counter() - start
                    with self.adapter.transaction(), logger.suppress_and_warn():
                        self.adapter.insert_append(
                            self.audit_table,
                            pd.DataFrame([audit_event]),
                        )
                    raise e
                audit_event["elapsed"] = time.perf_counter() - start
                audit_event["success"] = not isinstance(rv, M.Err)
                audit_event["properties"].update(output_props(rv))
                audit_event["properties"] = json.dumps(audit_event["properties"])
                with self.adapter.transaction(), logger.suppress_and_warn():
                    self.adapter.insert_append(
                        self.audit_table,
                        pd.DataFrame([audit_event]),
                    )
                return rv

            return wrapper

        return decorator

    def audit(
        self, event: str, success: bool = True, elapsed: float = 0.0, **properties: JSON
    ) -> None:
        """Audit an event"""
        payload = {
            "event": event,
            "timestamp": time.time(),
            "elapsed": elapsed,
            "success": success,
            "properties": json.dumps(properties),
            "execution_id": execution_id.get(),
        }
        with self.adapter.transaction(), logger.suppress_and_warn():
            self.adapter.insert_append(
                self.audit_table,
                pd.DataFrame([payload]),
            )

    def fetch_audits(
        self, *event_names: str, limit: int = 100, failed_only: bool = False
    ):
        """List all audit events"""
        assert limit > 0 and limit < 1000, "Limit must be between 1 and 1000"
        q = (
            exp.select("*")
            .from_(self.audit_table)
            .order_by("timestamp DESC")
            .limit(limit)
        )
        if failed_only:
            q = q.where("success = false")
        if event_names:
            q = q.where(f"event IN {tuple(event_names)}")
        df = self.adapter.fetchdf(q).sort_values("timestamp", ascending=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        localtz = timezone(timedelta(seconds=-time.timezone))
        df["timestamp"] = df["timestamp"].dt.tz_convert(localtz)
        return df

    def clear_audits(self):
        """Clear all audit events"""
        self.adapter.delete_from(self.audit_table, "1 = 1")

    def capture_extract_info(self, info: ExtractInfo) -> None:
        """Capture extract information"""
        d = self._info_to_payload(info)
        if not d:
            return
        with self.adapter.transaction(), logger.suppress_and_warn():
            self.adapter.insert_append(self.extract_table, pd.DataFrame(d))

    def capture_normalize_info(self, info: NormalizeInfo) -> None:
        """Capture normalize information"""
        d = self._info_to_payload(info)
        if not d:
            return
        with self.adapter.transaction(), logger.suppress_and_warn():
            self.adapter.insert_append(self.normalize_table, pd.DataFrame(d))

    def capture_load_info(self, info: LoadInfo) -> None:
        """Capture load information"""
        d = self._info_to_payload(info)
        if not d:
            return
        with self.adapter.transaction(), logger.suppress_and_warn():
            self.adapter.insert_append(self.load_table, pd.DataFrame(d))

    @staticmethod
    def _info_to_payload(
        info: t.Union[ExtractInfo, NormalizeInfo, LoadInfo],
    ) -> t.List[t.Dict[str, t.Any]]:
        """Convert an info object to a payload"""
        payload = []
        for pkg in info.load_packages:
            payload.append(
                {
                    "load_id": pkg.load_id,
                    "timestamp": int(time.time()),
                    "pipeline": info.pipeline.pipeline_name,
                    "dataset": info.pipeline.dataset_name,
                    "destination_name": info.pipeline.destination.destination_name,
                    "destination_type": info.pipeline.destination.destination_type,
                    "data": json.dumps(pkg.asdict(), default=str),
                    "success": pkg.state != "aborted",
                    "elapsed": sum(
                        [j.elapsed for k in pkg.jobs.keys() for j in pkg.jobs[k]]
                    ),
                    "execution_id": execution_id.get(),
                }
            )
        return payload

    def fetch_extracted(
        self, *load_ids: str, limit: int = 100, failed_only: bool = False
    ):
        """List all extracted data"""
        assert limit > 0 and limit < 1000, "Limit must be between 1 and 1000"
        q = (
            exp.select("*")
            .from_(self.extract_table)
            .order_by("timestamp DESC")
            .limit(limit)
        )
        if failed_only:
            q = q.where("success = false")
        if load_ids:
            q = q.where(f"load_id IN {tuple(load_ids)}")
        df = self.adapter.fetchdf(q).sort_values("timestamp", ascending=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        localtz = timezone(timedelta(seconds=-time.timezone))
        df["timestamp"] = df["timestamp"].dt.tz_convert(localtz)
        return df

    def fetch_normalized(
        self, *load_ids: str, limit: int = 100, failed_only: bool = False
    ):
        """List all normalized data"""
        assert limit > 0 and limit < 1000, "Limit must be between 1 and 1000"
        q = (
            exp.select("*")
            .from_(self.normalize_table)
            .order_by("timestamp DESC")
            .limit(limit)
        )
        if failed_only:
            q = q.where("success = false")
        if load_ids:
            q = q.where(f"load_id IN {tuple(load_ids)}")
        df = self.adapter.fetchdf(q).sort_values("timestamp", ascending=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        localtz = timezone(timedelta(seconds=-time.timezone))
        df["timestamp"] = df["timestamp"].dt.tz_convert(localtz)
        return df

    def fetch_loaded(self, *load_ids: str, limit: int = 100, failed_only: bool = False):
        """List all loaded data"""
        assert limit > 0 and limit < 1000, "Limit must be between 1 and 1000"
        q = (
            exp.select("*")
            .from_(self.load_table)
            .order_by("timestamp DESC")
            .limit(limit)
        )
        if failed_only:
            q = q.where("success = false")
        if load_ids:
            q = q.where(f"load_id IN {tuple(load_ids)}")
        df = self.adapter.fetchdf(q)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        localtz = timezone(timedelta(seconds=-time.timezone))
        df["timestamp"] = df["timestamp"].dt.tz_convert(localtz)
        return df


def with_audit(
    event: str,
    input_props: t.Union[t.Callable[P, JSON], t.Dict[str, JSON]] = _no_props,
    output_props: t.Callable[[T], t.Dict[str, JSON]] = _no_props,
) -> t.Callable[[t.Callable[P, T]], t.Callable[P, T]]:
    """Decorator to add audit logging to a function given an active project"""

    def decorator(func: t.Callable[P, T]) -> t.Callable[P, T]:
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            project = active_project.get(None)
            if project is None:
                return func(*args, **kwargs)
            return project.state.with_audit(
                event,
                input_props,
                output_props,
            )(func)(*args, **kwargs)

        return wrapper

    return decorator


def audit(
    event: str, success: bool = True, elapsed: float = 0.0, **properties: JSON
) -> None:
    """Audit an event given an active project"""
    properties.setdefault("execution_id", execution_id.get())
    project = active_project.get(None)
    if project is not None:
        project.state.audit(event, success, elapsed, **properties)
