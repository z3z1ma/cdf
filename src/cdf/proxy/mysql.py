"""A MySQL proxy server which uses SQLMesh to execute queries."""

import typing as t
import asyncio
import logging
from collections import defaultdict

import sqlmesh
from mysql_mimic import MysqlServer, Session
from mysql_mimic.server import logger
from sqlglot import exp


async def file_watcher(context: sqlmesh.Context) -> None:
    """Watch for changes in the workspace and refresh the context."""
    while True:
        await asyncio.sleep(5.0)
        await asyncio.to_thread(context.refresh)


class SQLMeshSession(Session):
    """A session for the MySQL proxy server which uses SQLMesh."""

    context: sqlmesh.Context

    async def query(
        self, expression: exp.Expression, sql: str, attrs: t.Dict[str, str]
    ) -> t.Tuple[t.Tuple[t.Tuple[t.Any], ...], t.List[str]]:
        """Execute a query."""
        tables = list(expression.find_all(exp.Table))
        if any((table.db, table.name) == ("__semantic", "__table") for table in tables):
            expression = self.context.rewrite(sql)
            logger.info("Compiled semantic expression!")
        logger.info(expression.sql(self.context.default_dialect))
        df = self.context.fetchdf(expression)
        logger.debug(df)
        return tuple(df.itertuples(index=False)), list(df.columns)

    async def schema(self) -> t.Dict[str, t.Dict[str, t.Dict[str, str]]]:
        """Get the schema of the database."""
        schema = defaultdict(dict)
        for model in self.context.models.values():
            fqn = model.fully_qualified_table
            if model.columns_to_types and all(
                typ is not None for typ in model.columns_to_types.values()
            ):
                schema[fqn.db][fqn.name] = model.columns_to_types
        return schema


async def run_mysql_proxy(context: sqlmesh.Context) -> None:
    """Run the MySQL proxy server."""

    logging.basicConfig(level=logging.DEBUG)
    server = MysqlServer(
        session_factory=type(
            "BoundSQLMeshSession",
            (SQLMeshSession,),
            {"context": context},
        )
    )
    asyncio.create_task(file_watcher(context))
    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        await server.wait_closed()


__all__ = ["run_mysql_proxy"]
