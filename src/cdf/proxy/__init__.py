"""The proxy module provides a MySQL proxy server for the CDF.

The proxy server is used to intercept MySQL queries and execute them using SQLMesh.
This allows it to integrate with BI tools and other MySQL clients. Furthermore,
during interception, the server can rewrite queries expanding semantic references
making it an easy to use semantic layer for SQLMesh.
"""

from cdf.proxy.mysql import run_mysql_proxy
from cdf.proxy.planner import run_plan_server

__all__ = ["run_mysql_proxy", "run_plan_server"]
