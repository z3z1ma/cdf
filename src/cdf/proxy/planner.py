"""An http server which executed a plan which is a pickled pydantic model

This is purely a POC. It will be replaced by a more robust solution in the future
using flask or fastapi. It will always be designed such that input must be
trusted. In an environment where the input is not trusted, the server should
never be exposed to the internet. It should always be behind a firewall and
only accessible by trusted clients.
"""

import http.server
import io
import json
import logging
import pickle
import socketserver
import traceback
import typing as t
import uuid
from contextlib import redirect_stderr, redirect_stdout

import sqlmesh


def run_plan_server(port: int, context: sqlmesh.Context) -> None:
    """Listen on a port and execute plans."""

    # TODO: move this
    logging.basicConfig(level=logging.DEBUG)

    def _plan(plan: t.Any) -> t.Any:
        """Run a plan"""
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            context.apply(plan)
        return {
            "stdout": stdout.getvalue(),
            "stderr": stderr.getvalue(),
            "execution_id": uuid.uuid4().hex,
        }

    class Handler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self) -> None:
            """Ping the server"""
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Pong")

        def do_POST(self) -> None:
            """Run the plan"""
            content_length = int(self.headers["Content-Length"])
            ser_plan = self.rfile.read(content_length)
            try:
                plan = pickle.loads(ser_plan)
                resp = _plan(plan)
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(resp).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                self.wfile.write(str(e).encode())
                self.wfile.write(b"\n")
                self.wfile.write(traceback.format_exc().encode())

    with socketserver.TCPServer(("", port), Handler) as httpd:
        logging.info("serving at port %s", port)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
