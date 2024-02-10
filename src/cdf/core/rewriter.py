"""This module contains the logic for rewriting the ast of a python script."""
import ast
import typing as t
from copy import deepcopy

from cdf.core.monads import Err, Ok, Result


class BasicPipeRewriter(ast.NodeTransformer):
    _cdf_entrypoint: str = "pipeline"

    def visit_Call(self, node: ast.Call) -> ast.AST:
        if isinstance(node.func, ast.Attribute):
            src_ns = t.cast(ast.Name, node.func.value)
            if node.func.attr == "pipeline" and src_ns.id == "dlt":
                src_ns.id = "cdf"
                node.func.attr = self._cdf_entrypoint
        return self.generic_visit(node)

    def visit_ImportFrom(self, node) -> ast.AST:
        if node.level >= 1:
            node.module = f"pipelines.{node.module}"
            node.level -= 1
            while node.level > 0:
                node.level -= 1
                node.module = f".{node.module}"
        return node


class InterceptingPipeRewriter(BasicPipeRewriter):
    _cdf_entrypoint: str = "intercepting_pipeline"


intercepting_pipe_rewriter = InterceptingPipeRewriter()
pipe_rewriter = BasicPipeRewriter()


def rewrite_pipeline(
    tree: ast.Module,
    rewriter: ast.NodeTransformer = pipe_rewriter,
    copy: bool = True,
) -> Result[str, Exception]:
    """Generates runtime code from a python script ast.

    Args:
        tree: The ast of a python script.
        rewriter: A custom ast rewriter to use. Defaults to BasicPipeRewriter.
        copy: Whether to deepcopy the tree before rewriting. Defaults to True.

    Returns:
        str: The transformed code of the python script.
    """
    try:
        tree = deepcopy(tree) if copy else tree
        tree.body[0] = ast.Import(names=[ast.alias(name="cdf", asname=None)])
        stringified_code = ast.unparse(ast.fix_missing_locations(rewriter.visit(tree)))
        return Ok(
            "\n".join(line for line in stringified_code.splitlines() if line.strip())
        )
    except Exception as e:
        return Err(e)
