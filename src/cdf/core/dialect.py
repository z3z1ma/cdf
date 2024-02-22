"""CDF Dialect."""
from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import Dialect


class Pipeline(exp.Expression):
    arg_types = {"expressions": True}


class Script(exp.Expression):
    arg_types = {"expressions": True}


class Notebook(exp.Expression):
    arg_types = {"expressions": True}


class Publisher(exp.Expression):
    arg_types = {"expressions": True}


class Sink(exp.Expression):
    arg_types = {"expressions": True}


class Project(exp.Expression):
    arg_types = {"expressions": True}


class Workspace(exp.Expression):
    arg_types = {"expressions": True}


CDFComponentDSL = t.Union[
    Pipeline, Script, Notebook, Publisher, Sink, Project, Workspace
]


def _create_parser(parser_type: t.Type[exp.Expression]) -> t.Callable:
    """Creates a parser for a given expression type."""

    def parse(self: parser.Parser) -> t.Optional[exp.Expression]:
        expressions = []
        while True:
            key_expression = self._parse_id_var(any_token=True)
            if not key_expression:
                break
            key = key_expression.name.lower()
            start = self._curr
            value = self._parse_bracket(self._parse_field(any_token=True))
            if isinstance(value, exp.Expression):
                value.meta["sql"] = self._find_sql(start, self._prev)  # type: ignore
            expressions.append(self.expression(exp.Property, this=key, value=value))
            if not self._match(tokens.TokenType.COMMA):
                break
        return self.expression(parser_type, expressions=expressions)

    return parse


PARSERS = {
    "PIPELINE": _create_parser(Pipeline),
    "SCRIPT": _create_parser(Script),
    "NOTEBOOK": _create_parser(Notebook),
    "PUBLISHER": _create_parser(Publisher),
    "SINK": _create_parser(Sink),
    "PROJECT": _create_parser(Project),
    "WORKSPACE": _create_parser(Workspace),
}


def _render_spec(
    self: generator.Generator,
    expression: Pipeline | Script | Notebook | Publisher | Sink | Project | Workspace,
    name: str,
) -> str:
    props = ",\n".join(
        self.indent(f"{prop.name} {self.sql(prop, 'value')}")
        for prop in expression.expressions
    )
    return "\n".join([f"{name} (", props, ")"])


class CDF(Dialect):
    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"']
        IDENTIFIERS = ["`"]
        KEYWORDS = {}

    class Parser(parser.Parser):
        def _parse_statement(self) -> t.Optional[exp.Expression]:
            if self._curr is None:
                return None
            parser = PARSERS.get(self._curr.text.upper())
            if parser:
                comments = self._curr.comments
                self._advance()
                meta = self._parse_wrapped(lambda: t.cast(t.Callable, parser)(self))
                meta.comments = comments
                return meta
            return super()._parse_statement()

    class Generator(generator.Generator):
        TRANSFORMS = {
            klass: lambda self, expression: _render_spec(
                self,
                expression,
                def_,
            )
            for klass, def_ in (
                (Pipeline, "PIPELINE"),
                (Script, "SCRIPT"),
                (Notebook, "NOTEBOOK"),
                (Publisher, "PUBLISHER"),
                (Sink, "SINK"),
                (Project, "PROJECT"),
                (Workspace, "WORKSPACE"),
            )
        }
