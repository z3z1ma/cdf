"""
This module contains functions for parsing cdf python scripts into metadata and AST.

Metadata is parsed from the docstring of the python script and is expected to be in cdf DSL format.
This is a SQL-like syntax that is used to define the metadata of a cdf component.

PIPELINE (
  name my_pipeline
);
"""
import ast
import typing as t
from pathlib import Path

from immutabledict import immutabledict
from sqlglot import exp, parse_one

from cdf.core.dialect import CDFComponentDSL
from cdf.core.monads import Result

PathLike = t.Union[str, Path]


class ParserError(ValueError):
    """An error raised by the parser."""


class ScriptNotFoundError(ParserError, FileNotFoundError):
    """An error raised when a script is not found."""


@Result.lift
def read_script(path: PathLike) -> str:
    """Reads a python file and returns its content.

    Args:
        path: The path to the python file.

    Raises:
        ScriptNotFoundError: If the script is not found.
        ParserError: If the file is not a python script.

    Returns:
        str: The content of the python file.
    """
    path = Path(path)
    if not path.exists():
        raise ScriptNotFoundError(f"File not found: {path}")
    if not (path.is_file() and path.suffix == ".py"):
        raise ParserError(f"File argument must be a python script: {path}")
    with path.open() as file:
        return file.read()


@Result.lift
def parse_python_ast(contents: str) -> ast.Module:
    """Returns the ast tree of a python script.

    Args:
        contents: A string containing python code.

    Raises:
        ParserError: If the python script cannot be parsed.

    Returns:
        ast.Module: The parsed python script.
    """
    try:
        return ast.parse(contents)
    except Exception as e:
        raise ParserError(f"Failed to parse python AST: {e}")


class AnnotatedScript(t.NamedTuple):
    """A parsed python script with a docstring."""

    doc: str
    """The docstring of the script which contains the raw spec."""
    tree: ast.Module
    """The ast of the script."""


@Result.lift
def extract_docstring_or_raise(mod: ast.Module) -> AnnotatedScript:
    """Parses the metadata from a cdf python script.

    Args:
        mod: The ast of a python script.

    Raises:
        ParserError: If the docstring cannot be extracted.

    Returns:
        The docstring of the python script.
    """
    doc = mod.body[0]
    if isinstance(doc, ast.Expr) and isinstance(inner := doc.value, ast.Constant):
        return AnnotatedScript(inner.value, mod)
    raise ParserError("No docstring discovered in the python script.")


@Result.lift
def parse_cdf_component_spec(dsl: str) -> CDFComponentDSL:
    """Parses a cdf DSL string into an AST.

    Args:
        contents: A string containing cdf DSL.

    Raises:
        ParserError: If the cdf DSL cannot be parsed or is not a valid cdf specification.

    Returns:
        exp.Expression: The parsed cdf DSL.
    """
    try:
        maybe_spec = parse_one(dsl, dialect="cdf")
        if not isinstance(maybe_spec, t.get_args(CDFComponentDSL)):
            raise ParserError("Not a valid cdf specification.")
        return t.cast(CDFComponentDSL, maybe_spec)
    except Exception as e:
        raise ParserError(f"Failed to parse cdf DSL: {e}")


@Result.lift
def _convert(prop: exp.Expression) -> t.Any:
    """Converts a cdf DSL property value to an equivalent python value.

    Args:
        prop: A cdf DSL property value.

    Raises:
        ParserError: If the cdf DSL property value cannot be converted.

    Returns:
        t.Any: An equivalent python value.
    """
    if isinstance(prop, exp.Literal):
        if prop.is_int:
            return int(prop.this)
        elif prop.is_number:
            return float(prop.this)
        else:
            return prop.text("this")
    if isinstance(prop, (exp.Identifier, exp.Column)):
        return prop.text("this")
    elif isinstance(prop, (exp.Array, exp.Tuple)):
        return [_convert(v).unwrap() for v in prop.expressions]
    elif isinstance(prop, exp.Properties):
        return props_to_dict(prop)
    else:
        raise ParserError(f"Unsupported value type: {type(prop)}")


@Result.lift
def props_to_dict(
    raw_spec: exp.Expression,
) -> immutabledict[str, Result[t.Any, ParserError]]:
    """Converts a cdf DSL node to a raw dictionary.

    Args:
        raw_spec: A cdf DSL node.

    Raises:
        ParserError: If the cdf DSL node cannot be converted.

    Returns:
        t.Dict[str, t.Any]: A dictionary representation of the cdf DSL node.
    """
    return immutabledict(
        {
            _convert(prop.this).unwrap_or(prop.this): _convert(prop.args["value"])
            for prop in raw_spec.expressions
        }
    )


class ParsedComponent(t.NamedTuple):
    """A parsed cdf python script."""

    type_: str
    """The type of the component."""
    tree: ast.Module
    """The AST of the script."""
    specification: immutabledict[str, Result[t.Any, ParserError]]
    """The parsed cdf metadata."""
    path: Path
    """The path to the script."""
    mtime: float
    """The last modified time of the script."""

    @property
    def name(self) -> str:
        """The name of the component."""
        return self.specification["name"].unwrap_or(self.path.stem)

    def to_script(self) -> str:
        """Returns a python script representation of the parsed component."""
        return "\n".join(
            [
                f"# {self.type_}",
                f"# {self.specification}",
                f"# {self.path}",
                ast.unparse(self.tree),
            ]
        )


@Result.lift
def process_script(path: PathLike) -> ParsedComponent:
    """Parses a python script containing a docstring with cdf DSL.

    Args:
        path: The path to the python script.

    Raises:
        ParserError: If the file is not a python script or the cdf DSL cannot be parsed.

    Returns:
        CDFRawComponentSpec: The parsed cdf python script and metadata.
    """
    path = Path(path)
    script, err = (
        read_script(path) >> parse_python_ast >> extract_docstring_or_raise
    ).to_parts()
    if err:
        raise ParserError(f"Failed to process script: {path}") from err
    spec_tree = parse_cdf_component_spec(script.doc)
    if spec_tree.is_err():
        raise ParserError(
            f"Failed to parse cdf DSL: {path}"
        ) from spec_tree.unwrap_err()
    return ParsedComponent(
        type_=spec_tree.map(lambda node: node.key).unwrap(),
        tree=script.tree,
        specification=spec_tree(props_to_dict).unwrap(),
        path=path,
        mtime=path.stat().st_mtime,
    )
