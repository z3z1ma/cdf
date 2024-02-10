import inspect

import pytest
from sqlglot import parse_one

import cdf.core.dialect as d
from cdf.core.parser import (
    ParserError,
    extract_docstring_or_raise,
    parse_cdf_component_spec,
    parse_python_ast,
    process_script,
    props_to_dict,
)
from cdf.core.rewriter import rewrite_pipeline


@pytest.fixture
def cdf_mod():
    return inspect.cleandoc(
        '''
    """
    PIPELINE (
        name sfdc,
        description 'Load SFDC data',
        tags [pii, main],
        cron '0 0 * * *',
        owner 'jdoe'
    );
    @METRIC("account_*", row_count); -- Number of rows
    @FILTER("account_*", pii_filter); -- Sensitive data
    """
    import dlt
    var = 1
    x = [var, 2, 3]
    def foo(): yield from x
    # comment 1
    dlt.pipeline("sfdc").run(foo, destination="duckdb", table_name="sfdc") # comment 2
    '''
    )


def test_parse_meta(cdf_mod: str):
    script = parse_python_ast(cdf_mod).bind(extract_docstring_or_raise).unwrap()
    assert "PIPELINE" in script.doc
    assert "name sfdc" in script.doc
    dsl = parse_cdf_component_spec(script.doc).unwrap()
    assert isinstance(dsl, d.Pipeline)


def test_parse_dsl():
    raw_spec_node = parse_one(
        """
    PIPELINE (
        name sfdc,
        description 'Load SFDC data',
        tags [pii, main],
        cron '0 0 * * *',
        owner 'jdoe'
    );
        """,
        dialect="cdf",
    )
    assert isinstance(raw_spec_node, d.Pipeline)
    meta = props_to_dict(raw_spec_node).unwrap()
    assert meta["name"] == "sfdc"


def test_process_script():
    with pytest.raises(ParserError):
        process_script("tests/core/fixtures/badd_spec.py").unwrap()

    with pytest.raises(ParserError, match="Failed to parse cdf DSL.*"):
        process_script("tests/core/fixtures/bad_spec.py").unwrap()

    with pytest.raises(ParserError, match="Failed to process script.*"):
        process_script("tests/core/fixtures/bad_spec.txt").unwrap()

    script = process_script("tests/core/fixtures/basic_pipe.py").unwrap()
    assert script.name == "data_pipeline"
    assert script.type_ == "pipeline"


def test_generate_runtime_code(cdf_mod):
    dump = parse_python_ast(cdf_mod).bind(rewrite_pipeline).unwrap()
    lines = dump.splitlines()
    assert lines[0] == "import cdf"
    assert "dlt.pipeline" not in dump
    assert "# comment 1" not in dump
    assert (
        lines[-1]
        == "cdf.pipeline('sfdc').run(foo, destination='duckdb', table_name='sfdc')"
    )
