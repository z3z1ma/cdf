from pathlib import Path

import dlt
import pytest

from cdf.core.workspace import Project, Workspace


def test_load_sources():
    # Ensure we can't instantiate a workspace with a non-existent path
    with pytest.raises(ValueError):
        Workspace(root=Path("tests/fixtures/it_projectz"))

    # Ensure we can instantiate a workspace
    ws = Workspace(root=Path("tests/fixtures/it_project"))

    # Ensure capabilities are detected based on layout
    assert ws.has_sources
    assert ws.has_dependencies

    sources = ws.load_sources()

    # Ensure we can load all sources
    assert len(sources) == 3

    # Test that requirements.txt is injected in the path
    # the below module imports simple_salesforce, which is not installed
    # in the test environment, but is installed in the requirements.txt
    assert "pokemon" in sources


def test_load_project():
    proj = Project.find_nearest(path=Path("examples/advanced"))
    assert proj is not None

    sources = proj.datateam.load_sources()
    assert len(sources) == 1

    meta = sources["hackernews"]
    # info = proj.data.get_extractor("hackernews")

    print(meta)
    # print(info)
