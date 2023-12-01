from pathlib import Path

import pytest

from cdf.core.workspace import Project, Workspace


def test_load_sources():
    # Ensure we can't instantiate a workspace with a non-existent path
    with pytest.raises(ValueError):
        Workspace(root=Path("./examples/multi_workspace/workspaces/idontexist"))

    # Ensure we can instantiate a workspace
    ws = Workspace(root=Path("./examples/multi_workspace/workspaces/alexb"))

    # Ensure capabilities are detected based on layout
    assert ws.has_pipelines
    assert not ws.has_dependencies

    sources = ws.pipelines

    # Ensure we can load all sources
    assert len(sources) == 5

    # Test that requirements.txt is injected in the path
    # the below module imports simple_salesforce, which is not installed
    # in the test environment, but is installed in the requirements.txt
    assert "dota2_stats" in sources


def test_load_project():
    proj = Project.find_nearest(path=Path("examples/multi_workspace"))
    assert proj is not None

    sources = proj.alexb.pipelines
    assert len(sources) > 0

    _ = sources["hackernews"]
