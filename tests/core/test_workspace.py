from cdf.core.workspace import exists, parse_metadata

# CLI, given a particular entrypoint...
# path -> workspace[components] -> hydrated component -> execute?
# so from path -> execute is the end state of the FP pipeline


def test_parse():
    workspace = parse_metadata("examples/multi_workspace/workspaces/alexb").unwrap()
    assert exists(workspace, "us_cities")
