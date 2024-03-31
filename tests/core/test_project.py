import pathlib
import typing as t

import dlt
import dlt.common.configuration

import cdf.core.context as context
from cdf.core.configuration import load_config
from cdf.core.feature_flag import load_feature_flag_provider
from cdf.core.filesystem import load_filesystem_provider


# Q: Is a Project a subclass of a Workspace? or vice versa?
#    No, instead we rely on mixins which are shared between Project & Workspace
def test_project_ideal_interface():
    r = (
        # A project class is created by get_project
        # This class defines the top-level configuration shared by all workspaces
        # it also injects itself into the dlt config providers container until __del__
        get_project("examples/sandbox")
        # A workspace class is created by get_workspace
        # This class has config defined as a ChainMap of the workspace and project settings
        # It also stores its root path
        >> (lambda project: project.get_workspace("workspace1"))
        # We can then access the pipeline by name
        # This returns a PipelineSpecification object
        # and leverages the root path and workspace settings
        >> (lambda workspace: workspace.get_pipeline("us_cities"))
        # The run method sets all the necessary context variables such that
        # the cdf.pipeline function will function in the script as expected
        >> (lambda pipeline: pipeline.run())
    )


def test_project_interface():
    project = load_config("examples/sandbox").unwrap()

    context.active_workspace.set("workspace1")
    context.inject_cdf_config_provider(project)

    source_config = project["pipelines"]["us_cities"]

    @dlt.common.configuration.with_config(auto_pipeline_section=True)
    def foo(
        pipeline_name: str, x: int = dlt.config.value, y: int = dlt.config.value
    ) -> t.Tuple[int, int]:
        return (x, y)

    assert foo("us_cities") == (100, 2)

    dlt.config.config_providers[-1].set_value("test123", 123, "us_cities")
    assert dlt.config["pipelines.us_cities.options.test123"] == 123

    dlt.config.config_providers[-1].set_value("test123", 123, "")
    assert dlt.config["test123"] == project["test123"]

    # set in pipeline options, which is very interesting
    pipeline = dlt.pipeline("us_cities")
    assert pipeline.runtime_config["dlthub_telemetry"] is False
    assert pipeline.destination.destination_type.endswith("duckdb")

    ff = load_feature_flag_provider("file", options={"path": "feature_flags.json"})
    fs = load_filesystem_provider("file", options={"compress": True})

    p = pipeline("us_cities", source_config)  # TODO: we need the root path?


def test_project():
    from cdf.core.project import get_project

    pipeline_ = (
        get_project("examples/sandbox")
        .bind(lambda project: project.get_workspace("workspace1"))
        .bind(lambda workspace: workspace.get_pipeline("us_cities"))
        .unwrap()
    )
    pipeline_.main()
