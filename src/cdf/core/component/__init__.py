import cdf.core.constants as c
from cdf.core.component._model.loader import CDFTransformLoader
from cdf.core.component.pipeline import CDFPipeline, pipeline_spec
from cdf.core.component.publisher import PublisherData, publisher_spec
from cdf.core.component.sink import destination, gateway, sink_spec


def export(
    *components: pipeline_spec | publisher_spec | sink_spec, scope: dict | None = None
) -> None:
    """Export components to the callers global scope.

    Args:
        *sinks (sink_spec): The sinks to export.
        scope (dict | None, optional): The scope to export to. Defaults to globals().
    """
    if scope is None:
        import inspect

        frame = inspect.currentframe()
        if frame is not None:
            frame = frame.f_back
        if frame is not None:
            scope = frame.f_globals

    for component in components:
        if isinstance(component, pipeline_spec):
            namespace = c.CDF_PIPELINES
        elif isinstance(component, publisher_spec):
            namespace = c.CDF_PUBLISHERS
        elif isinstance(component, sink_spec):
            namespace = c.CDF_SINKS
        else:
            raise ValueError(
                f"Tried to export invalid cdf component of type {type(component)}"
            )

        (scope or globals()).setdefault(namespace, []).append(component)


__all__ = [
    "pipeline_spec",
    "publisher_spec",
    "sink_spec",
    "export",
    "gateway",
    "destination",
    "CDFPipeline",
    "CDFTransformLoader",
    "PublisherData",
]
