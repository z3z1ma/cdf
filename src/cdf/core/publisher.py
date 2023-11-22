import typing as t
from dataclasses import dataclass

from dlt.common.configuration import with_config
from dlt.common.configuration.specs.base_configuration import BaseConfiguration

import cdf.core.constants as c

T = t.TypeVar("T", bound=BaseConfiguration)


@dataclass
class publisher_spec(t.Generic[T]):
    runner: t.Callable[[T], None]
    from_model: str
    mapping: t.Dict[str, str]
    version: int = 1
    owners: t.Sequence[str] = ()
    description: str = ""
    tags: t.Sequence[str] = ()
    cron: str | None = None
    enabled = True
    config: t.Type[T] = t.Any

    def __post_init__(self) -> None:
        runner = self.runner

        @with_config(spec=self.config, sections=("publishers", runner.__name__))
        def _runner(config: T) -> None:
            runner(config)

        _runner.__wrapped__ = runner
        self.runner = _runner


def export_publishers(
    *, scope: dict | None = None, **publishers: publisher_spec
) -> None:
    """Export publishers to the global scope.

    Args:
        scope (dict | None, optional): The scope to export to. Defaults to globals().
        **publishers (publisher_spec): The publishers to export.
    """
    (scope or globals()).setdefault(c.CDF_PUBLISHER, {}).update(publishers)
