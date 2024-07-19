import typing as t

from cdf.injector.registry import Dependency, DependencyRegistry, StringOrKey


class Workspace:
    """A CDF workspace that allows for dependency injection."""

    name: str
    version: str = "0.1.0"

    def __init__(self) -> None:
        """Initialize the workspace."""
        self.injector = DependencyRegistry()
        for name, definition in self.get_services().items():
            self.add_dependency(name, definition)

    def get_services(self) -> t.Dict[StringOrKey, Dependency]:
        """Return a dictionary of services that the workspace provides."""
        return {}

    def add_dependency(self, name: StringOrKey, definition: Dependency) -> None:
        """Add a dependency to the workspace."""
        self.injector.add_definition(name, definition)


class DataTeamWorkspace(Workspace):
    name = "data-team"

    def get_services(self):
        return {
            "a": Dependency(1),
            "b": Dependency(lambda a: a + 1),
            "prod_bigquery": Dependency("dwh-123"),
        }


datateam = DataTeamWorkspace()


def c(b: int) -> int:
    return b * 10


datateam.add_dependency("c", Dependency(c))


def source_a(a: int, prod_bigquery: str):
    print(f"Source A: {a=}, {prod_bigquery=}")


print(datateam.injector(source_a))
print(datateam.name)
