import os
import string
import typing as t
from collections import ChainMap

from dynaconf import Dynaconf, LazySettings
from dynaconf.vendor.box import Box

from cdf.injector import (
    ConfigResolver,
    ConfigSource,
    Dependency,
    DependencyRegistry,
    StringOrKey,
)


class Workspace:
    """A CDF workspace that allows for dependency injection."""

    name: str
    version: str = "0.1.0"

    def __init__(
        self,
        dependency_registry: DependencyRegistry = DependencyRegistry(),
        config_resolver: ConfigResolver = ConfigResolver(),
    ) -> None:
        """Initialize the workspace."""
        self.injector = dependency_registry

        for source in self.get_config_sources():
            config_resolver.import_(source)
        config_resolver.set_environment(self.get_environment())
        self.add_dependency("cdf_config", Dependency.from_instance(config_resolver))
        self.config_resolver = config_resolver

        for name, definition in self.get_services().items():
            if callable(definition.factory):
                definition = Dependency(
                    config_resolver.inject_defaults(definition.factory), *definition[1:]
                )
            self.add_dependency(name, definition)

    def get_environment(self) -> str:
        """Return the environment of the workspace."""
        return os.getenv("CDF_ENVIRONMENT", "dev")

    def get_config_sources(self) -> t.Iterable[ConfigSource]:
        """Return a sequence of configuration sources."""
        return ["cdf.toml", "cdf.yaml", "cdf.json", "~/.cdf.toml"]

    def get_services(self) -> t.Dict[StringOrKey, Dependency]:
        """Return a dictionary of services that the workspace provides."""
        return {}

    def add_dependency(self, name: StringOrKey, definition: Dependency) -> None:
        """Add a dependency to the workspace DI container."""
        self.injector.add_definition(name, definition)


class DataTeamWorkspace(Workspace):
    name = "data-team"
    version = "0.1.1"

    def get_services(self):
        return {
            "a": Dependency(1),
            "b": Dependency(lambda a: a + 1),
            "prod_bigquery": Dependency("dwh-123"),
            "sfdc": Dependency(
                ConfigResolver.map_section("sfdc")(
                    lambda username: f"https://sfdc.com/{username}"
                )
            ),
        }

    def get_config_sources(self):
        return [
            {
                "sfdc": {"username": "abc"},
                "bigquery": {"project_id": ...},
            },
            *super().get_config_sources(),
        ]


datateam = DataTeamWorkspace()


@ConfigResolver.map_values(b="a.b.c")
def c(b: int) -> int:
    return b * 10


datateam.add_dependency("c", Dependency(c))


def source_a(a: int, prod_bigquery: str):
    print(f"Source A: {a=}, {prod_bigquery=}")


print(datateam.injector(source_a))
print(datateam.name)
print(datateam.config_resolver["sfdc.username"])

print(datateam.injector.get_or_raise("sfdc"))
