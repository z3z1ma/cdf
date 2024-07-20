import abc
import os
import string
import typing as t
from collections import ChainMap

from typing_extensions import ParamSpec

import cdf.injector as injector

T = t.TypeVar("T")
P = ParamSpec("P")


class AbstractWorkspace(abc.ABC):
    name: str
    version: str = "0.1.0"

    @abc.abstractmethod
    def get_environment(self) -> str:
        pass

    @abc.abstractmethod
    def get_config_sources(self) -> t.Iterable[injector.ConfigSource]:
        pass

    @abc.abstractmethod
    def get_services(self) -> t.Dict[injector.DependencyKey, injector.Dependency]:
        pass

    @property
    def cli(self) -> t.Callable:
        import click

        @click.command()
        def entrypoint():
            click.echo(f"Hello, {self.name} {self.version}!")

        return entrypoint


class Service(t.TypedDict):
    """A service that the workspace provides."""

    name: injector.DependencyKey
    dependency: injector.Dependency
    description: str
    owner: str


class Workspace(AbstractWorkspace):
    """A CDF workspace that allows for dependency injection."""

    name: str
    version: str = "0.1.0"

    def __init__(
        self,
        dependency_registry: injector.DependencyRegistry = injector.DependencyRegistry(),
        configuration: injector.ConfigResolver = injector.ConfigResolver(),
    ) -> None:
        """Initialize the workspace."""
        self.injector = dependency_registry

        for source in self.get_config_sources():
            configuration.import_(source)
        configuration.set_environment(self.get_environment())
        self.add_dependency(
            "cdf_config", injector.Dependency.from_instance(configuration)
        )
        self.configuration = configuration

        self._services = self.get_services()
        for service in self._services:
            if callable(service["dependency"].factory):
                service["dependency"] = injector.Dependency(
                    configuration.inject_defaults(service["dependency"].factory),
                    *service["dependency"][1:],
                )
            self.add_dependency(service["name"], service["dependency"])

        # TODO: Now we add sources which depend on services

    def get_environment(self) -> str:
        """Return the environment of the workspace."""
        return os.getenv("CDF_ENVIRONMENT", "dev")

    def get_config_sources(self) -> t.Iterable[injector.ConfigSource]:
        """Return an iterable of configuration sources."""
        return ["cdf.toml", "cdf.yaml", "cdf.json", "~/.cdf.toml"]

    def get_services(self) -> t.Iterable[Service]:
        """Return a iterable of services that the workspace provides."""
        return []

    def add_dependency(
        self, name: injector.DependencyKey, definition: injector.Dependency
    ) -> None:
        """Add a dependency to the workspace DI container."""
        self.injector.add_definition(name, definition)

    def import_config(self, config: t.Dict[str, t.Any]) -> None:
        """Import a configuration dictionary into the workspace."""
        self.configuration.import_(config)

    def __call__(
        self, func_or_cls: t.Callable[P, T], *args: t.Any, **kwargs: t.Any
    ) -> T:
        """Invoke a function with configuration and dependencies defined in the workspace."""
        configured = self.configuration.inject_defaults(func_or_cls)
        return self.injector.wire(configured)(*args, **kwargs)


if __name__ == "__main__":
    # Example usage

    # Modularity is acheived through regular Python modules

    # from workspaces.datateam.services import SFDC, Asana, BigQuery, Snowflake
    # from workspaces.datateam.config import CONFIG
    # from workspaces.datateam.sources import salesforce_source

    # Define a workspace
    class DataTeamWorkspace(Workspace):
        name = "data-team"
        version = "0.1.1"

        def get_services(self) -> t.List[Service]:
            # These can be used by simply using the name of the service in a function argument
            return [
                {
                    "name": "a",
                    "dependency": injector.Dependency(1),
                    "description": "A secret number",
                    "owner": "Alex",
                },
                {
                    "name": "b",
                    "dependency": injector.Dependency(lambda a: a + 1),
                    "description": "Add one to the secret number",
                    "owner": "Alex",
                },
                {
                    "name": "prod_bigquery",
                    "dependency": injector.Dependency("dwh-123"),
                    "description": "The production BigQuery project",
                    "owner": "DataTeam",
                },
                {
                    "name": "sfdc",
                    "dependency": injector.Dependency(
                        injector.map_section("sfdc")(
                            lambda username: f"https://sfdc.com/{username}"
                        )
                    ),
                    "description": "The primary Salesforce instance",
                    "owner": "RevOps",
                },
            ]

        def get_config_sources(self) -> t.List[injector.ConfigSource]:
            return [
                {
                    "sfdc": {"username": "abc"},
                    "bigquery": {"project_id": ...},
                },
                *super().get_config_sources(),
            ]

    # Create an instance of the workspace
    datateam = DataTeamWorkspace()

    @injector.map_values(secret_number="a.b.c")
    def c(secret_number: int, sfdc: str) -> int:
        print(f"SFDC: {sfdc=}")
        return secret_number * 10

    # Imperatively add dependencies or config if needed
    datateam.add_dependency("c", injector.Dependency(c))
    datateam.configuration.import_({"a.b.c": 10})

    def source_a(a: int, prod_bigquery: str):
        print(f"Source A: {a=}, {prod_bigquery=}")

    # Some interface examples
    print(datateam(source_a))
    print(datateam.name)
    print(datateam.configuration["sfdc.username"])
    print(datateam.injector.get_or_raise("sfdc"))
    print(datateam(c))

    # Run the autogenerated CLI
    datateam.cli()
