"""
The workspace module is responsible for generating the Workspace data structure
"""
import itertools
import typing as t
from operator import attrgetter
from pathlib import Path

import dotenv
import tomlkit

import cdf.core.constants as c
import cdf.core.logger as logger
from cdf.core.monads import Just, Maybe, Nothing, Result, State
from cdf.core.parser import ParsedComponent, process_script

PathLike = t.Union[str, Path]


class WorkspaceDoesNotExist(ValueError):
    """An error raised when a workspace does not exist."""


class InvalidWorkspace(ValueError):
    """An error raised when a workspace is invalid."""


class InvalidProjectDefinition(ValueError):
    """An error raised when a project definition is invalid."""


class Workspace(t.NamedTuple):
    root: Path
    pipelines: t.Tuple[ParsedComponent, ...] = ()
    publishers: t.Tuple[ParsedComponent, ...] = ()
    scripts: t.Tuple[ParsedComponent, ...] = ()
    notebooks: t.Tuple[ParsedComponent, ...] = ()

    @property
    def name(self) -> str:
        return self.root.name

    def search(
        self,
        name: str,
        key: t.Literal[
            "pipelines", "publishers", "scripts", "notebooks", "all"
        ] = "all",
    ) -> Maybe[ParsedComponent]:
        """Finds a component by name."""
        if key == "all":
            candidates = itertools.chain(
                self.pipelines,
                self.publishers,
                self.scripts,
                self.notebooks,
            )
        else:
            candidates = getattr(self, key)
        for component in candidates:
            if component.name == name:
                return Just(component)
        return Nothing()

    def exists(self, name: str) -> bool:
        """Checks if a component exists by name."""
        return self.search(name).is_just()


@Result.lift
def process_directory(path: PathLike) -> Workspace:
    """Parses a workspace from a given path."""
    path = Path(path)
    if not path.exists():
        raise WorkspaceDoesNotExist(f"Directory not found: {path}")
    if not path.is_dir():
        raise InvalidWorkspace(f"Workspace argument must be a directory: {path}")

    def _process(subdirectory: str) -> t.Tuple[str, t.Tuple[ParsedComponent, ...]]:
        p = path.joinpath(subdirectory)
        if not p.exists():
            return (subdirectory, ())
        vec = []
        for f in p.glob("**/*.py"):
            script, err = process_script(f).to_parts()
            if err:
                logger.debug(err)
                continue
            vec.append(script)
        vec.sort(key=attrgetter("name"))
        return (subdirectory, tuple(vec))

    return Workspace(
        path, **dict(map(_process, (c.PIPELINES, c.PUBLISHERS, c.SCRIPTS, c.NOTEBOOKS)))
    )


class Project(t.NamedTuple):
    root: Path
    members: t.Tuple[Workspace, ...]

    @property
    def name(self) -> str:
        return self.root.name

    def search(self, name: str) -> Maybe[Workspace]:
        """Finds a workspace by name."""
        for member in self.members:
            if member.name == name:
                return Just(member)
        return Nothing()


@Result.lift
def load_project(path: PathLike) -> Project:
    """Loads a project"""
    path = Path(path)
    dotenv.load_dotenv(path.joinpath(".env"))

    project_file = path.joinpath(c.PROJECT_FILE)
    if not project_file.exists():
        return Project(path, (process_directory(path).unwrap(),))

    try:
        with project_file.open() as f:
            doc = tomlkit.parse(f.read())
        members = doc.value["project"]["members"]
    except KeyError as key_err:
        raise InvalidProjectDefinition(
            f"Invalid project definition: {project_file}"
        ) from key_err
    except Exception as e:
        raise InvalidProjectDefinition(
            f"Failed to load project definition: {project_file}"
        ) from e

    return Project(
        path,
        tuple(process_directory(path.joinpath(member)).unwrap() for member in members),
    )
