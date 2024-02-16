"""The workspace module is responsible for generating the immutable Project/Workspace data structure."""
import itertools
import sys
import typing as t
from operator import attrgetter
from pathlib import Path

import dotenv
from immutabledict import immutabledict

import cdf.core.constants as c
import cdf.core.exceptions as ex
import cdf.core.logger as logger
import cdf.core.sandbox as sandbox
from cdf.core.monads import Err, Ok, Result
from cdf.core.parser import ParsedComponent, process_definition, process_script

PathLike = t.Union[str, Path]


class DoesNotExist(ex.CDFError, ValueError):
    """An error raised when something does not exist."""


class WorkspaceDoesNotExist(DoesNotExist):
    """An error raised when a workspace does not exist."""


class ComponentDoesNotExist(DoesNotExist):
    """An error raised when a component does not exist."""


class InvalidWorkspace(ex.CDFError, ValueError):
    """An error raised when a workspace is invalid."""


class InvalidProjectDefinition(ex.CDFError, ValueError):
    """An error raised when a project definition is invalid."""


class Workspace(t.NamedTuple):
    root: Path
    pipelines: t.Tuple[ParsedComponent, ...] = ()
    publishers: t.Tuple[ParsedComponent, ...] = ()
    scripts: t.Tuple[ParsedComponent, ...] = ()
    notebooks: t.Tuple[ParsedComponent, ...] = ()
    sinks: t.Tuple[ParsedComponent, ...] = ()
    meta: immutabledict = immutabledict()

    @property
    def name(self) -> str:
        maybe_name = self.meta.get("name")
        if maybe_name is not None:
            return maybe_name.unwrap_or(self.root.name)
        return self.root.name

    def search(
        self,
        name: str,
        key: t.Literal[
            "pipelines", "publishers", "scripts", "notebooks", "sinks", "all"
        ] = "all",
    ) -> Result[ParsedComponent, DoesNotExist]:
        """Finds a component by name."""
        if key == "all":
            candidates = itertools.chain(
                self.pipelines,
                self.publishers,
                self.scripts,
                self.notebooks,
                self.sinks,
            )
        else:
            candidates = getattr(self, key)
        for component in candidates:
            if component.name == name:
                return Ok(component)
        return Err(ComponentDoesNotExist(f"Component not found: {name}"))

    def exists(self, name: str) -> bool:
        """Checks if a component exists by name."""
        return self.search(name).is_ok()


@Result.lift
def process_directory(path: PathLike) -> Workspace:
    """Parses a workspace from a given path."""
    path = Path(path).resolve()
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
        path,
        **dict(
            map(_process, (c.PIPELINES, c.PUBLISHERS, c.SCRIPTS, c.NOTEBOOKS, c.SINKS))
        ),
        meta=(
            process_definition(path / c.WORKSPACE_FILE)
            .map(lambda def_: def_.specification)
            .unwrap_or(immutabledict(name=Ok(path.name)))
        ),
    )


class Project(t.NamedTuple):
    root: Path
    members: t.Tuple[Workspace, ...]
    meta: immutabledict = immutabledict()

    @property
    def name(self) -> str:
        maybe_name = self.meta.get("name")
        if maybe_name is not None:
            return maybe_name.unwrap_or(self.root.name)
        return self.root.name

    def search(self, name: str) -> Result[Workspace, DoesNotExist]:
        """Finds a workspace by name."""
        for member in self.members:
            if member.name == name:
                return Ok(member)
        return Err(WorkspaceDoesNotExist(f"Workspace not found: {name}"))

    @classmethod
    def from_cwd(cls) -> "Project":
        cwd = Path.cwd()
        return Project(
            cwd,
            (process_directory(cwd).unwrap(),),
            meta=immutabledict({"name": Ok(cwd.name)}),
        )


def find_nearest(path: PathLike = ".") -> Result[Project, ex.CDFError]:
    """Loads a project"""
    path = Path(path).resolve()
    dotenv.load_dotenv(path.joinpath(".env"))

    project_file = path.joinpath(c.PROJECT_FILE)
    if not project_file.exists():
        if not path.parents:
            return Ok(Project.from_cwd())
        return find_nearest(path.parent)

    try:
        def_ = process_definition(project_file).unwrap()
        members = def_.specification["members"].unwrap()
    except KeyError as key_err:
        raise InvalidProjectDefinition(
            f"Invalid project definition: {project_file}"
        ) from key_err
    except Exception as e:
        raise InvalidProjectDefinition(
            f"Failed to load project definition: {project_file}"
        ) from e

    return Ok(
        Project(
            path,
            tuple(
                process_directory(path.joinpath(member)).unwrap() for member in members
            ),
            meta=def_.specification,
        )
    )


class HasRoot(t.Protocol):
    @property
    def root(self) -> Path:
        ...


T = t.TypeVar("T", bound=HasRoot)


def augment_sys_path(this: T) -> T:
    """Augments sys.path with the project/workspace root."""
    sys.path.append(str(this.root))
    return this


def get_gateway(
    project: Project, workspace: str, sink: str
) -> Result[t.Dict[str, t.Any], ex.CDFError]:
    """Gets a SQLMesh gateway from a project sink tuple. Useful in config.py"""
    return (
        Ok(project)
        .bind(lambda p: p.search(workspace))
        .bind(lambda w: w.search(sink, "sinks"))
        .bind(lambda c: sandbox.run(c.to_script()))
        .map(lambda ex: ex["sink"][2])
    )
