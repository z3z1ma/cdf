from . import find_nearest, get_workspace_from_path

settings = (
    get_workspace_from_path(".")
    .unwrap_or(find_nearest(".").unwrap())
    .configuration.maps[0]
)
