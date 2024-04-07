from . import find_nearest, get_workspace

settings = (
    get_workspace(".").unwrap_or(find_nearest(".").unwrap()).configuration.maps[0]
)
