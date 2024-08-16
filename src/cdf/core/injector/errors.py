class DependencyCycleError(Exception):
    """Raised when a dependency cycle is detected."""

    pass


class DependencyMutationError(Exception):
    """Raised when an instance/singleton dependency has already been resolved but a mutation is attempted."""

    pass
