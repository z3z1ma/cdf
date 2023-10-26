"""All common exceptions for the cdf package."""


class RegistryTypeError(TypeError):
    ...


class RegistryAttributeError(AttributeError):
    ...


class SourceNotFoundError(RegistryAttributeError):
    ...


class SourceDirectoryNotFoundError(FileNotFoundError):
    ...


class SourceDirectoryEmpty(FileNotFoundError):
    ...
