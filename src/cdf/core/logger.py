"""Logger for CDF"""
import logging
import typing as t
import warnings

from rich.logging import RichHandler

if t.TYPE_CHECKING:

    class Representable(t.Protocol):
        def __str__(self) -> str:
            ...

    class LogMethod(t.Protocol):
        """Protocol for logger methods."""

        def __call__(
            self, message: Representable, *args: t.Any, **kwargs: t.Any
        ) -> None:
            ...


__all__ = [
    "configure",
    "create",
    "set_level",
    "LOG_LEVEL",
    "LOGGER",
]


class CDFLoggerAdapter(logging.LoggerAdapter):
    extra: t.Dict[str, t.Any]
    logger: logging.Logger


LOGGER = CDFLoggerAdapter(logging.getLogger("cdf"), {})
"""CDF logger instance."""

LOG_LEVEL = logging.INFO
"""The active log level for CDF."""


def configure(level: int | str = logging.INFO) -> None:
    """Configure logging.

    Args:
        level (int, optional): Logging level. Defaults to logging.INFO.
    """
    if LOGGER.extra.get("configured"):
        return
    LOGGER.setLevel(LOG_LEVEL := level)
    console_handler = RichHandler(
        LOG_LEVEL,
        markup=True,
        rich_tracebacks=True,
        omit_repeated_times=False,
    )
    LOGGER.logger.addHandler(console_handler)
    LOGGER.extra["configured"] = True


@t.overload
def create(name: t.Literal["cdf"] | None) -> CDFLoggerAdapter:
    ...


@t.overload
def create(name: str) -> logging.Logger:
    ...


def create(name: str | None = None) -> CDFLoggerAdapter | logging.Logger:
    """Get or create a logger.

    Args:
        name (str, optional): The name of the logger. If None, the package logger is
            returned. Defaults to None. If a name is provided, a child logger is
            created.

    Returns:
        The logger.
    """
    if name is None:
        return LOGGER
    return LOGGER.logger.getChild(name)


def set_level(level: int | str) -> None:
    """Set the package log level.

    Args:
        level (int | str): The new log level.

    Raises:
        ValueError: If the log level is not valid.
    """
    global LOG_LEVEL

    if not LOGGER.extra.get("configured"):
        configure(LOG_LEVEL := level)
    else:
        LOGGER.setLevel(LOG_LEVEL := level)


def __getattr__(name: str) -> "LogMethod":
    """Get a logger method from the package logger."""
    if not LOGGER.extra.get("configured"):
        configure()
    return getattr(LOGGER, name)


def monkeypatch_dlt() -> None:
    """Monkeypatch the dlt logging module."""
    from dlt.common.runtime import logger

    patched = create("dlt")
    setattr(logger, "_init_logging", lambda *a, **kw: patched)
    setattr(logger, "LOGGER", patched)


def monkeypatch_sqlglot() -> None:
    logger = logging.getLogger("sqlglot")
    patched = create("sqlglot")
    logger.handlers = patched.handlers
    logger.setLevel(logging.ERROR)
    logger.propagate = False
    warnings.filterwarnings(
        "ignore",
        message=r"^Possible nested set .*",
        category=FutureWarning,
        module="sqlglot",
    )
