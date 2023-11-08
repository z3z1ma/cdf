"""Logger for CDF"""
import logging
import typing as t

from rich.logging import RichHandler

if t.TYPE_CHECKING:

    class LogMethod(t.Protocol):
        """Protocol for logger methods."""

        def __call__(self, message: str, *args: t.Any, **kwargs: t.Any) -> None:
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


def configure(level: int = logging.DEBUG) -> None:
    """Configure logging.

    Args:
        level (int, optional): Logging level. Defaults to logging.INFO.
    """
    if LOGGER.extra.get("configured"):
        return
    set_level(level)
    console_handler = RichHandler(
        LOG_LEVEL,
        markup=True,
        rich_tracebacks=True,
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

    LOGGER.setLevel(LOG_LEVEL := level)


def __getattr__(name: str) -> "LogMethod":
    """Get a logger method from the package logger."""
    if not LOGGER.extra.get("configured"):
        configure()
    return getattr(LOGGER, name)
