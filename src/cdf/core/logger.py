"""Logger for CDF"""

from __future__ import annotations

import contextlib
import logging
import typing as t
import warnings

from rich.logging import RichHandler

if t.TYPE_CHECKING:

    class Representable(t.Protocol):
        def __str__(self) -> str: ...

    class LogMethod(t.Protocol):
        """Protocol for logger methods."""

        def __call__(
            self, msg: Representable, *args: t.Any, **kwargs: t.Any
        ) -> None: ...


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
def create(name: t.Literal["cdf"] | None) -> CDFLoggerAdapter: ...


@t.overload
def create(name: str) -> logging.Logger: ...


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


def log_level() -> str:
    """Returns current log level"""
    return logging.getLevelName(LOGGER.logger.level)


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


@contextlib.contextmanager
def suppress_and_warn() -> t.Iterator[None]:
    """Suppresses exception and logs it as warning"""
    try:
        yield
    except Exception:
        LOGGER.warning("Suppressed exception", exc_info=True)


@contextlib.contextmanager
def mute() -> t.Iterator[None]:
    """Mute the logger."""
    LOGGER.logger.disabled = True
    try:
        yield
    finally:
        LOGGER.logger.disabled = False


def __getattr__(name: str) -> "LogMethod":
    """Get a logger method from the package logger."""
    if not LOGGER.extra.get("configured"):
        configure()

    def wrapper(msg: "Representable", *args: t.Any, **kwargs: t.Any) -> None:
        stacklevel = 3 if name == "exception" else 2
        getattr(LOGGER, name)(msg, *args, **kwargs, stacklevel=stacklevel)

    return wrapper


def _monkeypatch_dlt() -> None:
    """Monkeypatch the dlt logging module."""
    from dlt.common import logger

    patched = create("dlt")
    setattr(logger, "_init_logging", lambda *a, **kw: patched)
    setattr(logger, "LOGGER", patched)


def _monkeypatch_sqlglot() -> None:
    """Monkeypatch the sqlglot logging module."""
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


def apply_patches() -> None:
    """Apply logger patches."""
    _monkeypatch_dlt()
    _monkeypatch_sqlglot()
