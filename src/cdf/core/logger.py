"""Logger for CDF"""
import logging
import typing as t


class CDFLogger(logging.Logger):
    """Wrapper for logging.Logger with a configured flag."""

    configured = False


LOGGER = t.cast(CDFLogger, CDFLogger.manager.getLogger("cdf"))
"""CDF logger instance."""

LOG_LEVEL = logging.INFO
"""The active log level for CDF."""


def configure_logging(level: int = logging.INFO) -> None:
    """Configure logging.

    Args:
        level (int, optional): Logging level. Defaults to logging.INFO.
    """
    global LOG_LEVEL
    LOG_LEVEL = level
    LOGGER.setLevel(LOG_LEVEL)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(LOG_LEVEL)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    LOGGER.addHandler(console_handler)
    LOGGER.configured = True


@t.overload
def get_logger(name: t.Literal["cdf"] | None) -> CDFLogger:
    ...


@t.overload
def get_logger(name: str) -> logging.Logger:
    ...


def get_logger(name: str | None = None) -> CDFLogger | logging.Logger:
    """Get a logger.

    Args:
        name (str, optional): The name of the logger to get. Defaults to None (cdf).

    Returns:
        The logger.
    """
    if name is None:
        return LOGGER
    return logging.getLogger(name)


def set_log_level(level: int) -> None:
    """Set the package log level.

    Args:
        level (int): The new log level.

    Raises:
        ValueError: If the log level is not valid.
    """
    global LOG_LEVEL
    LOG_LEVEL = level
    LOGGER.setLevel(LOG_LEVEL)


class LogMethod(t.Protocol):
    """Protocol for logger methods."""

    def __call__(self, message: str, *args: t.Any, **kwargs: t.Any) -> None:
        ...


def __getattr__(name: str) -> LogMethod:
    """Get a logger method from the package logger."""
    if not LOGGER.configured:
        configure_logging()
    return getattr(LOGGER, name)
