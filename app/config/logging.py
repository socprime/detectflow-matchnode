"""
Structured logging configuration using structlog

Provides consistent, machine-parseable logging across the application
with human-readable console output for development.
"""

import builtins
import logging
import sys
from typing import Any

import structlog

# Monkey-patch print() to ALWAYS ignore flush parameter
# This is necessary because Flink's CustomPrint doesn't support it
_original_print = builtins.print


def _flink_safe_print(*args, **kwargs):
    """Print function that ignores flush parameter for Flink's CustomPrint"""
    # Remove flush parameter if present
    kwargs.pop("flush", None)
    return _original_print(*args, **kwargs)


# Apply monkey-patch globally - it's safe even if not in Flink environment
builtins.print = _flink_safe_print


def configure_logging(log_level: str = "INFO", log_format: str = "console") -> None:
    """
    Configure structlog with sensible defaults

    Features:
    - Structured logging with key-value pairs
    - ISO8601 timestamps
    - Colored output for console (development)
    - JSON output for log aggregation (production)
    - Integration with standard logging
    - Context variables support

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Output format:
            - "console": Human-readable colored output (default)
            - "json": JSON format for Fluent Bit, Elasticsearch, etc.
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Configure standard logging (for compatibility with libraries)
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=numeric_level,
    )

    # Suppress noisy libraries
    logging.getLogger("apache_beam").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.WARNING)

    # Configure structlog processors
    shared_processors = [
        # Add log level
        structlog.stdlib.add_log_level,
        # Add logger name
        structlog.stdlib.add_logger_name,
        # Add timestamp in ISO8601 format
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        # Stack info on exceptions
        structlog.processors.StackInfoRenderer(),
        # Format exceptions
        structlog.processors.format_exc_info,
        # Decode unicode
        structlog.processors.UnicodeDecoder(),
    ]

    # Choose renderer based on format
    if log_format.lower() == "json":
        # JSON format for log aggregation (Fluent Bit, Loki, Elasticsearch)
        renderer = structlog.processors.JSONRenderer()
    else:
        # Console format - no colors in Flink environment (CustomPrint doesn't support it)
        # Check if we're in a Flink Python UDF environment
        in_flink_env = hasattr(sys.stdout, "print") and "CustomPrint" in type(sys.stdout).__name__
        renderer = structlog.dev.ConsoleRenderer(
            colors=False if in_flink_env else sys.stdout.isatty(),
            exception_formatter=structlog.dev.plain_traceback,
        )

    structlog.configure(
        processors=[
            *shared_processors,
            # Prepare for stdlib logging
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        # Logger factory
        logger_factory=structlog.stdlib.LoggerFactory(),
        # Cache loggers
        cache_logger_on_first_use=True,
    )

    # Configure the formatter for standard logging integration
    formatter = structlog.stdlib.ProcessorFormatter(
        # These run ONLY on `logging` entries
        foreign_pre_chain=shared_processors,
        # These run on ALL entries (structlog + logging)
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    # Update root logger handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(numeric_level)


def get_logger(name: str | None = None) -> Any:
    """
    Get a structlog logger instance

    Args:
        name: Logger name (usually __name__)

    Returns:
        Structlog logger with bound context

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("processing_started", event_count=100, window_id=5)
    """
    return structlog.get_logger(name)


def bind_context(**kwargs: Any) -> None:
    """
    Bind context variables to all subsequent log calls in this thread

    Useful for adding instance_id, key, or correlation_id that should
    appear in all logs.

    Args:
        **kwargs: Key-value pairs to bind

    Example:
        >>> bind_context(instance_id=2, key="dc01")
        >>> logger.info("window_started")  # Will include instance_id and key
    """
    structlog.contextvars.bind_contextvars(**kwargs)


def unbind_context(*keys: str) -> None:
    """
    Remove context variables

    Args:
        *keys: Keys to unbind

    Example:
        >>> unbind_context("instance_id", "key")
    """
    structlog.contextvars.unbind_contextvars(*keys)


def clear_context() -> None:
    """Clear all context variables"""
    structlog.contextvars.clear_contextvars()
