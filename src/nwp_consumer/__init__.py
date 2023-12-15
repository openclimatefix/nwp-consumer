"""Logging configuration for the application."""

import logging
import os
import sys

import psutil
import structlog

# Ignore modules' emitted logs
for name in (
    "boto",
    "elasticsearch",
    "urllib3",
    "cfgrib",
    "xarray",
    "ecmwfapi",
    "api",
    "multiprocessing",
):
    logging.getLogger(name).setLevel(logging.ERROR)

# Set the log level
LOGLEVEL = os.getenv("LOGLEVEL", "INFO").upper()
_nameToLevel = {
    "CRITICAL": logging.CRITICAL,
    "FATAL": logging.FATAL,
    "ERROR": logging.ERROR,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}


class UsageProfiler:
    """Add CPU and RAM usage to the log event."""

    def __call__(
        self,
        logger: structlog.types.WrappedLogger,  # noqa: ARG002
        name: str, # noqa: ARG002
        event_dict: structlog.types.EventDict,
    ) -> structlog.types.EventDict:
        """Override the default structlog processor to add CPU and RAM usage to the log event."""
        event_dict["cpu"] = psutil.cpu_percent(1)
        event_dict["ram"] = psutil.virtual_memory().used / 1024 / 1024
        return event_dict

shared_processors = [
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.CallsiteParameterAdder(
        [
            structlog.processors.CallsiteParameter.FILENAME,
            structlog.processors.CallsiteParameter.LINENO,
        ],
    ),
    structlog.stdlib.add_log_level,
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.processors.StackInfoRenderer(),
    UsageProfiler(),
]

if sys.stderr.isatty():
    # Pretty printing when we run in a terminal session.
    # Automatically prints pretty tracebacks when "rich" is installed
    processors = [
        *shared_processors,
        structlog.dev.ConsoleRenderer(),
    ]

else:
    # Print JSON when we run, e.g., in a Docker container.
    # Also print structured tracebacks.
    processors = [
        *shared_processors,
        structlog.processors.EventRenamer("message", replace_by="_event"),
        structlog.processors.dict_tracebacks,
        structlog.processors.JSONRenderer(sort_keys=True),
    ]

# Add required processors and formatters to structlog
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(_nameToLevel[LOGLEVEL]),
    processors=processors,
)
