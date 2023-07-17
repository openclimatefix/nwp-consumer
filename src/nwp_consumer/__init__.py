import os

import structlog
import logging

# Ignore modules' emitted logs
for _ in ("boto", "elasticsearch", "urllib3", "cfgrib", "xarray"):
    logging.getLogger(_).setLevel(logging.CRITICAL)

# Set the log level
LOGLEVEL = os.getenv("LOGLEVEL", "INFO").upper()
_nameToLevel = {
    'CRITICAL': logging.CRITICAL,
    'FATAL': logging.FATAL,
    'ERROR': logging.ERROR,
    'WARN': logging.WARNING,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'NOTSET': logging.NOTSET,
}

# Add required processors and formatters to structlog
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(_nameToLevel[LOGLEVEL]),
    processors=[
        structlog.processors.EventRenamer("message", replace_by="_event"),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.CallsiteParameterAdder(
            [
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.LINENO
            ],
        ),
        structlog.processors.dict_tracebacks,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(sort_keys=True),
    ],
)

