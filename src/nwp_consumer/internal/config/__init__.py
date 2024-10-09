"""Configuration for the service.

The service is configured via environment variables in accordance with the
12-factor philosophy.
"""

from .config import AppConfig, parse

__all__ = [
    "AppConfig",
    "parse",
]
