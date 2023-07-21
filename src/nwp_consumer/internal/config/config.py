"""Config struct for application running."""

import os
from distutils.util import strtobool
from typing import get_type_hints
import structlog

log = structlog.getLogger()


class _EnvParseMixin:
    """Mixin to parse environment variables into class fields."""

    def __init__(self):
        for field, _ in get_type_hints(self).items():
            # Skip item if not upper case
            if not field.isupper():
                continue

            # Log Error if required field not supplied
            default_value = getattr(self, field, None)
            if default_value is None and os.environ.get(field) is None:
                log.warn(
                    event=f"environment variable not set",
                    variable=field,
                )
                default_value = ""
            # Cast env var value to string
            value = str(os.environ.get(field, default_value))
            self.__setattr__(field, value)


class CEDAConfig(_EnvParseMixin):
    """Config for CEDA FTP server."""

    CEDA_FTP_USER: str
    CEDA_FTP_PASS: str


class MetOfficeConfig(_EnvParseMixin):
    """Config for Met Office API."""

    METOFFICE_ORDER_ID: str
    METOFFICE_CLIENT_ID: str
    METOFFICE_CLIENT_SECRET: str


class S3Config(_EnvParseMixin):
    """Config for S3."""

    AWS_S3_BUCKET: str
    AWS_ACCESS_KEY: str
    AWS_ACCESS_SECRET: str
    AWS_REGION: str
