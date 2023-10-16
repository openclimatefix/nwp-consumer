"""Config struct for application running."""

import os
from typing import get_type_hints

import structlog

log = structlog.getLogger()


class EnvParser:
    """Mixin to parse environment variables into class fields."""

    def __init__(self) -> None:
        for field, _ in get_type_hints(self).items():
            # Skip item if not upper case
            if not field.isupper():
                continue

            # Log Error if required field not supplied
            default_value = getattr(self, field, None)
            if default_value is None and os.environ.get(field) is None:
                log.warn(
                    event="environment variable not set",
                    variable=field,
                )
                default_value = ""
            # Cast env var value to string
            value = str(os.environ.get(field, default_value))
            self.__setattr__(field, value)


# --- Inputs environment variables --- #


class CEDAEnv(EnvParser):
    """Config for CEDA FTP server."""

    CEDA_FTP_USER: str
    CEDA_FTP_PASS: str


class MetOfficeEnv(EnvParser):
    """Config for Met Office API."""

    METOFFICE_ORDER_ID: str
    METOFFICE_CLIENT_ID: str
    METOFFICE_CLIENT_SECRET: str


class ECMWFMARSEnv(EnvParser):
    """Config for ECMWF MARS API."""

    ECMWF_API_KEY: str
    ECMWF_API_URL: str
    ECMWF_API_EMAIL: str
    ECMWF_AREA: str = "uk"
    ECMWF_HOURS: str = "48"

# --- Outputs environment variables --- #


class LocalEnv(EnvParser):
    """Config for local storage."""

    # Required for EnvParser to believe it's a valid class
    dummy_field: str = ""


class S3Env(EnvParser):
    """Config for S3."""

    AWS_S3_BUCKET: str
    AWS_ACCESS_KEY: str
    AWS_ACCESS_SECRET: str
    AWS_REGION: str


class HuggingFaceEnv(EnvParser):
    """Config for HuggingFace API."""

    HUGGINGFACE_TOKEN: str
    HUGGINGFACE_REPO_ID: str

