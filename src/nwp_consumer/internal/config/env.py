"""Config struct for application running."""
import os
from distutils.util import strtobool
from typing import Literal, get_type_hints

import structlog

log = structlog.getLogger()


class EnvParser:
    """Mixin to parse environment variables into class fields.

    Whilst this could be done with Pydantic, it's nice to avoid the
    extra dependency if possible, and pydantic would be overkill for
    this small use case.
    """

    def __init__(self) -> None:
        """Parse environment variables into class fields.

        If the class field is upper case, parse it into the indicated
        type from the environment. Required fields are those set in
        the child class without a default value.

        Examples:
        >>> MyEnv(EnvParser):
        >>>     REQUIRED_ENV_VAR: str
        >>>     OPTIONAL_ENV_VAR: str = "default value"
        >>>     ignored_var: str = "ignored"
        """
        for field, t in get_type_hints(self).items():
            # Skip item if not upper case
            if not field.isupper():
                continue

            # Log Error if required field not supplied
            default_value = getattr(self, field, None)
            match (default_value, os.environ.get(field)):
                case (None, None):
                    # No default value, and field not in env
                    raise OSError(f"Required field {field} not supplied")
                case (_, None):
                    # A default value is set and field not in env
                    pass
                case (_, _):
                    # Field is in env
                    env_value: str | bool = os.environ[field]
                    # Handle bools seperately as bool("False") == True
                    if t == bool:
                        env_value = bool(strtobool(os.environ[field]))
                    # Cast to desired type
                    self.__setattr__(field, t(env_value))


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
    ECMWF_HOURS: int = 48
    ECMWF_PARAMETER_GROUP: str = "default"


class ICONEnv(EnvParser):
    """Config for ICON API."""

    ICON_MODEL: str = "europe"
    ICON_HOURS: int = 48
    ICON_PARAMETER_GROUP: str = "default"


# --- Outputs environment variables --- #


class LocalEnv(EnvParser):
    """Config for local storage."""

    # Required for EnvParser to believe it's a valid class
    dummy_field: str = ""


class S3Env(EnvParser):
    """Config for S3."""

    AWS_S3_BUCKET: str
    AWS_ACCESS_KEY: str = ""
    AWS_ACCESS_SECRET: str = ""
    AWS_REGION: str


class HuggingFaceEnv(EnvParser):
    """Config for HuggingFace API."""

    HUGGINGFACE_TOKEN: str
    HUGGINGFACE_REPO_ID: str
