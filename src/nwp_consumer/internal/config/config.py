"""Config struct for application running."""

import os
from distutils.util import strtobool
from typing import get_type_hints


class _EnvParseMixin:
    """Mixin to parse environment variables into class fields."""

    def __init__(self):
        for field, fieldType in get_type_hints(self).items():
            # Skip item if not upper case
            if not field.isupper():
                continue

            # Raise Error if required field not supplied
            default_value = getattr(self, field, None)
            if default_value is None and os.environ.get(field) is None:
                raise EnvironmentError(f'The {field} field is required, and is not set')
            # Cast env var value to expected type and raise AppConfigError on failure
            try:
                if fieldType == bool:
                    value = strtobool(os.environ.get(field, default_value))
                else:
                    value = fieldType(os.environ.get(field, default_value))

                self.__setattr__(field, value)
            except ValueError as e:
                raise EnvironmentError(
                    f'Unable to cast value of "{os.environ[field]}" to type '
                    f'"{fieldType}" for "{field}" field') from e


class CEDAConfig(_EnvParseMixin):
    """Config for CEDA FTP server."""

    CEDA_FTP_USER: str
    CEDA_FTP_PASS: str


class MetOfficeConfig(_EnvParseMixin):
    """Config for Met Office API."""

    METOFFICE_ORDER_ID: str
    METOFFICE_CLIENT_ID: str
    METOFFICE_CLIENT_SECRET: str


class LocalFSConfig(_EnvParseMixin):
    """Config for local filesystem."""

    RAW_DIR: str
    ZARR_DIR: str


