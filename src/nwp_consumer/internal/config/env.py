"""Config struct for application running."""
import os
from distutils.util import strtobool
from typing import get_type_hints

import structlog

from nwp_consumer import internal
from nwp_consumer.internal import inputs, outputs

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

    @classmethod
    def print_env(cls) -> None:
        """Print the required environment variables."""
        message: str = f"Environment variables for {cls.__class__.__name__}:\n"
        for field, _ in get_type_hints(cls).items():
            if not field.isupper():
                continue
            default_value = getattr(cls, field, None)
            message += f"\t{field}{'(default: ' + default_value + ')' if default_value else ''}\n"
        log.info(message)

    def configure_fetcher(self) -> internal.FetcherInterface:
        """Configure the associated fetcher."""
        raise NotImplementedError(
            "Fetcher not implemented for this environment. Check the available inputs.",
        )

    def configure_storer(self) -> internal.StorageInterface:
        """Configure the associated storer."""
        raise NotImplementedError(
            "Storer not implemented for this environment. Check the available outputs.",
        )


# --- Configuration environment variables --- #


class ConsumerEnv(EnvParser):
    """Config for Consumer."""

    DASK_SCHEDULER_ADDRESS: str = ""


# --- Inputs environment variables --- #


class CEDAEnv(EnvParser):
    """Config for CEDA FTP server."""

    CEDA_FTP_USER: str
    CEDA_FTP_PASS: str

    def configure_fetcher(self) -> internal.FetcherInterface:
        """Overrides the corresponding method in the parent class."""
        return inputs.ceda.Client(ftpUsername=self.CEDA_FTP_USER, ftpPassword=self.CEDA_FTP_PASS)


class MetOfficeEnv(EnvParser):
    """Config for Met Office API."""

    METOFFICE_ORDER_ID: str
    METOFFICE_API_KEY: str

    def configure_fetcher(self) -> internal.FetcherInterface:
        """Overrides the corresponding method in the parent class."""
        return inputs.metoffice.Client(
            apiKey=self.METOFFICE_API_KEY,
            orderID=self.METOFFICE_ORDER_ID,
        )


class ECMWFMARSEnv(EnvParser):
    """Config for ECMWF MARS API."""

    ECMWF_API_KEY: str
    ECMWF_API_URL: str
    ECMWF_API_EMAIL: str
    ECMWF_AREA: str = "uk"
    ECMWF_HOURS: int = 48
    ECMWF_PARAMETER_GROUP: str = "default"

    def configure_fetcher(self) -> internal.FetcherInterface:
        """Overrides the corresponding method in the parent class."""
        return inputs.ecmwf.MARSClient(
            area=self.ECMWF_AREA,
            hours=self.ECMWF_HOURS,
            param_group=self.ECMWF_PARAMETER_GROUP,
        )


class ECMWFS3Env(EnvParser):
    """Config for ECMWF S3."""

    ECMWF_AWS_S3_BUCKET: str
    ECMWF_AWS_ACCESS_KEY: str = ""
    ECMWF_AWS_ACCESS_SECRET: str = ""
    ECMWF_AWS_REGION: str
    ECMWF_AREA: str = "uk"

    def configure_fetcher(self) -> internal.FetcherInterface:
        """Overrides the corresponding method in the parent class."""
        return inputs.ecmwf.S3Client(
            bucket=self.ECMWF_AWS_S3_BUCKET,
            area=self.ECMWF_AREA,
            region=self.ECMWF_AWS_REGION,
            key=self.ECMWF_AWS_ACCESS_KEY,
            secret=self.ECMWF_AWS_ACCESS_SECRET,
        )


class ICONEnv(EnvParser):
    """Config for ICON API."""

    ICON_MODEL: str = "europe"
    ICON_HOURS: int = 48
    ICON_PARAMETER_GROUP: str = "default"

    def configure_fetcher(self) -> internal.FetcherInterface:
        """Overrides the corresponding method in the parent class."""
        return inputs.icon.Client(
            model=self.ICON_MODEL,
            hours=self.ICON_HOURS,
            param_group=self.ICON_PARAMETER_GROUP,
        )


class CMCEnv(EnvParser):
    """Config for CMC API."""

    CMC_MODEL: str = "gdps"
    CMC_HOURS: int = 240
    CMC_PARAMETER_GROUP: str = "full"

    def configure_fetcher(self) -> internal.FetcherInterface:
        """Overrides the corresponding method in the parent class."""
        return inputs.cmc.Client(
            model=self.CMC_MODEL,
            hours=self.CMC_HOURS,
            param_group=self.CMC_PARAMETER_GROUP,
        )

class GFSEnv(EnvParser):
    """Config for GFS API."""

    GFS_MODEL: str = "global"
    GFS_HOURS: int = 48
    GFS_PARAMETER_GROUP: str = "default"

    def configure_fetcher(self) -> internal.FetcherInterface:
        """Overrides the corresponding method in the parent class."""
        return inputs.noaa.AWSClient(
            model=self.GFS_MODEL,
            param_group=self.GFS_PARAMETER_GROUP,
            hours=self.GFS_HOURS,
        )


# --- Outputs environment variables --- #


class LocalEnv(EnvParser):
    """Config for local storage."""

    # Required for EnvParser to believe it's a valid class
    dummy_field: str = ""

    def configure_storer(self) -> internal.StorageInterface:
        """Overrides the corresponding method in the parent class."""
        return outputs.localfs.Client()


class S3Env(EnvParser):
    """Config for S3."""

    AWS_S3_BUCKET: str
    AWS_ACCESS_KEY: str = ""
    AWS_ACCESS_SECRET: str = ""
    AWS_REGION: str

    def configure_storer(self) -> internal.StorageInterface:
        """Overrides the corresponding method in the parent class."""
        return outputs.s3.Client(
            bucket=self.AWS_S3_BUCKET,
            region=self.AWS_REGION,
            key=self.AWS_ACCESS_KEY,
            secret=self.AWS_ACCESS_SECRET,
        )


class HuggingFaceEnv(EnvParser):
    """Config for HuggingFace API."""

    HUGGINGFACE_TOKEN: str
    HUGGINGFACE_REPO_ID: str

    def configure_storer(self) -> internal.StorageInterface:
        """Overrides the corresponding method in the parent class."""
        return outputs.huggingface.Client(
            token=self.HUGGINGFACE_TOKEN,
            repoID=self.HUGGINGFACE_REPO_ID,
        )
