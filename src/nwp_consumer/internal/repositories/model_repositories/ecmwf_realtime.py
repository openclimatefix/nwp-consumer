"""Model repository implementation for ECMWF live data from S3.

Repository Information
======================

Documented Structure
--------------------

When getting live or realtime data from ECMWF, grib files are sent by
a data provider to a location of choice, in this case an S3 bucket.
The `ECMWF Dissemination Schedule <https://confluence.ecmwf.int/display/DAC/Dissemination+schedule>`_
describes the naming convention and time ordering for these files:

- A 2-character prefix
- A 1-character dissemination stream indicator
- 8 digits representing the initialization time in the format mmddHHMM
- 8 digits representing the target time in the format mmddHHMM
- 1 digit representing the file number(?)

So a file named ``A2D10250000D10260100`` would be for an initialization
time of 2024-10-25 00:00 and a target time of 2024-10-26 01:00 (step of 25 hours).

The file contents is specific to the order agreed with the data provider.
For the order that OCF has created, there are four distinct datasets.
This is because OCF has ordered two separate regions and 17 variables,
which are split across two datasets.

Also, some of the data contains larger steps than we are interested in due
to necessities in the order creation process.

"""

import datetime as dt
import logging
import os
import pathlib
import re
from collections.abc import Callable, Iterator
from typing import override

import cfgrib
import s3fs
import xarray as xr
from joblib import delayed
from returns.result import Failure, ResultE, Success

from nwp_consumer.internal import entities, ports

log = logging.getLogger("nwp-consumer")


class ECMWFRealTimeS3ModelRepository(ports.ModelRepository):
    """Model repository implementation for ECMWF live data from S3."""

    bucket: str
    _fs: s3fs.S3FileSystem

    def __init__(self, bucket: str, fs: s3fs.S3FileSystem) -> None:
        """Create a new instance of the class."""
        self.bucket = bucket
        self._fs = fs


    @staticmethod
    @override
    def repository() -> entities.ModelRepositoryMetadata:
        return entities.ModelRepositoryMetadata(
            name="ECMWF-Realtime-S3",
            is_archive=False,
            is_order_based=True,
            running_hours=[0, 6, 12, 18],
            delay_minutes=(60 * 6), # 6 hours
            max_connections=100,
            required_env=[
                "ECMWF_REALTIME_S3_ACCESS_KEY",
                "ECMWF_REALTIME_S3_ACCESS_SECRET",
                "ECMWF_REALTIME_S3_BUCKET",
                "ECMWF_REALTIME_S3_REGION",
            ],
            optional_env={
                "ECMWF_REALTIME_DISSEMINATION_FILE_PREFIX": "A2",
                "ECMWF_REALTIME_S3_BUCKET_PREFIX": "ecmwf",
            },
            postprocess_options=entities.PostProcessOptions(),
        )

    @staticmethod
    @override
    def model() -> entities.ModelMetadata:
        return entities.ModelMetadata(
            name="HRES-IFS",
            resolution="0.1 degrees",
            expected_coordinates=entities.NWPDimensionCoordinateMap(
                init_time=[],
                step=list(range(0, 85, 1)),
                variable=[
                    entities.Parameter.WIND_U_COMPONENT_10m,
                    entities.Parameter.WIND_V_COMPONENT_10m,
                    entities.Parameter.WIND_U_COMPONENT_100m,
                    entities.Parameter.WIND_V_COMPONENT_100m,
                    entities.Parameter.WIND_U_COMPONENT_200m,
                    entities.Parameter.WIND_V_COMPONENT_200m,
                    entities.Parameter.TEMPERATURE_SL,
                    entities.Parameter.TOTAL_PRECIPITATION_RATE_GL,
                    entities.Parameter.DOWNWARD_SHORTWAVE_RADIATION_FLUX_GL,
                    entities.Parameter.DOWNWARD_LONGWAVE_RADIATION_FLUX_GL,
                    entities.Parameter.CLOUD_COVER_HIGH,
                    entities.Parameter.CLOUD_COVER_MEDIUM,
                    entities.Parameter.CLOUD_COVER_LOW,
                    entities.Parameter.CLOUD_COVER_TOTAL,
                    entities.Parameter.SNOW_DEPTH_GL,
                    entities.Parameter.VISIBILITY_SL,
                    entities.Parameter.DIRECT_SHORTWAVE_RADIATION_FLUX_GL,
                    entities.Parameter.DOWNWARD_ULTRAVIOLET_RADIATION_FLUX_GL,
                ],
                latitude=[float(f"{lat / 10:.2f}") for lat in range(900, -900 - 1, -1)],
                longitude=[float(f"{lon / 10:.2f}") for lon in range(-1800, 1800 + 1, 1)],
            ),
        )

    @override
    def fetch_init_data(self, it: dt.datetime) \
            -> Iterator[Callable[..., ResultE[list[xr.DataArray]]]]:
        # List relevant files in the S3 bucket
        try:
            urls: list[str] = [
                f"s3://{f}"
                for f in self._fs.ls(f"{self.bucket}/ecmwf")
                if self._wanted_file(
                    filename=f.split("/")[-1],
                    it=it,
                    max_step=max(self.model().expected_coordinates.step),
                )
            ]
        except Exception as e:
            yield delayed(Failure)(ValueError(
                f"Failed to list files in bucket path '{self.bucket}/ecmwf'. "
                "Ensure the path exists and the caller has relevant access permissions. "
                f"Encountered error: {e}",
            ))
            return

        if len(urls) == 0:
            yield delayed(Failure)(ValueError(
                f"No raw files found for init time '{it.strftime('%Y-%m-%d %H:%M')}' "
                f"in bucket path '{self.bucket}/ecmwf'. Ensure files exist at the given path "
                "named with the expected pattern, e.g. 'A2S10250000102603001.",
            ))
            return

        log.debug(
            f"Found {len(urls)} file(s) for init time '{it.strftime('%Y-%m-%d %H:%M')}' "
            f"in bucket path '{self.bucket}/ecmwf'.",
        )
        for url in urls:
            yield delayed(self._download_and_convert)(url=url)

    @classmethod
    @override
    def authenticate(cls) -> ResultE["ECMWFRealTimeS3ModelRepository"]:
        missing_envs = cls.repository().missing_required_envs()
        if len(missing_envs) > 0:
            return Failure(OSError(
                f"Cannot authenticate with ECMWF Realtime S3 service due to "
                f"missing required environment variables: {', '.join(missing_envs)}",
            ))
        try:
            bucket: str = os.environ["ECMWF_REALTIME_S3_BUCKET"]
            _fs: s3fs.S3FileSystem = s3fs.S3FileSystem(
                key=os.environ["ECMWF_REALTIME_S3_ACCESS_KEY"],
                secret=os.environ["ECMWF_REALTIME_S3_ACCESS_SECRET"],
                client_kwargs={
                    "endpoint_url": os.environ.get("AWS_ENDPOINT_URL", None),
                    "region_name": os.environ["ECMWF_REALTIME_S3_REGION"],
                },
            )
        except Exception as e:
            return Failure(ConnectionError(
                "Failed to connect to S3 for ECMWF data. "
                f"Credentials may be wrong or undefined. Encountered error: {e}",
            ))

        log.debug(f"Successfully authenticated with S3 instance '{bucket}'")
        return Success(cls(bucket=bucket, fs=_fs))


    def _download_and_convert(self, url: str) -> ResultE[list[xr.DataArray]]:
        """Download and convert a file to xarray DataArrays.

        Args:
            url: The URL of the file to download.
        """
        return self._download(url=url).bind(self._convert)

    def _download(self, url: str) -> ResultE[pathlib.Path]:
        """Download an ECMWF realtime file from S3.

        Args:
            url: The URL to the S3 object.
        """
        local_path: pathlib.Path = (
            pathlib.Path(
                os.getenv(
                    "RAWDIR",
                    f"~/.local/cache/nwp/{self.repository().name}/{self.model().name}/raw",
                ),
            ) / url.split("/")[-1]
        ).with_suffix(".grib").expanduser()

        # Only download the file if not already present
        if not local_path.exists() or local_path.stat().st_size == 0:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            log.debug("Requesting file from S3 at: '%s'", url)

            try:
                if not self._fs.exists(url):
                    raise FileNotFoundError(f"File not found at '{url}'")

                with local_path.open("wb") as lf, self._fs.open(url, "rb") as rf:
                    for chunk in iter(lambda: rf.read(12 * 1024), b""):
                        lf.write(chunk)
                        lf.flush()

            except Exception as e:
                return Failure(OSError(
                    f"Failed to download file from S3 at '{url}'. Encountered error: {e}",
                ))

            if local_path.stat().st_size != self._fs.info(url)["size"]:
                return Failure(ValueError(
                    f"Failed to download file from S3 at '{url}'. "
                    "File size mismatch. File may be corrupted.",
                ))

        return Success(local_path)

    @staticmethod
    def _convert(path: pathlib.Path) -> ResultE[list[xr.DataArray]]:
        """Convert a grib file to an xarray DataArray.

        Args:
            path: The path to the grib file.
        """
        try:
            dss: list[xr.Dataset] = cfgrib.open_datasets(path.as_posix())
        except Exception as e:
            return Failure(OSError(
                f"Error opening '{path}' as list of xarray Datasets: {e}",
            ))
        if len(dss) == 0:
            return Failure(ValueError(
                f"No datasets found in '{path}'. File may be corrupted. "
                "A redownload of the file may be required.",
            ))

        processed_das: list[xr.DataArray] = []
        for i, ds in enumerate(dss):
            try:
                da: xr.DataArray = (
                    entities.Parameter.rename_else_drop_ds_vars(
                        ds=ds,
                        allowed_parameters=ECMWFRealTimeS3ModelRepository.model().expected_coordinates.variable,
                    )
                    .rename(name_dict={"time": "init_time"})
                    .expand_dims(dim="init_time")
                    .expand_dims(dim="step")
                    .to_dataarray(name=ECMWFRealTimeS3ModelRepository.model().name)
                )
                da = (
                    da.drop_vars(
                        names=[
                            c for c in ds.coords
                            if c not in ["init_time", "step", "variable", "latitude", "longitude"]
                        ],
                        errors="ignore",
                    )
                    .transpose("init_time", "step", "variable", "latitude", "longitude")
                    .sortby(variables=["step", "variable", "longitude"])
                    .sortby(variables="latitude", ascending=False)
                )
            except Exception as e:
                return Failure(ValueError(
                    f"Error processing dataset {i} from '{path}' to DataArray: {e}",
                ))
            # Put each variable into its own DataArray:
            # * Each raw file does not contain a full set of parameters
            # * and so may not produce a contiguous subset of the expected coordinates.
            processed_das.extend(
                [
                    da.where(cond=da["variable"] == v, drop=True)
                    for v in da["variable"].values
                ],
            )

        return Success(processed_das)

    @staticmethod
    def _wanted_file(filename: str, it: dt.datetime, max_step: int) -> bool:
        """Determine if the file is wanted based on the init time.

        See module docstring for the file naming convention.
        Returns True if the filename describes data corresponding to the input
        initialization time and model metadata.

        Args:
            filename: The name of the file.
            it: The init time of the model run.
            max_step: The maximum step in hours to consider.
        """
        prefix: str = os.getenv("ECMWF_DISSEMINATION_REALTIME_FILE_PREFIX", "A2")
        pattern: str = r"^" + prefix + r"[DS](\d{8})(\d{8})\d$"
        match: re.Match[str] | None = re.search(pattern=pattern, string=filename)
        if match is None:
            return False
        if it.strftime("%m%d%H%M") != match.group(1):
            return False
        tt: dt.datetime = dt.datetime.strptime(
            str(it.year) + match.group(2) + "+0000",
            "%Y%m%d%H%M%z",
        )
        return tt < it + dt.timedelta(hours=max_step)
