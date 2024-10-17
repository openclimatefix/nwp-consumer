"""Model repository implementation for ECMWF live data from S3.

When getting live or realtime data from ECMWF, grib files are sent by
your data provider to a location of choice, in this case an S3 bucket.
"""

import datetime as dt
import logging
import os
import pathlib
from collections.abc import Callable, Iterator, Collection
from typing import override

import cfgrib
import s3fs
import xarray as xr
from joblib import delayed
from returns.result import Failure, Result, ResultE, Success

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
    def metadata() -> entities.ModelRepositoryMetadata:
        return entities.ModelRepositoryMetadata(
            name="ecmwf_realtime_operational_uk_11km",
            is_archive=False,
            is_order_based=True,
            running_hours=[0, 12],
            delay_minutes=(60 * 6), # 6 hours
            max_connections=100,
            required_env=[
                "ECMWF_REALTIME_S3_ACCESS_KEY",
                "ECMWF_REALTIME_S3_ACCESS_SECRET",
                "ECMWF_REALTIME_S3_BUCKET",
                "ECMWF_REALTIME_S3_REGION",
            ],
            optional_env={},
            expected_coordinates=entities.NWPDimensionCoordinateMap(
                init_time=[],
                step=list(range(0, 84, 1)),
                variable=[
                    entities.params.wind_u_component_10m,
                    entities.params.wind_u_component_100m,
                    entities.params.wind_v_component_10m,
                    entities.params.wind_v_component_100m,
                    entities.params.wind_u_component_200m,
                    entities.params.wind_v_component_200m,
                    entities.params.temperature_sl,
                    entities.params.downward_shortwave_radiation_flux_gl,
                    entities.params.downward_longwave_radiation_flux_gl,
                    entities.params.cloud_cover_high,
                    entities.params.cloud_cover_medium,
                    entities.params.cloud_cover_low,
                    entities.params.cloud_cover_total,
                    entities.params.snow_depth_gl,
                    entities.params.visibility_sl,
                ],
                latitude=[float(f"{lat/10:.2f}") for lat in range(900, -900 - 1, -1)],
                longitude=[float(f"{lon/10:.2f}") for lon in range(-1800, 1800 + 1, 1)],
            ),
            postprocess_options=entities.PostProcessOptions(),
        )

    @override
    def fetch_init_data(self, it: dt.datetime) \
            -> Iterator[Callable[..., ResultE[list[xr.DataArray]]]]:
        authenticate_result = self.authenticate()
        if isinstance(authenticate_result, Failure):
            yield delayed(Result.from_failure)(authenticate_result.failure())
            return

        # List relevant files in the S3 bucket
        try:
            urls: list[str] = [
                f"s3://{self.bucket}/ecmwf/{f}"
                for f in self._fs.ls(f"{self.bucket}/ecmwf")
                if it.strftime("%m%d%H%M") in f
            ]
        except Exception as e:
            yield delayed(Result.from_failure)(ValueError(
                f"Failed to list files in bucket path '{self.bucket}/ecmwf'. "
                "Ensure the path exists and is accessible. Encountered error: {e}",
            ))
            return

        if len(urls) == 0:
            yield delayed(Result.from_failure)(ValueError(
                f"No raw files found for init time '{it.strftime('%Y-%m-%d %H:%M')}' "
                f"in bucket path '{self.bucket}/ecmwf'. Ensure files exist at the given path "
                "named with the 'A1...MMDDHHMM...' pattern.",
            ))

        for url in urls:
            yield delayed(self._download_and_convert)(url=url)

    @classmethod
    def authenticate(cls) -> ResultE["ECMWFRealTimeS3ModelRepository"]:
        """Authenticate with the S3 bucket.

        Returns:
            The authenticated S3 filesystem object.
        """
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

        return Success(cls(bucket=bucket, fs=_fs))


    def _download_and_convert(self, url: str) -> ResultE[Collection[xr.DataArray]]:
        # TODO
        pass

    def _download(self, url: str) -> ResultE[pathlib.Path]:
        """Download an ECMWF realtime file from S3.

        Args:
            url: The URL to the S3 object.
        """
        if self.bucket is None or self._fs is None:
            return Result.from_failure(
                ConnectionError(
                    "Attempted to download file from S3 while not authenticated. "
                    "Ensure the 'authenticate' method has been called prior to download.",
                ),
            )

        local_path: pathlib.Path = (
            pathlib.Path(
                os.getenv("RAWDIR", f"~/.local/cache/nwp/{self.metadata().name}/raw"),
            ) / url.split("/")[-1]
        )

        # Only download the file if not already present
        if not local_path.exists():
            log.debug("Requesting file from S3 at: '%s'", url)
            try:
                with local_path.open("wb") as lf, self._fs.open(url, "rb") as rf:
                    for chunk in iter(lambda: rf.read(12 * 1024), b""):
                        lf.write(chunk)
                        lf.flush()

                    if local_path.stat().st_size != self._fs.info(url)["size"]:
                        raise ValueError(
                            f"Failed to download file from S3 at '{url}'. "
                            "File size mismatch. File may be corrupted.",
                        )

            except Exception as e:
                return Result.from_failure(
                    OSError(
                        f"Failed to download file from S3 at '{url}'. Encountered error: {e}",
                    ),
                )

        return Result.from_value(local_path)

    @staticmethod
    def _convert(path: pathlib.Path) -> ResultE[list[xr.DataArray]]:
        """Convert a grib file to an xarray DataArray.

        Args:
            path: The path to the grib file.
        """
        try:
            dss: list[xr.Dataset] = cfgrib.open_datasets(path.as_posix())
        except Exception as e:
            return Result.from_failure(OSError(f"Error opening '{path}' as xarray Dataset: {e}"))
        # TODO: Rename the variables to match the expected names
        # TODO 2024-10-18: Change all calls to `metadata` to use the new staticmethod
        pass




