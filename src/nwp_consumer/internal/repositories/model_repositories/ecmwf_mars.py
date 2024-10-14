"""Model repository implementation for ECMWF live data from S3.

When getting live or realtime data from ECMWF, grib files are sent by
your data provider to a location of choice, in this case an S3 bucket.
"""

import datetime as dt
import logging
from collections.abc import Callable, Iterator
from typing import override
from joblib import delayed

import xarray as xr
from returns.result import ResultE

from nwp_consumer.internal import entities, ports

log = logging.getLogger("nwp-consumer")


class ECMWFRealTimeS3ModelRepository(ports.ModelRepository):
    """Model repository implementation for ECMWF live data from S3."""

    @override
    @property
    def metadata(self) -> entities.ModelRepositoryMetadata:
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
                latitude=[float(f"{lat:.4f}") for lat in range(90, -90 - 0.1, -0.1)],
                longitude=[float(f"{lon:.4f}") for lon in range(-180, 180 + 0.1, 0.1)],
            ),
            postprocess_options=entities.PostProcessOptions(),
        )

    @override
    def fetch_init_data(self, it: dt.datetime) -> Iterator[Callable[..., ResultE[xr.DataArray]]]:
        bucket: str = os.environ["ECMWF_REALTIME_S3_BUCKET"]
        try:
            fs: s3fs.S3FileSystem = s3fs.S3FileSystem(
                key=os.environ["ECMWF_REALTIME_S3_ACCESS_KEY"],
                secret=os.environ["ECMWF_REALTIME_S3_ACCESS_SECRET"],
                client_kwargs={
                    "endpoint_url": os.environ.get("AWS_ENDPOINT_URL", None),
                    "region_name": os.environ["ECMWF_REALTIME_S3_REGION"],
            )
        except Exception as e:
            yield delayed(Result.from_failure)(ConnectionError(
                "Failed to connect to S3 for ECMWF data. "
                f"Credentials may be wrong or undefined. Encountered error: {e}",
            ))
        # List relevant files in the S3 bucket
        try:
            urls: list[str] = [
                f"s3://{bucket}/ecmwf/{f}"
                for f in fs.ls((bucket / "ecmwf").as_posix())
                if it.strftime("A1D%m%d%H%M") in f
            ]
        except Exception as e:
            yield delayed(Result.from_failure)(ValueError(
                f"Failed to list files in bucket path '{bucket}/ecmwf'. "
                "Ensure the path exists and is accessible. Encountered error: {e}",
            ))
        if len(urls) == 0:
            yield delayed(Result.from_failure)(ValueError(
                f"No raw files found for init time '{it.strftime('%Y-%m-%d %H:%M')}' "
                f"in bucket path '{bucket}/ecmwf'. Ensure files exist at the given path "
                "named with the 'A1DMMDDHHMM...' prefix.",
            ))
        for file in files:
            yield delayed(self._download_and_convert)(url=url)


    def _download_and_convert(self, url: str) -> ResultE[xr.DataArray]:
        # TODO
        pass

    def _download(self, url: str) -> ResultE[pathlib.Path]:
        """Download an ECMWF realtime file from S3.

        Args:
            url: The URL to the S3 object.
        """
        # TODO
        pass


