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


class ECMWFRealTimeS3RawRepository(ports.RawRepository):
    """Model repository implementation for ECMWF live data from S3."""

    bucket: str
    _fs: s3fs.S3FileSystem

    def __init__(self, bucket: str, fs: s3fs.S3FileSystem) -> None:
        """Create a new instance of the class."""
        self.bucket = bucket
        self._fs = fs

    @staticmethod
    @override
    def repository() -> entities.RawRepositoryMetadata:
        return entities.RawRepositoryMetadata(
            name="ECMWF-Realtime-S3",
            is_archive=False,
            is_order_based=True,
            delay_minutes=(60 * 7),  # 7 hours
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
            available_models={
                "default": entities.Models.ECMWF_HRES_IFS_0P1DEGREE.with_region("uk-north60"),
                "hres-ifs-uk": entities.Models.ECMWF_HRES_IFS_0P1DEGREE.with_region("uk-north60"),
                "hres-ifs-india": entities.Models.ECMWF_HRES_IFS_0P1DEGREE.with_region(
                    "india",
                ).with_chunk_count_overrides({"variable": 1}),
                "hres-ifs-nl": entities.Models.ECMWF_HRES_IFS_0P1DEGREE.with_region(
                    "nl",
                ).with_max_step(84),
            },
        )

    @staticmethod
    @override
    def model() -> entities.ModelMetadata:
        requested_model: str = os.getenv("MODEL", default="default")
        if requested_model not in ECMWFRealTimeS3RawRepository.repository().available_models:
            log.warning(
                f"Unknown model '{requested_model}' requested, falling back to default ",
                "ECMWF Realtime S3 repository only supports "
                f"'{list(ECMWFRealTimeS3RawRepository.repository().available_models.keys())}'. "
                "Ensure MODEL environment variable is set to a valid model name.",
            )
            requested_model = "default"
        return ECMWFRealTimeS3RawRepository.repository().available_models[requested_model]

    @override
    def fetch_init_data(
        self, it: dt.datetime,
    ) -> Iterator[Callable[..., ResultE[list[xr.DataArray]]]]:
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
            yield delayed(Failure)(
                ValueError(
                    f"Failed to list files in bucket path '{self.bucket}/ecmwf'. "
                    "Ensure the path exists and the caller has relevant access permissions. "
                    f"Encountered error: {e}",
                ),
            )
            return

        if len(urls) == 0:
            yield delayed(Failure)(
                ValueError(
                    f"No raw files found for init time '{it.strftime('%Y-%m-%d %H:%M')}' "
                    f"in bucket path '{self.bucket}/ecmwf'. Ensure files exist at the given path "
                    "named with the expected pattern, e.g. 'A2S10250000102603001.",
                ),
            )
            return

        log.debug(
            f"Found {len(urls)} file(s) for init time '{it.strftime('%Y-%m-%d %H:%M')}' "
            f"in bucket path '{self.bucket}/ecmwf'.",
        )
        for url in urls:
            yield delayed(self._download_and_convert)(url=url)

    @classmethod
    @override
    def authenticate(cls) -> ResultE["ECMWFRealTimeS3RawRepository"]:
        missing_envs = cls.repository().missing_required_envs()
        if len(missing_envs) > 0:
            return Failure(
                OSError(
                    f"Cannot authenticate with ECMWF Realtime S3 service due to "
                    f"missing required environment variables: {', '.join(missing_envs)}",
                ),
            )
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
            return Failure(
                ConnectionError(
                    "Failed to connect to S3 for ECMWF data. "
                    f"Credentials may be wrong or undefined. Encountered error: {e}",
                ),
            )

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
            (
                pathlib.Path(
                    os.getenv(
                        "RAWDIR",
                        f"~/.local/cache/nwp/{self.repository().name}/{self.model().name}/raw",
                    ),
                )
                / url.split("/")[-1]
            )
            .with_suffix(".grib")
            .expanduser()
        )

        # Only download the file if not already present
        if local_path.exists() and local_path.stat().st_size > 0:
            log.debug("Skipping download for existing file at '%s'.", local_path.as_posix())
        else:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            log.debug("Requesting file from S3 at: '%s'", url)

            try:
                if not self._fs.exists(url):
                    raise FileNotFoundError(f"File not found at '{url}'")

                log.debug("Writing file from '%s' to '%s'", url, local_path.as_posix())
                with local_path.open("wb") as lf, self._fs.open(url, "rb") as rf:
                    for chunk in iter(lambda: rf.read(12 * 1024), b""):
                        lf.write(chunk)
                        lf.flush()

            except Exception as e:
                return Failure(
                    OSError(
                        f"Failed to download file from S3 at '{url}'. Encountered error: {e}",
                    ),
                )

            if local_path.stat().st_size != self._fs.info(url)["size"]:
                return Failure(
                    ValueError(
                        f"Failed to download file from S3 at '{url}'. "
                        "File size mismatch. File may be corrupted.",
                    ),
                )

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
            return Failure(
                OSError(
                    f"Error opening '{path}' as list of xarray Datasets: {e}",
                ),
            )
        if len(dss) == 0:
            return Failure(
                ValueError(
                    f"No datasets found in '{path}'. File may be corrupted. "
                    "A redownload of the file may be required.",
                ),
            )

        processed_das: list[xr.DataArray] = []
        num_skipped: int = 0
        expected_lons = ECMWFRealTimeS3RawRepository.model().expected_coordinates.longitude
        expected_lats = ECMWFRealTimeS3RawRepository.model().expected_coordinates.latitude

        for i, ds in enumerate(dss):
            # ECMWF Realtime provides all regions in one set of datasets,
            # so distinguish via their coordinates
            is_relevant_dataset_predicate: bool = (
                (expected_lons is not None and expected_lats is not None)
                and (expected_lons[0] <= max(ds.coords["longitude"].values) <= expected_lons[-1])
                and (expected_lats[-1] <= max(ds.coords["latitude"].values) <= expected_lats[0])
            )
            if not is_relevant_dataset_predicate:
                num_skipped += 1
                continue
            try:
                da: xr.DataArray = (
                    entities.Parameter.rename_else_drop_ds_vars(
                        ds=ds,
                        allowed_parameters=ECMWFRealTimeS3RawRepository.model().expected_coordinates.variable,
                    )
                    .rename(name_dict={"time": "init_time"})
                    .expand_dims(dim="init_time")
                    .expand_dims(dim="step")
                    .to_dataarray(name=ECMWFRealTimeS3RawRepository.model().name)
                )
                da = (
                    da.drop_vars(
                        names=[
                            c
                            for c in ds.coords
                            if c
                            not in ECMWFRealTimeS3RawRepository.model().expected_coordinates.dims
                        ],
                        errors="ignore",
                    )
                    .transpose(*ECMWFRealTimeS3RawRepository.model().expected_coordinates.dims)
                    .sortby(variables=["step", "variable", "longitude"])
                    .sortby(variables="latitude", ascending=False)
                )

            except Exception as e:
                return Failure(
                    ValueError(
                        f"Error processing dataset {i} from '{path}' to DataArray: {e}",
                    ),
                )
            # Put each variable into its own DataArray:
            # * Each raw file does not contain a full set of parameters
            # * and so may not produce a contiguous subset of the expected coordinates.
            processed_das.extend(
                [da.where(cond=da["variable"] == v, drop=True) for v in da["variable"].values],
            )

        if len(processed_das) == 0:
            return Failure(
                ValueError(
                    f"Skipped {num_skipped}/{len(dss)} datasets from '{path}'. "
                    "File may not contain the expected parameters and geographic bounds.",
                ),
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
        prefix: str = os.getenv("ECMWF_REALTIME_DISSEMINATION_FILE_PREFIX", "A2")
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
        return tt <= it + dt.timedelta(hours=max_step)
