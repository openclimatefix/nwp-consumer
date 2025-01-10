"""Repository implementation for NOAA GFS data stored in S3.

This module contains the implementation of the model repository for the
NOAA GFS data stored in an S3 bucket.

Repository Information
======================

TODO: provide links etc

Documented Structure
--------------------

TODO: document filestructure
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


class NOAAS3RawRepository(ports.RawRepository):
    """Model repository implementation for GFS data stored in S3."""

    @staticmethod
    @override
    def repository() -> entities.RawRepositoryMetadata:
        return entities.RawRepositoryMetadata(
            name="NOAA-GFS-S3",
            is_archive=False,
            is_order_based=False,
            running_hours=[0, 6, 12, 18],
            delay_minutes=(60 * 5),  # 5 hours
            max_connections=100,
            required_env=[],
            optional_env={},
            postprocess_options=entities.PostProcessOptions(),
            available_models={
                "default": entities.Models.NCEP_GFS_1DEGREE,
                "gfs-1deg": entities.Models.NCEP_GFS_1DEGREE,
            },
        )

    @staticmethod
    @override
    def model() -> entities.ModelMetadata:
        return NOAAS3RawRepository.repository().available_models["default"]

    @override
    def fetch_init_data(
        self, it: dt.datetime,
    ) -> Iterator[Callable[..., ResultE[list[xr.DataArray]]]]:
        # List relevant files in the s3 bucket
        bucket_path: str = f"noaa-gfs-bdp-pds/gfs.{it:%Y%m%d}/{it:%H}/atmos"
        try:
            fs = s3fs.S3FileSystem(anon=True)
            urls: list[str] = [
                f"s3://{f}"
                for f in fs.ls(bucket_path)
                if self._wanted_file(
                    filename=f.split("/")[-1],
                    it=it,
                    max_step=max(self.model().expected_coordinates.step),
                )
            ]
        except Exception as e:
            yield delayed(Failure)(
                ValueError(
                    f"Failed to list file in bucket path '{bucket_path}'. "
                    "Ensure the path exists and the bucket does not require auth. "
                    f"Encountered error: '{e}'",
                ),
            )
            return

        if len(urls) == 0:
            yield delayed(Failure)(
                ValueError(
                    f"No files found for init time '{it:%Y-%m-%d %H:%M}'. "
                    "in bucket path '{bucket_path}'. Ensure files exists at the given path "
                    "with the expected filename pattern. ",
                ),
            )

        for url in urls:
            yield delayed(self._download_and_convert)(url=url, it=it)

    @classmethod
    @override
    def authenticate(cls) -> ResultE["NOAAS3RawRepository"]:
        return Success(cls())

    def _download_and_convert(self, url: str, it: dt.datetime) -> ResultE[list[xr.DataArray]]:
        """Download and convert a file from S3.

        Args:
            url: The URL to the S3 object.
            it: The init time of the object in question, used in the saved path
        """
        return self._download(url=url, it=it).bind(self._convert)

    def _download(self, url: str, it: dt.datetime) -> ResultE[pathlib.Path]:
        """Download a grib file from NOAA S3.

        The URLs have the following format::

          https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20230911/06/atmos/gfs.t06z.pgrb2.1p00.f087
          <------------------bucket---------------><---inittime--->      <-------filename----step>

        Args:
            url: The URL to the S3 object.
            it: The init time of the object in question, used in the saved path
        """
        local_path: pathlib.Path = (
            pathlib.Path(
                os.getenv(
                    "RAWDIR",
                    f"~/.local/cache/nwp/{self.repository().name}/{self.model().name}/raw",
                ),
            ) / it.strftime("%Y/%m/%d/%H") / (url.split("/")[-1] + ".grib")
        ).expanduser()

        # Only download the file if not already present
        if local_path.exists():
            return Success(local_path)

        local_path.parent.mkdir(parents=True, exist_ok=True)
        log.debug("Requesting file from S3 at: '%s'", url)

        fs = s3fs.S3FileSystem(anon=True)
        try:
            if not fs.exists(url):
                raise FileNotFoundError(f"File not found at '{url}'")

            with local_path.open("wb") as lf, fs.open(url, "rb") as rf:
                for chunk in iter(lambda: rf.read(12 * 1024), b""):
                    lf.write(chunk)
                    lf.flush()

        except Exception as e:
            return Failure(OSError(
                f"Failed to download file from S3 at '{url}'. Encountered error: {e}",
            ))

        # For some reason, the GFS files are about 2MB larger when downloaded
        # then their losted size in AWS. I'd be interested to know why!
        if local_path.stat().st_size < fs.info(url)["size"]:
            return Failure(ValueError(
                f"File size mismatch from file at '{url}': "
                f"{local_path.stat().st_size} != {fs.info(url)['size']} (remote). "
                "File may be corrupted.",
            ))

        # Also download the associated index file
        # * This isn't critical, but speeds up reading the file in when converting
        # TODO: Re-incorporate this when https://github.com/ecmwf/cfgrib/issues/350
        # TODO: is resolved. Currently downloaded index files are ignored due to
        # TODO: path differences once downloaded.
        # index_url: str = url + ".idx"
        # index_path: pathlib.Path = local_path.with_suffix(".grib.idx")
        # try:
        #     with index_path.open("wb") as lf, fs.open(index_url, "rb") as rf:
        #         for chunk in iter(lambda: rf.read(12 * 1024), b""):
        #             lf.write(chunk)
        #             lf.flush()
        # except Exception as e:
        #     log.warning(
        #         f"Failed to download index file from S3 at '{url}'. "
        #         "This will require a manual indexing when converting the file. "
        #         f"Encountered error: {e}",
        #     )

        return Success(local_path)

    @staticmethod
    def _convert(path: pathlib.Path) -> ResultE[list[xr.DataArray]]:
        """Convert a GFS file to an xarray DataArray collection.

        Args:
            path: The path to the local grib file.
        """
        try:
            # Use some options when opening the datasets:
            # * 'squeeze' reduces length-1- dimensions to scalar coordinates,
            #   thus single-level variables should not have any extra dimensions
            # * 'ignore_keeys' reduces the number of variables loaded to only those
            #   with level types of interest
            dss: list[xr.Dataset] = cfgrib.open_datasets(
                path.as_posix(),
                backend_kwargs={
                    "squeeze": True,
                    "ignore_keys": {
                        "levelType": ["isobaricInhPa", "depthBelowLandLayer", "meanSea"],
                    },
                    "errors": "raise",
                    "indexpath": "",  # TODO: Change when above TODO is resolved
                },
            )
        except Exception as e:
            return Failure(ValueError(
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
                ds = entities.Parameter.rename_else_drop_ds_vars(
                    ds=ds,
                    allowed_parameters=NOAAS3RawRepository.model().expected_coordinates.variable,
                )
                # Ignore datasets with no variables of interest
                if len(ds.data_vars) == 0:
                    continue
                # Ignore datasets with multi-level variables
                # * This would not work without the "squeeze" option in the open_datasets call,
                #   which reduces single-length dimensions to scalar coordinates
                if any(x not in ["latitude", "longitude" ,"time"] for x in ds.dims):
                    continue
                da: xr.DataArray = (
                    ds
                    .drop_vars(names=[
                        c for c in ds.coords if c not in ["time", "step", "latitude", "longitude"]
                    ])
                    .rename(name_dict={"time": "init_time"})
                    .expand_dims(dim="init_time")
                    .expand_dims(dim="step")
                    .to_dataarray(name=NOAAS3RawRepository.model().name)
                )
                da = (
                    da.drop_vars(
                        names=[
                            c for c in da.coords
                            if c not in NOAAS3RawRepository.model().expected_coordinates.dims
                        ],
                    )
                    .transpose(*NOAAS3RawRepository.model().expected_coordinates.dims)
                    .assign_coords(coords={"longitude": (da.coords["longitude"] + 180) % 360 - 180})
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

        if len(processed_das) == 0:
            return Failure(ValueError(
                f"The file at '{path}' does not contain any variables of interest. "
                "Ensure the conversion pipeline is not accidentally dropping wanted variables, ",
                "and that the file contains variables of interest.",
            ))

        return Success(processed_das)

    @staticmethod
    def _wanted_file(filename: str, it: dt.datetime, max_step: int) -> bool:
        """Determine if a file is wanted based on the init time and max step.

        See module docstring for file naming convention.
        """
        pattern: str = r"^gfs\.t(\d{2})z\.pgrb2\.1p00\.f(\d{3})$"
        match: re.Match[str] | None = re.search(pattern=pattern, string=filename)
        if match is None:
            return False
        if int(match.group(1)) != it.hour:
            return False
        return not int(match.group(2)) > max_step


