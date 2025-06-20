"""Repository implementation for NOAA GFS data stored in S3.

This module contains the implementation of the model repository for the
NOAA GFS data stored in an S3 bucket.

Repository Information
======================

TODO: provide links etc

Documented Structure
--------------------

The data is provided with a file per step. There is a difference between what is
provided in the 0-step file and the remaning 3-hourly files. The 0-step file
does not contain e.g. downward shortwave radiation flux and downward longwave
radiation flux. As such, these will be NaNs in the 0-step file. This is because
the 0-step file is contains an analysis of the forecast, while the remaining
files are the forecast itself. I think this is just a quirk of NOAA's distribution.

See Also:
    - https://www.nco.ncep.noaa.gov/pmb/products/gfs/
    - https://www.nco.ncep.noaa.gov/pmb/products/gfs/gfs.t00z.pgrb2.1p00.f000.shtml
    - https://www.nco.ncep.noaa.gov/pmb/products/gfs/gfs.t00z.pgrb2.1p00.f003.shtml

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
        self,
        it: dt.datetime,
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
                    steps=self.model().expected_coordinates.step,
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
            )
            / it.strftime("%Y/%m/%d/%H")
            / (url.split("/")[-1] + ".grib")
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
            return Failure(
                OSError(
                    f"Failed to download file from S3 at '{url}'. Encountered error: {e}",
                ),
            )

        # For some reason, the GFS files are about 2MB larger when downloaded
        # then their losted size in AWS. I'd be interested to know why!
        if local_path.stat().st_size < fs.info(url)["size"]:
            return Failure(
                ValueError(
                    f"File size mismatch from file at '{url}': "
                    f"{local_path.stat().st_size} != {fs.info(url)['size']} (remote). "
                    "File may be corrupted.",
                ),
            )

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
            # * 'filter_by_keys' reduces the number of variables loaded to only those
            #   with names of interest. "t" is filtered out as it exists in multiple
            #   levels
            filters: list[dict[str, list[str] | list[int] | int]] = [
                {
                    "cfVarName": ["tcc", "hcc", "lcc", "mcc"],
                    "level": 0,
                    "typeOfLevel": [
                        "highCloudLayer",
                        "lowCloudLayer",
                        "middleCloudLayer",
                        "convectiveCloudLayer",
                    ],
                    "stepType": ["instant"],
                },
                {"cfVarName": ["u10", "v10"]},
                {"cfVarName": ["u100", "v100"]},
                {
                    "typeOfLevel": ["surface", "heightAboveGround"],
                    "level": [0, 2],
                    "stepType": ["instant"],
                },
                {
                    "typeOfLevel": ["surface"],
                    "stepType": ["avg"],
                    "cfVarName": ["sdswrf", "sdlwrf"],
                },
            ]
            ds: xr.Dataset = xr.merge(
                [
                    cfgrib.open_dataset(path.as_posix(), backend_kwargs={"filter_by_keys": f})
                    for f in filters
                ],
                compat="minimal",
            ).drop_vars("t", errors="ignore")
        except Exception as e:
            return Failure(
                ValueError(
                    f"Error opening '{path}' as list of xarray Datasets: {e}",
                ),
            )

        if len(ds.data_vars) == 0:
            return Failure(
                ValueError(
                    f"No datasets found in '{path}'. File may be corrupted. "
                    "A redownload of the file may be required.",
                ),
            )

        try:
            ds = ds.drop_vars("sdwe", errors="ignore")  # Datasets contain both SDWE and SD
            ds = entities.Parameter.rename_else_drop_ds_vars(
                ds=ds,
                allowed_parameters=NOAAS3RawRepository.model().expected_coordinates.variable,
            )
            da: xr.DataArray = (
                ds.drop_vars(
                    names=[
                        c for c in ds.coords if c not in ["time", "step", "latitude", "longitude"]
                    ],
                )
                .rename(name_dict={"time": "init_time"})
                .expand_dims(dim="init_time")
                .expand_dims(dim="step")
                .to_dataarray(name=NOAAS3RawRepository.model().name)
            )
            da = (
                da.drop_vars(
                    names=[
                        c
                        for c in da.coords
                        if c not in NOAAS3RawRepository.model().expected_coordinates.dims
                    ],
                )
                .transpose(*NOAAS3RawRepository.model().expected_coordinates.dims)
                .assign_coords(coords={"longitude": (da.coords["longitude"] + 180) % 360 - 180})
                .sortby(variables=["step", "variable", "longitude"])
                .sortby(variables="latitude", ascending=False)
            )
        except Exception as e:
            return Failure(
                ValueError(
                    f"Error processing dataset from '{path}' to DataArray: {e}",
                ),
            )

        return Success([da])

    @staticmethod
    def _wanted_file(filename: str, it: dt.datetime, steps: list[int]) -> bool:
        """Determine if a file is wanted based on the init time and max step.

        See module docstring for file naming convention.
        """
        pattern: str = r"^gfs\.t(\d{2})z\.pgrb2\.1p00\.f(\d{3})$"
        match: re.Match[str] | None = re.search(pattern=pattern, string=filename)
        if match is None:
            return False
        if int(match.group(1)) != it.hour:
            return False
        return int(match.group(2)) in steps
