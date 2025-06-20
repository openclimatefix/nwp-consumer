"""Model repository implementation for MetOffice Global data from CEDA.

Repository information:
=======================

The original model is from the UK Met Office, who don't provide their own archive.
CEDA (Centre for Environmental Data Analysis) host the data on their DAP server [2].
The CEDA catalogue for the Met Office Global can be found
`here <https://catalogue.ceda.ac.uk/uuid/86df725b793b4b4cb0ca0646686bd783>`_,
and the spec sheet from the Met Office is detailed in
`this PDF <https://www.metoffice.gov.uk/binaries/content/assets/metofficegovuk/pdf/data/global-atmospheric-model-17-km-resolution.pdf>`_.

For further details on the repository, see the
`CEDAFTPRawRepository.repository` implementation.

Data discrepancies and corrections
==================================

*MetOffice global* model data is stored on CEDA in segments:

- 4 areas for the northern hemisphere (A,B,C,D)
- 4 areas for the southern hemisphere (E,F,G,H)

Each area contains a subset of the data for a given time step.

Documented structure
--------------------

According to the MetOffice documentation [2], the files have the following structure::

    Northern hemisphere:
    - AreaA: Lat: 89.9 -> 0.3, Lon: -45 -> 45
    - AreaB: Lat: 89.9 -> 0.3, Lon: 45 -> 135
    - AreaC: Lat: 89.9 -> 0.3, Lon: 135 -> -135 (wraps around 180)
    - AreaD: Lat: 89.9 -> 0.3, Lon: -135 -> -45

    Southern hemisphere:
    - AreaE: Lat: -0.3 -> -89.9, Lon: -45 -> 45
    - AreaF: Lat: -0.3 -> -89.9, Lon: 45 -> 135
    - AreaG: Lat: -0.3 -> -89.9, Lon: 135 -> -135 (wraps around 180)
    - AreaH: Lat: -0.3 -> -89.9, Lon: -135 -> -45

With steps of 0.153 degrees in latitude and 0.234 degrees in longitude.

Actual structure
----------------

In my experience however, the data is not quite as described in the documentation.
Using the eccodes grib tool as shown::

    $ grib_ls -n geography -wcount=13 file.grib

I found that the grids are in fact as follows::

    - AreaA: Lat: 0 -> 89.856, Lon: 315 -> 45.09
    - AreaB: Lat: 0 -> 89.856, Lon: 45 -> 135.09
    - AreaC: Lat: 0 -> 89.856, Lon: 135 -> 225.09 (wraps around 180)
    - AreaD: Lat: 0 -> 89.856, Lon: 225 -> 315.09
    - AreaE: Lat: -89.856 -> 0, Lon: 315 -> 45.09
    - AreaF: Lat: -89.856 -> 0, Lon: 45 -> 135.09
    - AreaG: Lat: -89.856 -> 0, Lon: 135 -> 225.09 (wraps around 180)
    - AreaH: Lat: -89.856 -> 0, Lon: 225 -> 315.09

With steps of 0.156 degrees in latitude and 0.234 degrees in longitude.

.. important:: Key takeaways from this are:

    - The latitude values are in reverse order as described in the documentation
    - The longitude values overlap each other and combine to form a non-uniform step size
    - The step size is slightly different
    - Smaller lat/lon chunks are needed to allow for the partial area files to be written
      in parallel

As a result, the incoming data is modified to alleviate these issues.

"""

import base64
import datetime as dt
import itertools
import json
import logging
import os
import pathlib
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator
from typing import override

import cfgrib
import numpy as np
import xarray as xr
from joblib import delayed
from returns.result import Failure, ResultE, Success

from nwp_consumer.internal import entities, ports

log = logging.getLogger("nwp-consumer")


class CEDARawRepository(ports.RawRepository):
    """Repository implementation for the MetOffice global model data."""

    url_base: str = "https://dap.ceda.ac.uk/badc/ukmo-nwp/data"
    """The base URL for the CEDA DAP server."""
    _token: str
    """The authentication token for the CEDA DAP server."""

    def __init__(self, token: str) -> None:
        """Create a new instance."""
        self._token = token

    @staticmethod
    @override
    def repository() -> entities.RawRepositoryMetadata:
        return entities.RawRepositoryMetadata(
            name="CEDA",
            is_archive=True,
            is_order_based=False,
            delay_minutes=(60 * 24 * 7) + (60 * 12),  # 7.5 days
            max_connections=20,
            required_env=["CEDA_USER", "CEDA_PASS"],
            optional_env={},
            postprocess_options=entities.PostProcessOptions(),
            available_models={
                "default": entities.Models.MO_UM_GLOBAL_17KM.with_chunk_count_overrides(
                    {
                        "latitude": 8,
                        "longitude": 8,
                    },
                ).with_running_hours([0, 12]),  # 6 and 18 exist, but are lacking variables
                "mo-um-global": entities.Models.MO_UM_GLOBAL_17KM.with_chunk_count_overrides(
                    {
                        "latitude": 8,
                        "longitude": 8,
                    },
                ).with_running_hours([0, 12]),
                "mo-um-ukv": entities.Models.MO_UM_UKV_2KM_OSGB,
            },
        )

    @staticmethod
    @override
    def model() -> entities.ModelMetadata:
        requested_model: str = os.getenv("MODEL", default="default")
        if requested_model not in CEDARawRepository.repository().available_models:
            log.warn(
                f"Unknown model '{requested_model}' requested, falling back to default ",
                "CEDA repository only supports "
                f"'{list(CEDARawRepository.repository().available_models.keys())}'. "
                "Ensure MODEL environment variable is set to a valid model name.",
            )
            requested_model = "default"
        return CEDARawRepository.repository().available_models[requested_model]

    @override
    def fetch_init_data(
        self, it: dt.datetime,
    ) -> Iterator[Callable[..., ResultE[list[xr.DataArray]]]]:
        parameter_stubs: list[str] = [
            "Total_Downward_Surface_SW_Flux",
            "high_cloud_amount",
            "low_cloud_amount",
            "medium_cloud_amount",
            "relative_humidity_1_5m",
            "snow_depth",
            "temperature_1_5m",
            # "total_cloud",
            # "total_precipitation_rate", Exists, but only has 3 hourly steps
            "visibility_1_5m",
            "wind_u_10m",
            "wind_v_10m",
        ]

        file_stubs: list[str] = [
            "Wholesale1.grib",
            "Wholesale1T54.grib",
            "Wholesale2.grib",
            "Wholesale2T54.grib",
        ]

        if self.model().name == entities.Models.MO_UM_UKV_2KM_OSGB.name:
            for file in file_stubs:
                url = (
                    f"{self.url_base}/ukv-grib/{it:%Y/%m/%d}"
                    + f"/{it:%Y%m%d%H%M}_u1096_ng_umqv_{file}"
                )
                yield delayed(self._download_and_convert)(url=url)
        else:
            for param, area in itertools.product(parameter_stubs, [f"Area{c}" for c in "ABCDEFGH"]):
                url = (
                    f"{self.url_base}/global-grib/{it:%Y/%m/%d}"
                    + f"/{it:%Y%m%d%H}_WSGlobal17km_{param}_{area}_000144.grib"
                )
                yield delayed(self._download_and_convert)(url=url)

        pass

    def _download_and_convert(self, url: str) -> ResultE[list[xr.DataArray]]:
        """Download and convert a file to xarray DataArrays.

        Args:
            url: The URL of the file to download.
        """
        if "global-grib" in url:
            return self._download(url).bind(self._convert_global)
        elif "ukv-grib" in url:
            return self._download(url).bind(self._convert_ukv)
        else:
            return Failure(ValueError(f"Unknown URL type: {url}"))

    @classmethod
    @override
    def authenticate(cls) -> ResultE["CEDARawRepository"]:
        """Authenticate with CEDA.

        Returns:
            A Result containing the instantiated class if successful, or an error if not.
        """
        missing_envs = cls.repository().missing_required_envs()
        if len(missing_envs) > 0:
            return Failure(
                OSError(
                    f"Cannot authenticate with CEDA FTP service due to "
                    f"missing required environment variables: {', '.join(missing_envs)}",
                ),
            )
        username: str = os.environ["CEDA_USER"]
        password: str = os.environ["CEDA_PASS"]
        token: str = base64.b64encode(f"{username}:{password}".encode()).decode("ascii")
        request: urllib.request.Request = urllib.request.Request(
            method="POST",
            url="https://services-beta.ceda.ac.uk/api/token/create/",
            headers={"Authorization": f"Basic {token}"},
        )
        with urllib.request.urlopen(request, timeout=20) as response:  # noqa: S310
            if response.status != 200:
                return Failure(
                    OSError(
                        f"Failed to authenticate with CEDA: {response.status} {response.reason}",
                    ),
                )
            try:
                access_token: str = json.loads(response.read())["access_token"]
            except Exception as e:
                return Failure(
                    OSError(
                        f"Failed to parse CEDA access token: {e}",
                    ),
                )
        log.debug("Generated access token for CEDA")

        return Success(cls(token=access_token))

    def _download(self, url: str) -> ResultE[pathlib.Path]:
        """Download a file from CEDA.

        Args:
            url: The URL of the file to download.
        """
        local_path: pathlib.Path = (
            pathlib.Path(
                os.getenv(
                    "RAWDIR",
                    f"~/.local/cache/nwp/{self.repository().name}/{self.model().name}/raw",
                ),
            )
            / url.split("/")[-1]
        ).expanduser()

        # Don't download the file if it already exists
        if not local_path.exists():
            local_path.parent.mkdir(parents=True, exist_ok=True)
            log.debug("Sending request to CEDA DAP server for: '%s'", url)
            try:
                request: urllib.request.Request = urllib.request.Request(  # noqa: S310
                    method="GET",
                    url=url,
                    headers={"Authorization": f"Bearer {self._token}"},
                )
                response = urllib.request.urlopen(request, timeout=30)  # noqa: S310
            except Exception as e:
                return Failure(OSError(f"Error fetching {url}: {e}"))

            local_path.parent.mkdir(parents=True, exist_ok=True)
            log.debug("Downloading %s to %s", url, local_path)
            try:
                with local_path.open("wb") as f:
                    for chunk in iter(lambda: response.read(16 * 1024), b""):
                        f.write(chunk)
                        f.flush()
                log.debug(
                    f"Downloaded '{url}' to '{local_path}' (%s bytes)",
                    local_path.stat().st_size,
                )
            except Exception as e:
                return Failure(
                    OSError(
                        f"Error saving '{url}' to '{local_path}': {e}",
                    ),
                )

        return Success(local_path)

    @staticmethod
    def _convert_global(path: pathlib.Path) -> ResultE[list[xr.DataArray]]:
        """Convert a local grib file to xarray DataArrays.

        Args:
            path: The path to the file to convert.
        """
        try:
            dss: list[xr.Dataset] = cfgrib.open_datasets(path)
        except Exception as e:
            return Failure(
                OSError(
                    f"Error opening '{path}' as xarray Datasets: {e}",
                ),
            )
        try:
            processed_das: list[xr.DataArray] = []
            for ds in dss:
                ds = entities.Parameter.rename_else_drop_ds_vars(
                    ds=ds,
                    allowed_parameters=CEDARawRepository.model().expected_coordinates.variable,
                )
                # Ignore datasets with no variables of interest
                if len(ds.data_vars) == 0:
                    return Failure(
                        OSError(
                            f"No relevant variables found in '{path}'. "
                            "Ensure file contains the expected variables, "
                            "and that desired variables are not being dropped.",
                        ),
                    )
                da: xr.DataArray = (
                    ds.where(
                        ds.step
                        <= np.timedelta64(
                            CEDARawRepository.model().expected_coordinates.step[-1],
                            "h",
                        ),
                        drop=True,
                    )
                    .drop_vars(
                        names=[
                            c
                            for c in ds.coords
                            if c not in ["time", "step", "latitude", "longitude"]
                        ],
                    )
                    .rename(name_dict={"time": "init_time"})
                    .expand_dims(dim="init_time")
                    .to_dataarray(name=CEDARawRepository.model().name)
                )
                da = da.transpose(*CEDARawRepository.model().expected_coordinates.dims)
                if "longitude" in da.coords:
                    # We are dealing with the "AreaX" files:
                    # * remove the last value of the longitude dimension
                    #   as it overlaps with the next file
                    # * reverse the latitude dimension to be in descending order
                    da = da.isel(longitude=slice(None, -1), latitude=slice(None, None, -1))
                # Put each variable into its own DataArray:
                # * Each raw file does not contain a full set of parameters
                # * and so may not produce a contiguous subset of the expected coordinates.
                processed_das.extend(
                    [
                        da.where(cond=da.coords["variable"] == v, drop=True)
                        for v in da.coords["variable"].values
                    ],
                )
        except Exception as e:
            return Failure(
                ValueError(
                    f"Error processing {path} to DataArray: {e}",
                ),
            )
        return Success(processed_das)

    @staticmethod
    def _convert_ukv(path: pathlib.Path) -> ResultE[list[xr.DataArray]]:
        """Convert a local wholesale grib file to xarray DataArrays."""
        # Load the wholesale file as a list of datasets
        # * cfgrib loads multiple hypercubes for a single multi-parameter grib file
        # * Can also set backend_kwargs={"indexpath": ""}, to avoid the index file
        try:
            dss: list[xr.Dataset] = cfgrib.open_datasets(
                path=path.as_posix(),
                chunks={"time": 1, "step": -1, "variable": -1, "x": "auto", "y": "auto"},
                backend_kwargs={"indexpath": ""},
            )
        except Exception as e:
            return Failure(
                OSError(
                    f"Error opening '{path}' as xarray Datasets: {e}",
                ),
            )

        processed_das: list[xr.DataArray] = []
        try:
            for ds in dss:
                # Ensure the temperature is defined at 1 meter above ground level
                # * In the early NWPs (definitely in the 2016-03-22 NWPs):
                #   - `heightAboveGround` only has one entry ("1" meter above ground)
                #   - `heightAboveGround` isn't set as a dimension for `t`.
                # * In later NWPs, 'heightAboveGround' has 2 values (0, 1) and is a
                #   dimension for `t`.
                if "t" in ds and "heightAboveGround" in ds["t"].dims:
                    ds = ds.sel(heightAboveGround=1)

                # Delete unnecessary data variables
                ds = entities.Parameter.rename_else_drop_ds_vars(
                    ds=ds,
                    allowed_parameters=CEDARawRepository.model().expected_coordinates.variable,
                )
                if len(ds.data_vars) == 0:
                    continue

                ds = (
                    ds.rename({"time": "init_time"})
                    .expand_dims("init_time")
                    .drop_vars(
                        names=[
                            c
                            for c in ds.coords
                            if c not in CEDARawRepository.model().expected_coordinates.dims
                        ],
                        errors="ignore",
                    )
                    .where(
                        ds.step
                        <= np.timedelta64(
                            CEDARawRepository.model().expected_coordinates.step[-1],
                            "h",
                        ),
                        drop=True,
                    )
                )
                # Adapted from https://stackoverflow.com/a/62667154 and
                # https://github.com/SciTools/iris-grib/issues/140#issuecomment-1398634288
                northing: list[int] = CEDARawRepository.model().expected_coordinates.y_osgb  # type: ignore
                easting: list[int] = CEDARawRepository.model().expected_coordinates.x_osgb  # type: ignore
                if ds.sizes["values"] != len(northing) * len(easting):
                    raise ValueError(
                        f"dataset has {ds.sizes['values']} values, "
                        f"but expected {len(northing) * len(easting)}",
                    )
                ds = ds.assign_coords(
                    {
                        "x_osgb": ("values", np.tile(easting, reps=len(northing))),
                        "y_osgb": ("values", np.repeat(northing, repeats=len(easting))),
                    },
                )
                # Set `values` to be a MultiIndex, indexed by `y` and `x`, then unstack
                # * This gets rid of the `values` dimension and indexes
                #   the data variables using `y` and `x`.
                ds = ds.set_index(values=("y_osgb", "x_osgb")).unstack("values")
                da: xr.DataArray = (
                    ds.to_array(name=CEDARawRepository.model().name)
                    .sortby(variables=["step", "variable", "x_osgb"])
                    .sortby(variables="y_osgb", ascending=False)
                    .transpose("init_time", "step", "variable", "y_osgb", "x_osgb")
                    .drop_vars(
                        names=[
                            c
                            for c in ds.coords
                            if c not in CEDARawRepository.model().expected_coordinates.dims
                        ],
                        errors="ignore",
                    )
                )
                # Put each variable into its own DataArray:
                # * Each raw file does not contain a full set of parameters
                # * and so may not produce a contiguous subset of the expected coordinates.
                processed_das.extend(
                    [
                        da.where(cond=da.coords["variable"] == v, drop=True)
                        for v in da.coords["variable"].values
                    ],
                )
        except Exception as e:
            return Failure(
                ValueError(
                    f"Error processing {path} to DataArray: {e}",
                ),
            )

        return Success(processed_das)
