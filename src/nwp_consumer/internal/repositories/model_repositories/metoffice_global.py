"""Model repository implementation for MetOffice Global data from CEDA.

Repository information:
=======================

The original model is from the UK Met Office, who don't provide their own archive.
CEDA (Centre for Environmental Data Analysis) host the data on their FTP server [2].
The CEDA catalogue for the Met Office Global can be found
`here <https://catalogue.ceda.ac.uk/uuid/86df725b793b4b4cb0ca0646686bd783>`_,
and the spec sheet from the Met Office is detailed in
`this PDF <https://www.metoffice.gov.uk/binaries/content/assets/metofficegovuk/pdf/data/global-atmospheric-model-17-km-resolution.pdf>`_.

For further details on the repository, see the `CedaMetOfficeGlobalModelRepository.metadata` implementation.

Data discrepancies and corrections
==================================

MetOffice global model data is stored on CEDA in segments:

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

import datetime as dt
import logging
import os
import pathlib
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator
from typing import override

import numpy as np
import xarray as xr
from joblib import delayed
from returns.result import Failure, Result, ResultE

from nwp_consumer.internal import entities, ports

log = logging.getLogger("nwp-consumer")


class CedaMetOfficeGlobalModelRepository(ports.ModelRepository):
    """Repository implementation for the MetOffice global model data."""

    url_base: str = "ftp.ceda.ac.uk/badc/ukmo-nwp/data/global-grib"
    """The base URL for the CEDA FTP server."""
    _url_auth: str | None = None
    """The URL prefix containing authentication information."""

    def __init__(self) -> None:
        """Create a new instance."""

    @override
    @property
    def metadata(self) -> entities.ModelRepositoryMetadata:
        return entities.ModelRepositoryMetadata(
            name="ceda_metoffice_global_17km",
            is_archive=True,
            is_order_based=False,
            running_hours=[0, 12],  # 6 and 18 exist, but are lacking variables
            delay_minutes=(60 * 24 * 7) + (60 * 12),  # 7.5 days
            max_connections=20,
            required_env=["CEDA_FTP_USER", "CEDA_FTP_PASS"],
            optional_env={},
            expected_coordinates=entities.NWPDimensionCoordinateMap(
                init_time=[],
                step=list(range(0, 48, 1)),
                variable=[
                    entities.params.downward_shortwave_radiation_flux_gl,
                    entities.params.cloud_cover_total,
                    entities.params.cloud_cover_high,
                    entities.params.cloud_cover_low,
                    entities.params.cloud_cover_medium,
                    entities.params.relative_humidity_sl,
                    entities.params.snow_depth_gl,
                    entities.params.temperature_sl,
                    entities.params.wind_u_component_10m,
                    entities.params.wind_v_component_10m,
                    entities.params.visibility_sl,
                ],
                latitude=[
                    float(f"{lat:.4f}") for lat in np.arange(89.856, -89.856 - 0.156, -0.156)
                ],
                longitude=[
                    float(f"{lon:.4f}") for lon in np.concatenate([
                        np.arange(-45, 45, 0.234),
                        np.arange(45, 135, 0.234),
                        np.arange(135, 225, 0.234),
                        np.arange(225, 315, 0.234),
                    ])
                ],
            ),
            postprocess_options=entities.PostProcessOptions(
                standardize_coordinates=True,
            ),
        )

    @override
    def fetch_init_data(self, it: dt.datetime) -> Iterator[Callable[..., ResultE[xr.DataArray]]]:
        # Ensure class is authenticated
        authenticate_result = self.authenticate()
        if isinstance(authenticate_result, Failure):
            yield delayed(Result.from_failure)(authenticate_result.failure())
            return

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

        for parameter in parameter_stubs:
            for area in [f"Area{c}" for c in "ABCDEFGH"]:
                url = (
                        f"{self.url_base}/{it:%Y/%m/%d}/"
                        + f"{it:%Y%m%d%H}_WSGlobal17km_{parameter}_{area}_000144.grib"
                )
                yield delayed(self._download_and_convert)(url=url)

        pass

    def _download_and_convert(
        self,
        url: str,
    ) -> ResultE[xr.DataArray]:
        """Download and convert a file to an xarray dataset.

        Args:
            url: The URL of the file to download.

        Returns:
            A ResultE containing the xarray dataset.
        """
        return self._download(url).bind(self._convert)

    def authenticate(self) -> ResultE[None]:
        """Authenticate with the CEDA FTP server.

        Returns:
            A Result containing None if successful, or an error if not.
        """
        if all(k not in os.environ for k in self.metadata.required_env):
            return Result.from_failure(ValueError(
                f"Missing required environment variables: {self.metadata.required_env}",
            ))
        username: str = urllib.parse.quote(os.environ["CEDA_FTP_USER"])
        password: str = urllib.parse.quote(os.environ["CEDA_FTP_PASS"])

        self._url_auth = f"ftp://{username}:{password}@"
        return Result.from_value(None)

    def _download(self, url: str) -> ResultE[pathlib.Path]:
        """Download a file from the CEDA FTP server.

        Args:
            url: The URL of the file to download.
        """
        if self._url_auth is None:
            return Result.from_failure(
                ValueError(
                    "Not authenticated with CEDA FTP server. "
                    "Ensure the 'authenticate' method has been called.",
                ),
            )

        local_path: pathlib.Path = (
                pathlib.Path(
                    f"{os.getenv('NWP_WORKDIR', f'~/.local/cache/nwp/{self.metadata.name}')}/raw",
                )
                / url.split("/")[-1]
        )

        # Don't download the file if it already exists
        if not local_path.exists():
            log.debug("Sending request to CEDA FTP server for: '%s'", url)
            try:
                response = urllib.request.urlopen(  # noqa: S310
                    self._url_auth + url,
                    timeout=30,
                )
            except Exception as e:
                return Result.from_failure(OSError(f"Error fetching {url}: {e}"))

            local_path.parent.mkdir(parents=True, exist_ok=True)
            log.debug("Downloading %s to %s", url, local_path)
            try:
                with local_path.open("wb") as f:
                    for chunk in iter(lambda: response.read(16 * 1024), b""):
                        f.write(chunk)
                        f.flush()
                log.debug(
                    "Downloaded %s to %s (%s bytes)",
                    url,
                    local_path,
                    local_path.stat().st_size,
                )
            except Exception as e:
                return Result.from_failure(
                    OSError(
                        f"Error saving '{url}' to '{local_path}': {e}",
                    ),
                )

        return Result.from_value(local_path)

    def _convert(self, path: pathlib.Path) -> ResultE[xr.DataArray]:
        """Convert a file to an xarray dataset.

        Args:
            path: The path to the file to convert.
        """
        try:
            ds: xr.Dataset = xr.open_dataset(path, engine="cfgrib")
        except Exception as e:
            return Result.from_failure(
                OSError(
                    f"Error opening '{path}' as xarray Dataset: {e}",
                ),
            )
        try:
            da: xr.DataArray = (
                ds.sel(step=[np.timedelta64(i, "h") for i in range(0, 48, 1)])
                .expand_dims(dim={"init_time": [ds["time"].values]})
                .drop_vars(
                    names=[
                        v
                        for v in ds.coords.variables
                        if v not in ["init_time", "step", "latitude", "longitude"]
                    ],
                )
                .pipe(_rename_vars)
                .to_dataarray(name=self.metadata.name)
                .transpose("init_time", "step", "variable", "latitude", "longitude")
                # Remove the last value of the longitude dimension as it overlaps with the next file
                # Reverse the latitude dimension to be in descending order
                .isel(longitude=slice(None, -1), latitude=slice(None, None, -1))
            )
        except Exception as e:
            return Result.from_failure(
                ValueError(
                    f"Error processing {path} to DataArray: {e}",
                ),
            )
        return Result.from_value(da)


def _rename_vars(ds: xr.Dataset) -> xr.Dataset:
    """Rename variables to match the expected names.

    To find the names as they exist in the raw files, the following
    function was used:

    >>> import xarray as xr
    >>> import urllib.request
    >>> import datetime as dt
    >>>
    >>> def download_single_file(parameter: str) -> xr.Dataset:
    >>>     it = dt.datetime(2021, 1, 1, 0, tzinfo=dt.UTC)
    >>>     base_url = "ftp://<user>:<pass>@ftp.ceda.ac.uk/badc/ukmo-nwp/data/global-grib"
    >>>     url = f"{base_url}/{it:%Y/%m/%d}/" + \
    >>>           f"{it:%Y%m%d%H}_WSGlobal17km_{parameter}_AreaA_000144.grib"
    >>>     response = urllib.request.urlopen(url)
    >>>     with open("/tmp/mo-global/test.grib", "wb") as f:
    >>>         for chunk in iter(lambda: response.read(16 * 1024), b""):
    >>>             f.write(chunk)
    >>>             f.flush()
    >>>
    >>>     ds = xr.open_dataset("/tmp/mo-global/test.grib", engine="cfgrib")
    >>>     return ds

    Args:
        ds: The xarray dataset to rename.
    """
    rename_map: dict[str, str] = {
        "t": entities.params.temperature_sl.name,
        "r": entities.params.relative_humidity_sl.name,
        "sf": entities.params.snow_depth_gl.name,
        "prate": entities.params.total_precipitation_rate_gl.name,
        "swavr": entities.params.downward_shortwave_radiation_flux_gl.name,
        "u": entities.params.wind_u_component_10m.name,
        "v": entities.params.wind_v_component_10m.name,
        "vis": entities.params.visibility_sl.name,
        "hcc": entities.params.cloud_cover_high.name,
        "lcc": entities.params.cloud_cover_low.name,
        "mcc": entities.params.cloud_cover_medium.name,
    }
    for old, new in rename_map.items():
        if old in ds.data_vars:
            ds = ds.rename_vars({old: new})
    return ds
