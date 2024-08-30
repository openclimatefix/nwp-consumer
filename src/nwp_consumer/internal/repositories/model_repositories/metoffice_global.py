"""Model repository implementation for MetOffice Global data from CEDA.

MetOffice global model data is stored on CEDA in segments:

- 4 areas for the northern hemisphere (A,B,C,D)
- 4 areas for the southern hemisphere (E,F,G,H)

See also:
    - https://www.metoffice.gov.uk/binaries/content/assets/metofficegovuk/pdf/data/global-atmospheric-model-17-km-resolution.pdf
    - https://catalogue.ceda.ac.uk/uuid/86df725b793b4b4cb0ca0646686bd783
"""

import datetime as dt
import logging
import os
import pathlib
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator

import numpy as np
import xarray as xr
from joblib import delayed
from returns.result import Result, ResultE

from nwp_consumer.internal import entities, ports

log = logging.getLogger("nwp-consumer")


class CedaMetOfficeGlobalModelRepository(ports.ModelRepository):
    """Repository implementation for the MetOffice global model data."""

    url_base: str = "ftp.ceda.ac.uk/badc/ukmo-nwp/data/global-grib"
    _url_auth: str

    def __init__(self) -> None:
        """Create a new instance."""
        username: str = urllib.parse.quote(os.environ["CEDA_FTP_USER"])
        password: str = urllib.parse.quote(os.environ["CEDA_FTP_PASS"])

        self._url_auth = f"ftp://{username}:{password}@"

    def fetch_init_data(self, it: dt.datetime) -> Iterator[Callable[..., ResultE[xr.DataArray]]]:
        """Overrides the corresponding method in the parent class."""
        parameter_stubs: list[str] = [
            "Total_Downward_Surface_SW_Flux",
            "high_cloud_amount",
            "low_cloud_amount",
            "medium_cloud_amount",
            "relative_humidity_1_5m",
            "snow_depth",
            "temperature_1_5m",
            "total_cloud",
            "total_precipitation_rate",
            "visibility",
            "wind_u_10m",
            "wind_v_10m",
        ]
        area_stubs: list[str] = [f"Area{c}" for c in "ABCDEFGH"]

        for parameter in parameter_stubs:
            for area in area_stubs:
                url = f"{self.url_base}/{it:%Y/%m/%d}/" + \
                      f"{it:%Y%m%d%H}_WSGlobal17km_{parameter}_{area}_000144.grib"

                yield delayed(self._download_and_convert)(url)

        pass

    @property
    def metadata(self) -> entities.ModelRepositoryMetadata:
        """Overrides the corresponding method in the parent class."""
        return entities.ModelRepositoryMetadata(
            name="ceda_metoffice_global_17km",
            is_archive=True,
            is_order_based=False,
            running_hours=[0, 6, 12, 18],
            delay_minutes=480,
            required_env=["CEDA_FTP_USER", "CEDA_FTP_PASS"],
            optional_env={},
            expected_coordinates={
                "init_time": [],
                "step": list(range(0, 48, 1)),
                "variable": [
                    entities.params.downward_shortwave_radiation_flux_gl,
                ],
                "latitude": np.arange(90, -90, -0.156).tolist(),
                "longitude": np.arange(-45, 316, 0.234).tolist(),
            },
        )

    def _download_and_convert(self, url: str) -> ResultE[xr.DataArray]:
        """Download and convert a file to an xarray dataset.

        Args:
            url: The URL of the file to download.

        Returns:
            A ResultE containing the xarray dataset.
        """
        log.debug("Sending request to CEDA FTP server for: '%s'", url)
        response = urllib.request.urlopen(self._url_auth + url)  # noqa: S310

        local_path: pathlib.Path = pathlib.Path(
            f"~/.local/cache/nwp/{self.metadata.name}/raw",
        ) / url.split("/")[-1]
        # Don't download the file if it already exists
        if not local_path.exists():
            local_path.parent.mkdir(parents=True, exist_ok=True)
            log.debug("Downloading %s to %s", url, local_path)
            try:
                with local_path.open("wb") as f:
                    for chunk in iter(lambda: response.read(16 * 1024), b""):
                        f.write(chunk)
                        f.flush()
            except Exception as e:
                return Result.from_failure(OSError(
                    f"Error saving {url} to {local_path}: {e}",
                ))

        try:
            da = xr.open_dataset(local_path, engine="cfgrib")
            da = (
                da.sel(step=[np.timedelta64(i, "h") for i in range(0, 48, 1)])
                .expand_dims(dim={"init_time": [da["time"].values]})
                .drop_vars(
                    names=[
                        v for v in da.coords.variables
                        if v not in ["init_time", "step", "latitude", "longitude"]
                    ],
                )
                .rename_vars({"swavr": "downward_shortwave_radiation_flux_gl"})
                .to_dataarray(name=self.metadata.name)
                .transpose("init_time", "step", "variable", "latitude", "longitude")
            )
        except Exception as e:
            return Result.from_failure(OSError(
                f"Error converting {url} to xarray dataset: {e}",
            ))

        return Result.from_value(da)
