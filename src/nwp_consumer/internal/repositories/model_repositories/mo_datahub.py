"""Repository implementation for data from MetOffice's DataHub service.

Repository Information
======================

The API documentation for the MetOffice Weather Datahub can be found at:
https://datahub.metoffice.gov.uk/docs/f/category/atmospheric/type/atmospheric/api-documentation

Documented Structure
--------------------

MetOffice provide a number of models, a few of which OCF consume. Their flagship deterministic
model us called the "Unified Model" (UM) and is run in two configurations: "Global" and "UK".
The "Global" model has a resolution of 10km and the "UK" model has a resolution of 2km.

See https://datahub.metoffice.gov.uk/docs/f/category/atmospheric/overview for more information.

Data is provided on a per-order basis, so the filestructure depends on the order ID.
For OCF's purposes, on file per parameter per step is requested.

Actual Structure
----------------

The latitude and longitude increments are ascertained from the GRIB2 file's metadata:
.. code-block:: none

    iDirectionIncrementInDegrees: 0.140625
    jDirectionIncrementInDegrees: 0.09375

"""

import datetime as dt
import json
import logging
import os
import pathlib
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, ClassVar, override

import xarray as xr
from joblib import delayed
from returns.result import Failure, ResultE, Success

from nwp_consumer.internal import entities, ports

if TYPE_CHECKING:
    import http.client

log = logging.getLogger("nwp-consumer")


class MetOfficeDatahubRawRepository(ports.RawRepository):
    """Repository implementation for data from MetOffice's DataHub service."""

    base_url: ClassVar[str] = "https://data.hub.api.metoffice.gov.uk/atmospheric-models/1.0.0/orders"

    request_url: str
    order_id: str
    _headers: dict[str, str]

    def __init__(self, order_id: str, api_key: str) -> None:
        """Create a new instance."""
        self._headers = {
            "Accept": "application/json",
            "apikey": api_key,
        }
        self.order_id = order_id
        self.request_url = f"{self.base_url}/{self.order_id}/latest"


    @staticmethod
    @override
    def repository() -> entities.RawRepositoryMetadata:
        return entities.RawRepositoryMetadata(
            name="MetOffice-Weather-Datahub",
            is_archive=False,
            is_order_based=True,
            running_hours=[0, 12],
            delay_minutes=60,
            max_connections=10,
            required_env=["METOFFICE_API_KEY", "METOFFICE_ORDER_ID"],
            optional_env={},
            postprocess_options=entities.PostProcessOptions(),
            available_models={
                "default": entities.Models.MO_UM_GLOBAL_10KM,
                "um-global-10km": entities.Models.MO_UM_GLOBAL_10KM,
            },
        )

    @staticmethod
    @override
    def model() -> entities.ModelMetadata:
        requested_model: str = os.getenv("MODEL", default="default")
        if requested_model not in MetOfficeDatahubRawRepository.repository().available_models:
            log.warn(
                f"Unknown model '{requested_model}' requested, falling back to default. ",
                "MetOffice Datahub repository only supports "
                f"'{list(MetOfficeDatahubRawRepository.repository().available_models.keys())}'. "
                "Ensure MODEL environment variable is set to a valid model name.",
            )
            requested_model = "default"
        return MetOfficeDatahubRawRepository.repository().available_models[requested_model]

    @classmethod
    @override
    def authenticate(cls) -> ResultE["MetOfficeDatahubRawRepository"]:
        """Authenticate with the MetOffice DataHub service."""
        missing_envs = cls.repository().missing_required_envs()
        if len(missing_envs) > 0:
            return Failure(OSError(
                f"Cannot authenticate with MetOffice DataHub service due to "
                f"missing required environment variables: {', '.join(missing_envs)}",
            ))
        api_key: str = os.environ["METOFFICE_API_KEY"]
        order_id: str = os.environ["METOFFICE_ORDER_ID"]
        return Success(cls(order_id=order_id, api_key=api_key))

    @override
    def fetch_init_data(
        self, it: dt.datetime,
    ) -> Iterator[Callable[..., ResultE[list[xr.DataArray]]]]:
        req: urllib.request.Request = urllib.request.Request(  # noqa: S310
            url=self.request_url + f"?detail=MINIMAL&runfilter={it:%Y%m%d%H}",
            headers=self._headers,
            method="GET",
        )
        log.debug(
            f"Calling MetOffice Datahub at '{req.get_full_url()}'",
        )

        # Request the list of files
        try:
            response: http.client.HTTPResponse = urllib.request.urlopen(req, timeout=30)  # noqa: S310
        except Exception as e:
            yield delayed(Failure)(OSError(
                "Unable to list files from MetOffice DataHub for order "
                f"{self.order_id} at '{self.request_url}'. "
                f"Ensure API key and Order ID are correct. Error context: {e}",
            ))
            return
        try:
            data = json.loads(
                response.read().decode(response.info().get_param("charset") or "utf-8"),  # type: ignore
            )
        except Exception as e:
            yield delayed(Failure)(ValueError(
                "Unable to decode JSON response from MetOffice DataHub. "
                "Check the response from the '/latest' endpoint looks as expected. "
                f"Error context: {e}",
            ))
            return
        urls: list[str] = []
        if "orderDetails" in data and "files" in data["orderDetails"]:
            for filedata in data["orderDetails"]["files"]:
                if "fileId" in filedata and "+" not in filedata["fileId"]:
                    urls.append(f"{self.request_url}/{filedata["fileId"]}/data")

        log.debug(
            f"Found {len(urls)} file(s) for init time '{it.strftime('%Y-%m-%d %H:%M')}' "
            f"in order '{self.order_id}'.",
        )

        for url in urls:
            yield delayed(self._download_and_convert)(url)

    def _download_and_convert(self, url: str) -> ResultE[list[xr.DataArray]]:
        """Download and convert a grib file from MetOffice Weather Datahub API.

        Args:
            url: The URL of the file of interest.
        """
        return self._download(url).bind(self._convert)

    def _download(self, url: str) -> ResultE[pathlib.Path]:
        """Download a grib file from MetOffice Weather Datahub API.

        Args:
            url: The URL of the file of interest.
        """
        local_path: pathlib.Path = (
                pathlib.Path(
                    os.getenv(
                        "RAWDIR",
                        f"~/.local/cache/nwp/{self.repository().name}/{self.model().name}/raw",
                    ),
                ) / f"{url.split("/")[-2]}.grib"
        ).expanduser()

        # Only download the file if not already present
        if not local_path.exists() or local_path.stat().st_size == 0:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            log.debug("Requesting file from MetOffice Weather Datahub API at: '%s'", url)

            req: urllib.request.Request = urllib.request.Request(  # noqa: S310
                url=url,
                headers=self._headers | {"Accept": "application/x-grib"},
                method="GET",
            )

            # Request the file
            try:
                response: http.client.HTTPResponse = urllib.request.urlopen(  # noqa: S310
                    req,
                    timeout=60,
                )
            except Exception as e:
                return Failure(OSError(
                    "Unable to request file data from MetOffice DataHub at "
                    f"'{url}': {e}",
                ))

            # Download the file
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
    def _convert(path: pathlib.Path) -> ResultE[list[xr.DataArray]]:
        """Convert a local grib file to xarray DataArrays.

        Args:
            path: The path to the file to convert.
        """
        try:
            # Read the file as a dataset, also reading the values of the keys in 'read_keys'
            ds: xr.Dataset = xr.open_dataset(
                path,
                engine="cfgrib",
                backend_kwargs={"read_keys": ["name", "parameterNumber"], "indexpath": ""},
                chunks={
                    "time": 1,
                    "step": -1,
                },
            )
        except Exception as e:
            return Failure(
                OSError(
                    f"Error opening '{path}' as xarray Dataset: {e}",
                ),
            )

        # Some parameters are surfaced in the dataset as 'unknown'
        # and have to be differentiated via the parameterNumber attribute
        # which lines up with the last number in the GRIB2 code specified below
        # https://datahub.metoffice.gov.uk/docs/glossary?sortOrder=GRIB2_CODE
        name = next(iter(ds.data_vars))
        parameter_number = ds[name].attrs["GRIB_parameterNumber"]
        match name, parameter_number:
            case "unknown", 192:
                ds = ds.rename({name: "u10"})
            case "unknown", 193:
                ds = ds.rename({name: "v10"})
            case "unknown", 194:
                ds = ds.rename({name: "wdir"})
            case "unknown", 195:
                ds = ds.rename({name: "wdir10"})
            case "unknown", 1:
                ds = ds.rename({name: "tcc"})
            case "unknown", _:
                log.warning(
                    f"Encountered unknown parameter with parameterNumber {parameter_number} "
                    f"in file '{path}'.",
                )

        try:
            da: xr.DataArray = (
                ds.pipe(
                    entities.Parameter.rename_else_drop_ds_vars,
                    allowed_parameters=MetOfficeDatahubRawRepository.model().expected_coordinates.variable,
                )
                .rename(name_dict={"time": "init_time"})
                .expand_dims(dim="init_time")
                .expand_dims(dim="step")
                .to_dataarray(name=MetOfficeDatahubRawRepository.model().name)
            )
            da = (
                da.drop_vars(
                    names=[
                        c for c in ds.coords
                        if c not in
                        MetOfficeDatahubRawRepository.model().expected_coordinates.dims
                    ],
                    errors="ignore",
                )
                .transpose(*MetOfficeDatahubRawRepository.model().expected_coordinates.dims)
                .sortby(variables=["step", "variable", "longitude"])
                .sortby(variables="latitude", ascending=False)
            )
        except Exception as e:
            return Failure(
                ValueError(
                    f"Error processing DataArray for path '{path}'. Error context: {e}",
                ),
            )


        return Success([da])
