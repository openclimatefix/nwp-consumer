"""Repository implementation for data from MetOffice's DataHub service.

Repository Information
======================

The API documentation for the MetOffice Weather Datahub can be found at:
https://datahub.metoffice.gov.uk/docs/f/category/atmospheric/type/atmospheric/api-documentation

Documented Structure
--------------------

MetOffice provide a number of models, a few of which OCF consume. Their flagship deterministic
model us called the "Unified Model" (UM) and is run in two configurations: "Global" and "UK".
The "Global" model has a resolution of 10km and the "UK" model has a resolution of ~2km.

See https://datahub.metoffice.gov.uk/docs/f/category/atmospheric/overview for more information.

Data is provided on a per-order basis, so the filestructure depends on the order ID.
For OCF's purposes, on file per parameter per step is requested.


Actual Structure
----------------

For the global model, the grid of the provided data matches the described structure.
The latitude and longitude increments are ascertained from the GRIB2 file's metadata:
.. code-block:: none

    iDirectionIncrementInDegrees: 0.140625
    jDirectionIncrementInDegrees: 0.09375

The UKV model X coordinate from MetOffice Datahub is as follows:

    DimCoord :  projection_x_coordinate / (m)
    points: [
        -575999.97097653, -573999.97097653, ...,  330000.02902347,
         332000.02902347]
    shape: (455,)
    dtype: float64
    standard_name: 'projection_x_coordinate'
    coord_system: LambertAzimuthalEqualArea(
        latitude_of_projection_origin=54.9,
        longitude_of_projection_origin=-2.5,
        false_easting=0.0,
        false_northing=0.0,
        ellipsoid=GeogCS(semi_major_axis=6378137.0, semi_minor_axis=6356752.314140356))

Which differs from the CEDA UKV data, as that is projected onto an OSGB grid:

    DimCoord :  projection_x_coordinate / (m)
    points: [-238000., -236000., ...,  854000.,  856000.]
    shape: (548,)
    dtype: float64
    standard_name: 'projection_x_coordinate'
    coord_system: TransverseMercator(
        latitude_of_projection_origin=49.0,
        longitude_of_central_meridian=-2.0,
        false_easting=400000.0,
        false_northing=-100000.0,
        scale_factor_at_central_meridian=0.0,
        ellipsoid=GeogCS(semi_major_axis=6377563.4, semi_minor_axis=6356256.91))

Similar for the Y values, for MetOffice:

    DimCoord :  projection_y_coordinate / (m)
    points: [
        -576000.00814487, -574000.00814487, ...,  697999.99185513,
         699999.99185513]
    shape: (639,)
    dtype: float64
    standard_name: 'projection_y_coordinate'
    coord_system: LambertAzimuthalEqualArea(
        latitude_of_projection_origin=54.9,
        longitude_of_projection_origin=-2.5,
        false_easting=0.0,
        false_northing=0.0,
        ellipsoid=GeogCS(semi_major_axis=6378137.0, semi_minor_axis=6356752.314140356))

And for CEDA:

    DimCoord :  projection_y_coordinate / (m)
    points: [1222000., 1220000., ..., -182000., -184000.]
    shape: (704,)
    dtype: float64
    standard_name: 'projection_y_coordinate'
    coord_system: TransverseMercator(
        latitude_of_projection_origin=49.0,
        longitude_of_central_meridian=-2.0,
        false_easting=400000.0,
        false_northing=-100000.0,
        scale_factor_at_central_meridian=0.0,
        ellipsoid=GeogCS(semi_major_axis=6377563.4, semi_minor_axis=6356256.91))


MetOffice's region selection allows you to choose a GB and EIRE region, which is then
defined as a bounding box from

    (-576719N, -576719E) to (700220N, 333408E)

Which doesn't quite line up with the values extracted from the grib above.


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

    base_url: ClassVar[str] = (
        "https://data.hub.api.metoffice.gov.uk/atmospheric-models/1.0.0/orders"
    )

    request_url: str
    order_id: str
    dataspec: str
    _headers: dict[str, str]

    def __init__(self, order_id: str, api_key: str) -> None:
        """Create a new instance."""
        self._headers = {
            "Accept": "application/json",
            "apikey": api_key,
        }
        self.order_id = order_id
        self.request_url = f"{self.base_url}/{self.order_id}/latest"
        self.dataspec = os.getenv(
            "METOFFICE_DATASPEC",
            self.repository().optional_env["METOFFICE_DATASPEC"],
        )

    @staticmethod
    @override
    def repository() -> entities.RawRepositoryMetadata:
        return entities.RawRepositoryMetadata(
            name="MetOffice-Weather-Datahub",
            is_archive=False,
            is_order_based=True,
            delay_minutes=120,
            max_connections=10,
            required_env=["METOFFICE_API_KEY", "METOFFICE_ORDER_ID"],
            optional_env={"METOFFICE_DATASPEC": "1.1.0"},
            postprocess_options=entities.PostProcessOptions(),
            available_models={
                "default": entities.Models.MO_UM_GLOBAL_10KM.with_region("india"),
                "um-global-10km-india": entities.Models.MO_UM_GLOBAL_10KM.with_region("india"),
                "um-global-10km-uk": entities.Models.MO_UM_GLOBAL_10KM.with_region("uk"),
                "um-ukv-2km": entities.Models.MO_UM_UKV_2KM_LAEA,
            },
        )

    @staticmethod
    @override
    def model() -> entities.ModelMetadata:
        requested_model: str = os.getenv("MODEL", default="default")
        if requested_model not in MetOfficeDatahubRawRepository.repository().available_models:
            log.warn(
                f"Unknown model '{requested_model}' requested, falling back to default. "
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
            return Failure(
                OSError(
                    f"Cannot authenticate with MetOffice DataHub service due to "
                    f"missing required environment variables: {', '.join(missing_envs)}",
                ),
            )
        api_key: str = os.environ["METOFFICE_API_KEY"]
        order_id: str = os.environ["METOFFICE_ORDER_ID"]
        return Success(cls(order_id=order_id, api_key=api_key))

    @override
    def fetch_init_data(
        self,
        it: dt.datetime,
    ) -> Iterator[Callable[..., ResultE[list[xr.DataArray]]]]:
        req: urllib.request.Request = urllib.request.Request(  # noqa: S310
            url=self.request_url + \
                f"?detail=MINIMAL&runfilter={it:%Y%m%d%H}&dataSpec={self.dataspec}",
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
            yield delayed(Failure)(
                OSError(
                    "Unable to list files from MetOffice DataHub for order "
                    f"{self.order_id} at '{self.request_url}'. "
                    f"Ensure API key and Order ID are correct. Error context: {e}",
                ),
            )
            return
        try:
            data = json.loads(
                response.read().decode(response.info().get_param("charset") or "utf-8"),  # type: ignore
            )
        except Exception as e:
            yield delayed(Failure)(
                ValueError(
                    "Unable to decode JSON response from MetOffice DataHub. "
                    "Check the response from the '/latest' endpoint looks as expected. "
                    f"Error context: {e}",
                ),
            )
            return
        urls: list[str] = []
        if "orderDetails" in data and "files" in data["orderDetails"]:
            for filedata in data["orderDetails"]["files"]:
                if "fileId" in filedata and "+" not in filedata["fileId"]:
                    urls.append(
                        f"{self.request_url}/{filedata["fileId"]}/data?dataSpec={self.dataspec}",
                    )

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
        if "um-global" in self.model().name:
            return self._download(url).bind(self._convert_global)
        elif "um-ukv" in self.model().name:
            return self._download(url).bind(self._convert_ukv)
        else:
            return Failure(
                ValueError(
                    f"Unknown model '{self.model().name}' requested. "
                    "Ensure MODEL environment variable is set to a valid model name.",
                ),
            )

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
            )
            / f"{url.split("/")[-2]}.grib"
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
                return Failure(
                    OSError(
                        "Unable to request file data from MetOffice DataHub at " f"'{url}': {e}",
                    ),
                )

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
    def _convert_global(path: pathlib.Path) -> ResultE[list[xr.DataArray]]:
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
                chunks={"time": 1, "step": -1},
            )
        except Exception as e:
            return Failure(OSError(f"Error opening '{path}' as xarray Dataset: {e}"))

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
                ds = ds.rename({name: "si10"})
            case "unknown", 1:
                ds = ds.rename({name: "tcc"})
            case "unknown", _:
                log.warning(
                    f"Encountered unknown parameter with parameterNumber {parameter_number} "
                    f"in file '{path}'.",
                )

        try:
            ds = (
                ds.pipe(
                    entities.Parameter.rename_else_drop_ds_vars,
                    allowed_parameters=MetOfficeDatahubRawRepository.model().expected_coordinates.variable,
                )
                .rename(name_dict={"time": "init_time"})
                .expand_dims(dim="init_time")
            )

            if "step" not in ds.dims:
                ds = ds.expand_dims(dim="step")

            da: xr.DataArray = ds.to_dataarray(name=MetOfficeDatahubRawRepository.model().name)
            da = da.drop_vars(
                names=[
                    c
                    for c in ds.coords
                    if c not in MetOfficeDatahubRawRepository.model().expected_coordinates.dims
                ],
                errors="ignore",
            ).transpose(*MetOfficeDatahubRawRepository.model().expected_coordinates.dims)

            da = da.sortby(variables=["step", "variable", "longitude"])
            da = da.sortby(variables="latitude", ascending=False)

        except Exception as e:
            return Failure(
                ValueError(f"Error processing DataArray for path '{path}'. Error context: {e}"),
            )

        return Success([da])

    @staticmethod
    def _convert_ukv(path: pathlib.Path) -> ResultE[list[xr.DataArray]]:
        """Convert a local UKV grib file to xarray DataArrays.

        Args:
            path: The path to the file to convert.
        """
        try:
            # Read the file as a dataset, also reading the values of the keys in 'read_keys'
            ds: xr.Dataset = xr.open_dataset(
                path,
                engine="cfgrib",
                backend_kwargs={"read_keys": ["name", "parameterNumber"], "indexpath": ""},
                chunks={"time": 1, "step": -1},
            )
        except Exception as e:
            return Failure(OSError(f"Error opening '{path}' as xarray Dataset: {e}"))

        try:
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
                    ds = ds.rename({name: "si10"})
                case "unknown", 1:
                    ds = ds.rename({name: "tcc"})
                case "unknown", _:
                    log.warning(
                        f"Encountered unknown parameter with parameterNumber {parameter_number} "
                        f"in file '{path}'.",
                    )

            # Replace the coords with the expected values, as cfrib struggles to read them in.
            # The actual values have been determined by using iris.
            expected_coords = MetOfficeDatahubRawRepository.model().expected_coordinates
            if ds.sizes["x"] != len(expected_coords.x_laea):  # type: ignore
                return Failure(
                    ValueError(
                        f"Coordinate length of '{ds.sizes['x']}' for the x dimension for file "
                        f"'{path!s}' does not match expected length {len(expected_coords.x_laea)}",  # type: ignore
                    ),
                )
            if ds.sizes["y"] != len(expected_coords.y_laea):  # type: ignore
                return Failure(
                    ValueError(
                        f"Coordinate length of '{ds.sizes['y']}' for the y dimension for file "
                        f"'{path!s}' does not match expected length {len(expected_coords.y_laea)}",  # type: ignore
                    ),
                )
            # Assign coordinates to the dataset, and reverse y so it is descending, before
            # replacing the stand in values with the actual ones
            ds = (
                ds.assign_coords(
                    x=list(range(ds.sizes["x"])),
                    y=list(range(ds.sizes["y"])),
                )
                .sortby("y", ascending=False)
                .sortby("x")
                .assign_coords(
                    x=expected_coords.x_laea,
                    y=expected_coords.y_laea,
                )
                .rename({"x": "x_laea", "y": "y_laea"})
            )

            # Remove unwanted variables
            ds = (
                ds.pipe(
                    entities.Parameter.rename_else_drop_ds_vars,
                    allowed_parameters=MetOfficeDatahubRawRepository.model().expected_coordinates.variable,
                )
                .rename(name_dict={"time": "init_time"})
                .expand_dims(dim="init_time")
            )

            if "step" not in ds.dims:
                ds = ds.expand_dims(dim="step")

            da: xr.DataArray = ds.to_dataarray(name=MetOfficeDatahubRawRepository.model().name)
            da = (
                da.drop_vars(
                    names=[
                        c
                        for c in ds.coords
                        if c not in MetOfficeDatahubRawRepository.model().expected_coordinates.dims
                    ],
                    errors="ignore",
                )
                .transpose(*MetOfficeDatahubRawRepository.model().expected_coordinates.dims)
                .sortby(variables=["step", "variable", "x_laea"])
            )

        except Exception as e:
            return Failure(
                ValueError(
                    f"Error processing DataArray for path '{path}'. Error context: {e}",
                ),
            )

        return Success([da])
