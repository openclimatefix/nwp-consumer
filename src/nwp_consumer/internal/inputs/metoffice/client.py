"""Implements a client to fetch the data from the MetOffice API."""

import datetime as dt
import pathlib
import urllib.request

import pyproj
import requests
import structlog.stdlib
import xarray as xr

from nwp_consumer import internal

from ._models import MetOfficeFileInfo, MetOfficeResponse

log = structlog.getLogger()

# Defines the mapping from MetOffice parameter names to OCF parameter names
PARAMETER_RENAME_MAP: dict[str, str] = {
    "t2m": internal.OCFShortName.TemperatureAGL.value,
    "si10": internal.OCFShortName.WindSpeedSurfaceAdjustedAGL.value,
    "wdir10": internal.OCFShortName.WindDirectionFromWhichBlowingSurfaceAdjustedAGL.value,
    "hcc": internal.OCFShortName.HighCloudCover.value,
    "mcc": internal.OCFShortName.MediumCloudCover.value,
    "lcc": internal.OCFShortName.LowCloudCover.value,
    "vis": internal.OCFShortName.VisibilityAGL.value,
    "r2": internal.OCFShortName.RelativeHumidityAGL.value,
    "rprate": internal.OCFShortName.RainPrecipitationRate.value,
    "tprate": internal.OCFShortName.RainPrecipitationRate.value,
    "sd": internal.OCFShortName.SnowDepthWaterEquivalent.value,
    "dswrf": internal.OCFShortName.DownwardShortWaveRadiationFlux.value,
    "dlwrf": internal.OCFShortName.DownwardLongWaveRadiationFlux.value,
}


class Client(internal.FetcherInterface):
    """Implements a client to fetch the data from the MetOffice API."""

    # Base https URL for MetOffice's data endpoint
    baseurl: str

    # Query string headers to pass to the MetOffice API
    __headers: dict[str, str]

    def __init__(self, *, orderID: str, clientID: str, clientSecret: str) -> None:
        """Create a new MetOfficeClient.

        Exposes a client for the MetOffice API which conforms to the FetcherInterface.
        MetOffice API credentials must be provided, as well as an orderID for the
        desired dataset.

        Args:
            orderID: The orderID to fetch from the MetOffice API.
            clientID: The clientID for the MetOffice API.
            clientSecret: The clientSecret for the MetOffice API.
        """
        if any(value in [None, "", "unset"] for value in [clientID, clientSecret, orderID]):
            raise KeyError("must provide clientID, clientSecret, and orderID")
        self.baseurl: str = (
            f"https://api-metoffice.apiconnect.ibmcloud.com/1.0.0/orders/{orderID}/latest"
        )
        self.querystring: dict[str, str] = {"detail": "MINIMAL"}
        self.__headers: dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-IBM-Client-Id": clientID,
            "X-IBM-Client-Secret": clientSecret,
        }

    def getInitHours(self) -> list[int]:  # noqa: D102
        # NOTE: This will depend on the order you have with the MetOffice.
        # Technically they can provide data for every hour of the day,
        # but OpenClimateFix choose to match what is available from CEDA.
        return [0, 3, 6, 9, 12, 15, 18, 21]

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:  # noqa: D102
        if (
            self.__headers.get("X-IBM-Client-Id") is None
            or self.__headers.get("X-IBM-Client-Secret") is None
        ):
            log.error("all metoffice API credentials not provided")
            return []

        if it.date() != dt.datetime.now(tz=dt.UTC).date():
            log.warn("metoffice API only supports fetching data for the current day")
            return []

        # Ignore inittimes that don't correspond to valid hours
        if it.hour not in self.getInitHours():
            return []

        # Fetch info for all files available on the input date
        response: requests.Response = requests.request(
            method="GET",
            url=self.baseurl,
            headers=self.__headers,
            params=self.querystring,
        )
        try:
            rj: dict = response.json()
        except Exception as e:
            log.warn(
                event="error parsing response from filelist endpoint",
                error=e,
                response=response.content,
            )
            return []
        if not response.ok or ("httpCode" in rj and int(rj["httpCode"]) > 399):
            log.warn(
                event="error response from filelist endpoint",
                url=response.url,
                response=rj,
            )
            return []

        # Map the response to a MetOfficeResponse object
        try:
            responseObj: MetOfficeResponse = MetOfficeResponse.Schema().load(response.json())
        except Exception as e:
            log.warn(
                event="response from metoffice does not match expected schema",
                error=e,
                response=response.json(),
            )
            return []

        # Filter the file infos for the desired init time
        wantedFileInfos: list[MetOfficeFileInfo] = [
            fo for fo in responseObj.orderDetails.files if _isWantedFile(fi=fo, dit=it)
        ]

        return wantedFileInfos

    def downloadToTemp(  # noqa: D102
        self,
        *,
        fi: internal.FileInfoModel,
    ) -> tuple[internal.FileInfoModel, pathlib.Path]:
        if (
            self.__headers.get("X-IBM-Client-Id") is None
            or self.__headers.get("X-IBM-Client-Secret") is None
        ):
            log.error("all metoffice API credentials not provided")
            return fi, pathlib.Path()

        log.debug(
            event="requesting download of file",
            file=fi.filename(),
        )
        url: str = f"{self.baseurl}/{fi.filepath()}"
        try:
            opener = urllib.request.build_opener()
            opener.addheaders = list(
                dict(
                    self.__headers,
                    **{"Accept": "application/x-grib"},
                ).items(),
            )
            urllib.request.install_opener(opener)
            response = urllib.request.urlopen(url=url)
            if response.status != 200:
                log.warn(
                    event="error response received for download file request",
                    response=response.json(),
                    url=url,
                )
                return fi, pathlib.Path()
        except Exception as e:
            log.warn(
                event="error calling url for file",
                url=url,
                filename=fi.filename(),
                error=e,
            )
            return fi, pathlib.Path()

        # Stream the filedata into a temporary file
        tfp: pathlib.Path = internal.TMP_DIR / fi.filename()
        with tfp.open("wb") as f:
            for chunk in iter(lambda: response.read(16 * 1024), b""):
                f.write(chunk)
                f.flush()

        log.debug(
            event="fetched all data from file",
            filename=fi.filename(),
            url=url,
            filepath=tfp.as_posix(),
            nbytes=tfp.stat().st_size,
        )

        return fi, tfp


    def mapTemp(self, *, p: pathlib.Path) -> xr.Dataset:  # noqa: D102
        if p.suffix != ".grib":
            log.warn(
                event="cannot map non-grib file to dataset",
                filepath=p.as_posix(),
            )
            return xr.Dataset()

        log.debug(
            event="mapping raw file to xarray dataset",
            filepath=p.as_posix(),
        )

        # Cfgrib is built upon eccodes which needs an in-memory file to read from
        # Load the GRIB file as a cube
        try:
            # Read the file as a dataset, also reading the values of the keys in 'read_keys'
            # * Can also set backend_kwargs={"indexpath": ""}, to avoid the index file
            parameterDataset: xr.Dataset = xr.open_dataset(
                p.as_posix(),
                engine="cfgrib",
                backend_kwargs={"read_keys": ["name", "parameterNumber"]},
                chunks={
                    "time": 1,
                    "step": -1,
                    "variable": -1,
                    "x": "auto",
                    "y": "auto",
                },
            )
        except Exception as e:
            log.warn(
                event="error loading raw file as dataset",
                error=e,
                filepath=p.as_posix(),
            )
            return xr.Dataset()

        # Make the DataArray OCF-compliant
        # 1. Rename the parameter to the OCF short name
        currentName = next(iter(parameterDataset.data_vars))
        parameterNumber = parameterDataset[currentName].attrs["GRIB_parameterNumber"]

        # The two wind dirs are the only parameters read in as "unknown"
        # * Tell them apart via the parameterNumber attribute
        #   which lines up with the last number in the GRIB2 code specified below
        #   https://gridded-data-ui.cda.api.metoffice.gov.uk/glossary?groups=Wind&sortOrder=GRIB2_CODE
        match currentName, parameterNumber:
            case "unknown", 194:
                parameterDataset = parameterDataset.rename(
                    {
                        currentName: internal.OCFShortName.WindDirectionFromWhichBlowingSurfaceAdjustedAGL.value,
                    },
                )
            case "unknown", 195:
                parameterDataset = parameterDataset.rename(
                    {currentName: internal.OCFShortName.WindSpeedSurfaceAdjustedAGL.value},
                )
            case x, int() if x in PARAMETER_RENAME_MAP:
                parameterDataset = parameterDataset.rename({x: PARAMETER_RENAME_MAP[x]})
            case _, _:
                log.warn(
                    event="encountered unknown parameter; ignoring file",
                    unknownparamname=currentName,
                    unknownparamnumber=parameterNumber,
                    filepath=p.as_posix(),
                )
                return xr.Dataset()

        # 2. Remove unneeded variables
        # 3. Rename and Expand the init_time dimension
        # 4. Create a chunked Dask Dataset from the input multi-variate Dataset.
        # *  Converts the input multivariate DataSet (with different DataArrays for
        #     each NWP variable) to a single DataArray with a `variable` dimension.
        # * This allows each Zarr chunk to hold multiple variables (useful for loading
        #     many/all variables at once from disk).
        # * The chunking is done in such a way that each chunk is a single time step
        #     for a single variable.
        # * Transpose the Dataset so that the dimensions are correctly ordered
        # * Reverse `latitude` so it's top-to-bottom via reindexing.
        parameterDataset = (
            parameterDataset.drop_vars(
                names=[
                    "height",
                    "pressure",
                    "valid_time",
                    "surface",
                    "heightAboveGround",
                    "atmosphere",
                    "cloudBase",
                    "meanSea",
                    "heightAboveGroundLayer",
                    "level",
                ],
                errors="ignore",
            )
            .rename({"time": "init_time"})
            .expand_dims(["init_time"])
            .to_array(dim="variable", name="UKV")
            .sortby("y", ascending=False)
            .to_dataset()
            .transpose("variable", "init_time", "step", "y", "x")
            .sortby("step")
            .sortby("variable")
            .chunk(
                {
                    "variable": -1,
                    "init_time": 1,
                    "step": -1,
                    "y": len(parameterDataset.y) // 2,
                    "x": len(parameterDataset.x) // 2,
                },
            )
        )

        # TODO: Remove this by moving this logic into ocf-datapipes and update PVNet1+2 to use that
        # TODO: See issue #26 https://github.com/openclimatefix/nwp-consumer/issues/26
        # 5. Create osgb x and y coordinates from the lat/lon coordinates
        # * The lat/lon coordinates are WGS84, i.e. EPSG:4326
        # * The OSGB coordinates are EPSG:27700
        # * Approximate the osgb values by taking the first row and column of the
        #   transformed x/y grids
        latlonOsgbTransformer = pyproj.Transformer.from_crs(
            crs_from=4326,
            crs_to=27700,
            always_xy=True,
        )
        osgbX, osgbY = latlonOsgbTransformer.transform(
            parameterDataset.longitude.values,
            parameterDataset.latitude.values,
        )
        osgbX = osgbX.astype(int)
        osgbY = osgbY.astype(int)
        parameterDataset = parameterDataset.assign_coords(
            {
                "x": osgbX[0],
                "y": [osgbY[i][0] for i in range(len(osgbY))],
            },
        )

        return parameterDataset


def _isWantedFile(*, fi: MetOfficeFileInfo, dit: dt.datetime) -> bool:
    """Check if the input FileInfo corresponds to a wanted GRIB file.

    :param fi: FileInfo describing the file to check
    :param dit: Desired init time
    """
    # False if item has an init_time not equal to desired init time
    if fi.it().replace(tzinfo=None) != dit.replace(tzinfo=None):
        return False
    # False if item is one of the ones ending in +HH
    if "+" in fi.filename():
        return False

    return True
