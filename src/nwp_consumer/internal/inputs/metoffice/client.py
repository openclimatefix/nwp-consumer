"""Implements a client to fetch the data from the MetOffice API."""

import datetime as dt
import pathlib
import urllib.request

import requests
import structlog.stdlib
import xarray as xr
from typeid import TypeID

from nwp_consumer import internal

from ._models import MetOfficeFileInfo, MetOfficeResponse

log = structlog.getLogger()

# Defines the mapping from MetOffice parameter names to OCF parameter names
PARAMETER_RENAME_MAP: dict[str, str] = {
    "t2m": internal.OCFShortName.TemperatureAGL.value,
    "si10": internal.OCFShortName.WindSpeedSurfaceAdjustedAGL.value,
    "wdir10":
        internal.OCFShortName.WindDirectionFromWhichBlowingSurfaceAdjustedAGL.value,
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


class MetOfficeClient(internal.FetcherInterface):
    """Implements a client to fetch the data from the MetOffice API."""

    # MetOffice API Order ID to pull data from
    orderID: str

    # Base https URL for MetOffice's data endpoint
    baseurl: str

    # Query string headers to pass to the MetOffice API
    __headers: dict[str, str]

    def __init__(self, *, orderID: str, clientID: str, clientSecret: str):
        if any([value in [None, "", "unset"] for value in [clientID, clientSecret, orderID]]):
            raise KeyError("must provide clientID, clientSecret, and orderID")
        self.orderID: str = orderID
        self.baseurl: str = f"https://api-metoffice.apiconnect.ibmcloud.com/1.0.0/orders/{self.orderID}/latest"
        self.querystring: dict[str, str] = {"detail": "MINIMAL"}
        self.__headers: dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-IBM-Client-Id": clientID,
            "X-IBM-Client-Secret": clientSecret,
        }

    def downloadToTemp(self, *, fi: MetOfficeFileInfo) -> tuple[internal.FileInfoModel, pathlib.Path]:

        if self.__headers.get("X-IBM-Client-Id") is None \
                or self.__headers.get("X-IBM-Client-Secret") is None:
            log.error("all metoffice API credentials not provided")
            return fi, pathlib.Path()

        log.debug(
            event=f"requesting download of file",
            filename=fi.fname()
        )
        url: str = f"{self.baseurl}/{fi.fname()}/data"
        try:
            opener = urllib.request.build_opener()
            opener.addheaders = list(dict(
                self.__headers, **{"Accept": "application/x-grib"}
            ).items())
            urllib.request.install_opener(opener)
            response = urllib.request.urlopen(url=url)
            if not response.status == 200:
                log.warn(
                    event="error response received for download file request",
                    response=response.json(),
                    url=url
                )
                return fi, pathlib.Path()
        except Exception as e:
            log.warn(
                event="error calling url for file",
                url=url,
                filename=fi.fname(),
                error=e
            )
            return fi, pathlib.Path()

        # Stream the filedata into a temporary file
        tfp: pathlib.Path = internal.TMP_DIR / str(TypeID(prefix='nwpc'))
        with tfp.open("wb") as f:
            for chunk in iter(lambda: response.read(16 * 1024), b''):
                f.write(chunk)

        log.debug(
            event="fetched all data from file",
            filename=fi.fname(),
            url=url,
            filepath=tfp.as_posix(),
            nbytes=tfp.stat().st_size
        )

        return fi, tfp

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:

        if self.__headers.get("X-IBM-Client-Id") is None \
                or self.__headers.get("X-IBM-Client-Secret") is None:
            log.error("all metoffice API credentials not provided")
            return []

        if it.date() != dt.datetime.utcnow().date():
            log.warn("metoffice API only supports fetching data for the current day")
            return []

        # Fetch info for all files available on the input date
        response: requests.Response = requests.request(
            method="GET",
            url=self.baseurl,
            headers=self.__headers,
            params=self.querystring
        )
        if not response.ok:
            log.warn(
                event="error response from filelist endpoint",
                url=response.url,
                response=response.json()
            )
            return []

        # Map the response to a MetOfficeResponse object
        try:
            responseObj: MetOfficeResponse = MetOfficeResponse.Schema().load(response.json())
        except Exception as e:
            log.warn(
                event="response from metoffice does not match expected schema",
                error=e,
                response=response.json()
            )
            return []

        # Filter the file infos for the desired init time
        wantedFileInfos: list[MetOfficeFileInfo] = [
            fo for fo in responseObj.orderDetails.files
            if _isWantedFile(fi=fo, dit=it)
        ]

        return wantedFileInfos

    def mapTemp(self, *, p: pathlib.Path) -> xr.Dataset:

        # Cfgrib is built upon eccodes which needs an in-memory file to read from
        # Load the GRIB file as a cube
        try:
            parameterDataset: xr.Dataset = xr.open_dataset(
                p.as_posix(),
                engine='cfgrib',
                backend_kwargs={'read_keys': ['name', 'parameterNumber'], 'indexpath': ''},
                chunks={},
            )
        except Exception as e:
            log.warn(
                event="error loading raw file as dataset",
                error=e,
                filepath=p.as_posix()
            )
            return xr.Dataset()

        # Make the DataArray OCF-compliant
        # 1. Rename the parameter to the OCF short name
        currentName = list(parameterDataset.data_vars)[0]
        parameterNumber = parameterDataset[currentName].attrs["GRIB_parameterNumber"]

        # The two wind dirs are the only parameters read in as "unknown"
        # * Tell them apart via the parameterNumber attribute
        #   which lines up with the last number in the GRIB2 code specified below
        #   https://gridded-data-ui.cda.api.metoffice.gov.uk/glossary?groups=Wind&sortOrder=GRIB2_CODE
        match currentName, parameterNumber:
            case "unknown", 194:
                parameterDataset = parameterDataset.rename({
                    currentName: internal.OCFShortName.WindSpeedSurfaceAdjustedAGL.value})
            case "unknown", 195:
                parameterDataset = parameterDataset.rename({
                    currentName: internal.OCFShortName.WindDirectionFromWhichBlowingSurfaceAdjustedAGL.value})
            case x, int() if x in PARAMETER_RENAME_MAP.keys():
                parameterDataset = parameterDataset.rename({
                    x: PARAMETER_RENAME_MAP[x]})
            case _, _:
                log.warn(
                    event=f"encountered unknown parameter; ignoring file",
                    unknownparamname=currentName,
                    unknownparamnumber=parameterNumber,
                    filepath=p.as_posix()
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
        parameterDataset = parameterDataset \
            .drop_vars(names=["height", "pressure", "valid_time", "surface", "heightAboveGround"], errors="ignore") \
            .rename({"time": "init_time"}) \
            .expand_dims("init_time") \
            .to_array(dim="variable", name="UKV") \
            .to_dataset() \
            .transpose("init_time", "step", "variable", "y", "x") \
            .sortby("step") \
            .sortby("variable") \
            .chunk({
                "init_time": 1,
                "step": -1,
                "variable": -1,
                "y": len(parameterDataset.y) // 2,
                "x": len(parameterDataset.x) // 2,
            })

        return parameterDataset


def _isWantedFile(*, fi: MetOfficeFileInfo, dit: dt.datetime) -> bool:
    """Check if the input FileInfo corresponds to a wanted GRIB file.

    :param fi: FileInfo describing the file to check
    :param dit: Desired init time
    """
    # False if item has an init_time not equal to desired init time
    if fi.initTime().replace(tzinfo=None) != dit.replace(tzinfo=None):
        return False
    # False if item is one of the ones ending in +HH
    if "+" in fi.fname():
        return False

    return True
