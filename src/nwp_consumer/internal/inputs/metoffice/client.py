"""Implements a client to fetch the data from the MetOffice API."""

import datetime as dt
import tempfile
import urllib.request

import requests
import structlog.stdlib
import xarray as xr

from nwp_consumer import internal

from ._models import MetOfficeFileInfo, MetOfficeResponse

log = structlog.stdlib.get_logger()

# Defines the mapping from MetOffice parameter names to OCF parameter names
PARAMETER_RENAME_MAP: dict[str, str] = {
    "temperature": internal.OCFShortName.TemperatureAGL,
    "wind-speed-surface-adjusted": internal.OCFShortName.WindSpeedSurfaceAdjustedAGL,
    "wind-direction-from-which-blowing-surface-adjusted":
        internal.OCFShortName.WindDirectionFromWhichBlowingSurfaceAdjustedAGL,
    "high-cloud-cover": internal.OCFShortName.HighCloudCover,
    "medium-cloud-cover": internal.OCFShortName.MediumCloudCover,
    "low-cloud-cover": internal.OCFShortName.LowCloudCover,
    "visibility": internal.OCFShortName.VisibilityAGL,
    "relative-humidity": internal.OCFShortName.RelativeHumidityAGL,
    "rain-precipitation-rate": internal.OCFShortName.RainPrecipitationRate,
    "total-precipitation-rate": internal.OCFShortName.RainPrecipitationRate,
    "snow-depth-water-equivalent": internal.OCFShortName.SnowDepthWaterEquivalent,
    "downward-short-wave-radiation-flux": internal.OCFShortName.DownwardShortWaveRadiationFlux,
    "downward-long-wave-radiation-flux": internal.OCFShortName.DownwardLongWaveRadiationFlux,
}


class MetOfficeClient(internal.FetcherInterface):
    """Implements a client to fetch the data from the MetOffice API."""

    # MetOffice API Order ID to pull data from
    orderID: str

    # Base https URL for MetOffice's data endpoint
    baseurl: str

    # Query string headers to pass to the MetOffice API
    __headers: dict[str, str]

    def __init__(self, orderID: str, clientID: str, clientSecret: str):
        if any([value in [None, "", "unset"] for value in [clientID, clientSecret, orderID]]):
            raise KeyError("Must provide clientID, clientSecret, and orderID")
        self.orderID: str = orderID
        self.baseurl: str = f"https://api-metoffice.apiconnect.ibmcloud.com/1.0.0/orders/{self.orderID}/latest"
        self.querystring: dict[str, str] = {"detail": "MINIMAL"}
        self.__headers: dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-IBM-Client-Id": clientID,
            "X-IBM-Client-Secret": clientSecret,
        }

    def fetchRawFileBytes(self, fileInfo: MetOfficeFileInfo) -> tuple[internal.FileInfoModel, bytes]:
        """Downloads a GRIB file corresponding to the input FileInfo object."""

        log.debug(f"Requesting download of {fileInfo.fileId}", item=fileInfo.fileId)
        url: str = f"{self.baseurl}/{fileInfo.fileId}/data"
        try:
            opener = urllib.request.build_opener()
            opener.addheaders = list(dict(self.__headers, **{"Accept": "application/x-grib"}).items())
            urllib.request.install_opener(opener)
            response = urllib.request.urlopen(url=url)
            if not response.status == 200:
                raise ConnectionError(f"Error response code {response.status} for url {url}: {response.read()}")
        except Exception as e:
            raise ConnectionError(f"Error calling url {url} for {fileInfo.fileId}: {e}")

        log.debug(f"Fetched all data from {fileInfo.fileId}", path=url)

        return fileInfo, response.read()

    def listRawFilesForInitTime(self, initTime: dt.datetime) -> list[internal.FileInfoModel]:
        """Get a list of FileInfo objects according to the input initTime."""

        if initTime.date() != dt.datetime.utcnow().date():
            log.warn("MetOffice API only supports fetching data for the current day")
            return []

        # Fetch info for all files available on the input date
        response: requests.Response = requests.request(
            method="GET",
            url=self.baseurl,
            headers=self.__headers,
            params=self.querystring
        )
        if not response.ok:
            raise AssertionError(f"Response did not return with an ok status: {response.content}")

        # Map the response to a MetOfficeResponse object
        try:
            responseObj: MetOfficeResponse = MetOfficeResponse.Schema().load(response.json())
        except Exception as e:
            raise TypeError(
                f"Error marshalling json to MetOfficeResponse object: {e}, response: {response.json()}"
            )

        # Filter the file infos for the desired init time
        wantedFileInfos: list[MetOfficeFileInfo] = [
            fo for fo in responseObj.orderDetails.files if _isWantedFile(fileInfo=fo, desiredInitTime=initTime)
        ]

        return wantedFileInfos

    def loadRawInitTimeDataAsOCFDataset(self, fileBytesList: list[bytes]) -> xr.Dataset:
        """Converts a list of raw file bytes into an OCF XArray Dataset."""

        # Load the single parameter files as OCF DataArrays
        parameterDataArrays: list[xr.Dataset] = [
            _loadSingleParameterGRIBAsOCFDataset(data=bd) for bd in fileBytesList
        ]

        # Merge the DataArrays into a single Dataset
        dataset = xr.merge(
            parameterDataArrays,
            compat='identical', combine_attrs='drop_conflicts'
        )

        # Load the whole dataset into memory
        # * Enables the deletion of the old DataArrays
        dataset.load()
        del parameterDataArrays

        # Add the init time as a coordinate
        dataset = dataset \
            .rename({"time": "init_time"}) \
            .expand_dims("init_time") \
            .chunk("auto") \
            .load()

        return dataset


def _loadSingleParameterGRIBAsOCFDataset(data: bytes) -> xr.Dataset:
    """Loads a MetOffice single-parameter GRIB file as an OCF-compliant DataArray."""
    parameterDataset: xr.Dataset = xr.Dataset()

    # Cfgrib is built upon eccodes which needs an in-memory file to read from
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".grib2") as tempParameterFile:
        # Copy the raw file to a local temp file
        tempParameterFile.write(data)
        tempParameterFile.seek(0)

        # Load the GRIB file as a cube
        try:
            parameterDataset: xr.Dataset = xr.open_dataset(
                tempParameterFile.name, engine='cfgrib',
                backend_kwargs={'read_keys': ['name', 'parameterName']}
            )
        except Exception as e:
            raise ValueError(f"Failed to load GRIB file as a cube: {e}")

        parameterDataset.load()

    # TODO: Handle unknowns
    # Make the DataArray OCF-compliant
    # * Rename the parameter to the OCF name
    # * Add the init time as a coordinate
    # * Rename the time dimension to step_time
    # * Compute the dataset to load the data from the temporary file
    for oldName, newName in PARAMETER_RENAME_MAP.items():
        if oldName in [parameterDataset.data_vars]:
            parameterDataset = parameterDataset.rename({oldName: str(newName)})
    parameterDataset = parameterDataset \
        .drop_vars(["height", "pressure", "valid_time", "surface"], errors="ignore") \
        .compute()

    return parameterDataset


def _isWantedFile(fileInfo: MetOfficeFileInfo, desiredInitTime: dt.datetime) -> bool:
    """Checks if the input FileInfo corresponds to a wanted GRIB file."""
    # False if item has an init_time not equal to desired init time
    if fileInfo.initTime().replace(tzinfo=dt.timezone.utc) != desiredInitTime.replace(tzinfo=dt.timezone.utc):
        return False
    # False if item is one of the ones ending in +HH
    if "+" in fileInfo.fname():
        return False

    log.debug("Found wanted file from MetOffice", file=fileInfo.fname())

    return True
