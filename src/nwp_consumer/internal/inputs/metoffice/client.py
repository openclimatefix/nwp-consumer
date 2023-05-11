"""Implements a client to fetch the data from the MetOffice API."""

import datetime as dt
import pathlib
import tempfile
import urllib.request
from concurrent.futures import ProcessPoolExecutor

import cfgrib
import numpy as np
import pandas as pd
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

    # Storage client
    storer: internal.StorageInterface

    def __init__(self, orderID: str, clientID: str, clientSecret: str, storer: internal.StorageInterface):
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
        self.storer = storer

    def downloadRawDataForInitTime(self, initTime: dt.datetime) -> list[pathlib.Path]:
        """Downloads the raw data for the given init time."""
        # Fetch all the raw file infos for the given init time
        fileInfosForInitTime: list[MetOfficeFileInfo] = self._getFileInfosForInitTime(initTime=initTime)

        if fileInfosForInitTime is None or len(fileInfosForInitTime) == 0:
            raise Exception(f"No files found for initTime {initTime}")

        # Download the raw parameter files given by the file infos
        with ProcessPoolExecutor(4) as p:
            rawRelPaths: list[pathlib.Path] = [x for x in p.map(self._downloadRawGRIBFile, fileInfosForInitTime)]

        # Shutdown the pool after all files have downloaded
        p.shutdown(wait=True, cancel_futures=False)

        return rawRelPaths

    def loadRawInitTimeDataAsOCFDataset(self, rawRelativePaths: list[pathlib.Path],
                                        initTime: dt.datetime) -> xr.Dataset:

        datasetName: str = initTime.strftime("%Y%m%dT%H%M")

        log.debug(
            f"Creating Dataset {datasetName} from {len(rawRelativePaths)} files",
            dataset=datasetName,
            files=[path.as_posix() for path in rawRelativePaths]
        )

        # Instantiate a list to hold the DataArrays for each parameter
        parameterDataArrays: list[xr.DataArray] = []

        # For each parameter file, load test_integration as a DataArray and add test_integration to the list
        for filePath in rawRelativePaths:
            try:
                parameterDataArray = self._loadSingleParameterGRIBAsOCFDataArray(path=filePath, initTime=initTime)
                parameterDataArrays.append(parameterDataArray)
            except Exception as e:
                raise Exception(f"Error loading file {filePath.as_posix()} as DataArray: {e}")

            log.debug(
                f"Merging DataArray from parameter {filePath.stem} into dataset {datasetName}",
                parameterFile=filePath.as_posix(), dataset=datasetName
            )

        # Merge the DataArrays into a single Dataset
        dataset = xr.merge(
            parameterDataArrays,
            compat='identical', combine_attrs='drop_conflicts'
        ).load()

        del parameterDataArrays

        dataset = dataset \
            .assign_coords({"init_time": np.datetime64(pd.Timestamp(initTime.replace(tzinfo=None)))}) \
            .expand_dims("init_time") \
            .chunk("auto") \
            .load()

        return dataset

    def _loadSingleParameterGRIBAsOCFDataArray(self, path: pathlib.Path, initTime: dt.datetime) -> xr.DataArray:
        """Loads a single-parameter GRIB file as an OCF-compliant DataArray."""
        # cfgrib can't take a file-like object as input, so we have to download the file to a tempfile again
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".grib2") as tempParameterFile:
            # Copy the raw file to a local temp file
            tempParameterFile.write(self.storer.readBytesFromRawDir(relativePath=path))
            tempParameterFile.seek(0)

            # Load the GRIB file as a cube
            try:
                parameterDataArray: xr.DataArray = cfgrib.open_dataset(tempParameterFile.name).to_array()
            except Exception as e:
                raise ValueError(f"Failed to load GRIB file from {path} as a cube: {e}")

            parameterDataArray.load()

        # Make the DataArray OCF-compliant
        # * Rename the parameter to the OCF name
        # * Add the init time as a coordinate
        # * Rename the time dimension to step_time
        # * Compute the dataset to load the data from the temporary file
        parameterDataArray = parameterDataArray \
            .rename(PARAMETER_RENAME_MAP[_getParameterNameFromFileName(fileName=path.stem)]) \
            .drop_vars(["height", "pressure", "valid_time", "time"], errors="ignore") \
            .compute()

        return parameterDataArray

    def _downloadRawGRIBFile(self, fileInfo: MetOfficeFileInfo) -> pathlib.Path:
        """Downloads a GRIB file corresponding to the input FileInfo object."""
        # Set the relative filepath to be the date of the file
        # * For example 2021/01/01/20210101T0000_Wholesale1.grib2
        relativeFilePath: pathlib.Path = pathlib.Path(
            fileInfo.initTime().strftime("%Y/%m/%d"), fileInfo.fileId)

        if self.storer.existsInRawDir(relativePath=relativeFilePath):
            log.debug(f"File already exists: {fileInfo.fileId}")

        else:
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
            try:
                savedPath = self.storer.writeBytesToRawDir(relativePath=relativeFilePath, data=response.read())
            except Exception as e:
                raise IOError(f"Error saving file {fileInfo.fileId} to {relativeFilePath}: {e}")

            log.debug(f"Downloaded item: {fileInfo.fileId}", item=fileInfo.fileId, path=savedPath.as_posix())

        return relativeFilePath

    def _getFileInfosForInitTime(self, initTime: dt.datetime) -> list[MetOfficeFileInfo]:
        """Get a list of FileInfo objects according to the input initTime."""
        # Fetch info for all files available on the input date
        response: requests.Response = requests.request(
            method="GET",
            url=self.baseurl,
            headers=self.__headers,
            params=self.querystring
        )
        if not response.ok:
            raise AssertionError(f"response did not return with an ok status: {response.content}")

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


def _getParameterNameFromFileName(fileName: str) -> str:
    """Gets the parameter name from the input file name.

    MetOffice single parameter files are in the format:
    <level>_<parameter>_<init_time>.grib

    so the parameter is the second element in the file name when split by underscores.
    """
    return fileName.split("_")[1]


def _isWantedFile(fileInfo: MetOfficeFileInfo, desiredInitTime: dt.datetime) -> bool:
    """Checks if the input FileInfo corresponds to a wanted GRIB file."""
    # False if item has an init_time not equal to desired init time
    if fileInfo.initTime().replace(tzinfo=dt.timezone.utc) != desiredInitTime.replace(tzinfo=dt.timezone.utc):
        return False
    # False if item is one of the ones ending in +HH
    if "+" in fileInfo.fileName():
        return False

    log.debug("Found wanted file from MetOffice", file=fileInfo.fileName())

    return True
