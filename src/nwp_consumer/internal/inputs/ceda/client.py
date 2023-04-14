import datetime as dt
import pathlib
import urllib.parse
import urllib.request
import requests
import typing
import xarray as xr
import iris_grib
import iris.cube
from multiprocessing import Pool
import numpy as np

import structlog
from eccodes import eccodes

from src.nwp_consumer import internal

from ._models import CEDAResponse, CEDAFileInfo
from src.nwp_consumer.internal.inputs import common

log = structlog.stdlib.get_logger()

# Defines parameters in CEDA that are not available from MetOffice
PARAMETER_IGNORE_LIST: typing.Sequence[str] = (
    "unknown", "h", "hcct", "cdcb", "dpt", "prmsl",
)

# Defines the mapping from CEDA parameter names to OCF parameter names
PARAMETER_RENAME_MAP: dict[str, str] = {
    "10wdir": internal.OCFShortName.WindDirectionFromWhichBlowingSurfaceAdjustedAGL,
    "10si": internal.OCFShortName.WindSpeedSurfaceAdjustedAGL,
    "prate": internal.OCFShortName.RainPrecipitationRate,
    "r": internal.OCFShortName.RelativeHumidityAGL,
    "t": internal.OCFShortName.TemperatureAGL,
    "vis": internal.OCFShortName.VisibilityAGL,
    "dswrf": internal.OCFShortName.DownwardShortWaveRadiationFlux,
    "dlwrf": internal.OCFShortName.DownwardLongWaveRadiationFlux,
    "hcc": internal.OCFShortName.HighCloudCover,
    "mcc": internal.OCFShortName.MediumCloudCover,
    "lcc": internal.OCFShortName.LowCloudCover,
    "sde": internal.OCFShortName.SnowDepthWaterEquivalent,
}


class CEDAClient(internal.FetcherInterface):
    """Implements a client to fetch data from CEDA."""

    # CEDA FTP Username
    __username: str

    # CEDA FTP Password
    __password: str

    # API urls for CEDA data
    dataUrl: str = "badc/ukmo-nwp/data/ukv-grib"
    httpsBase: str = "https://data.ceda.ac.uk"
    __ftpBase: str

    # Storage client
    storageClient: internal.StorageInterface
    # TODO: Would prefer it if the fetcher client was not coupled to the storage client

    def __init__(self, ftpUsername: str, ftpPassword: str, storer: internal.StorageInterface):
        self.__username: str = urllib.parse.quote(ftpUsername)
        self.__password: str = urllib.parse.quote(ftpPassword)
        self.__ftpBase: str = f'ftp://{self.__username}:{self.__password}@ftp.ceda.ac.uk'
        self.storageClient: internal.StorageInterface = storer

    def getDatasetForInitTime(self, initTime: dt.datetime) -> xr.Dataset:
        """Download a dataset for the given init_time."""

        # Fetch all the raw file infos for the given init time
        fileInfosForInitTime: list[CEDAFileInfo] = self._getFileInfosForInitTime(initTime=initTime)

        if fileInfosForInitTime is None or len(fileInfosForInitTime) == 0:
            raise Exception(f"No files found for initTime {initTime}")

        # Download the wholesale files given by the file infos
        with Pool(4) as p:
            wholesaleFilePaths: list[pathlib.Path] = p.map(self._downloadRawGRIBFile, fileInfosForInitTime)

        # For each wholesale CEDA file, split the GRIB into individual per-parameter GRIB files
        parameterPathsForInitTime: list[pathlib.Path] = []
        for rawPath in wholesaleFilePaths:
            parameterPaths: list[pathlib.Path] = self._splitRawGribPerParameter(gribFilePath=rawPath)
            parameterPathsForInitTime.extend(parameterPaths)

        # Delete the wholesale files
        for path in wholesaleFilePaths:
            path.unlink()

        # Merge all the single-parameter GRIB files for the initTime into one dataset
        allParameterDataset: xr.Dataset = common.combineSingleParamGRIBsAsOCFDataset(
            client=self,
            parameterFilePaths=parameterPathsForInitTime,
            initTime=initTime
        )

        # Delete the single parameter files
        for path in parameterPathsForInitTime:
            path.unlink()

        return allParameterDataset

    def loadSingleParameterGRIBAsOCFDataArray(self, path: pathlib.Path, initTime: dt.datetime) -> xr.DataArray:
        """Loads a single-parameter GRIB file as an OCF-compliant DataArray."""

        # Load the GRIB file as a cube
        cube: iris.cube.Cube = iris.cube.CubeList(iris_grib.load_cubes(path.as_posix())).merge_cube()
        # Convert the cube to a DataArray
        parameterDataArray: xr.DataArray = xr.DataArray.from_iris(cube)
        # Make the DataArray OCF-compliant
        parameterDataArray = parameterDataArray \
            .rename(PARAMETER_RENAME_MAP[path.stem]) \
            .assign_coords({"init_time": np.datetime64(initTime.replace(tzinfo=None))}) \
            .rename({"time": "step_time"}) \
            .rename({"projection_x_coordinate": "x", "projection_y_coordinate": "y"}) \
            .drop_vars(["height", "pressure"], errors="ignore") \
            .chunk("auto")

        # Snow depth is in `m` from CEDA, but OCF expects `kg m-2`. A scaling factor of 1000 converts between the two.
        # See "Snow Depth" entry in https://gridded-data-ui.cda.api.metoffice.gov.uk/glossary
        if path.stem == "sde":
            parameterDataArray = parameterDataArray * 1000

        return parameterDataArray

    def _downloadRawGRIBFile(self, fileInfo: CEDAFileInfo) -> pathlib.Path:
        """Download a GRIB file corresponding to the input FileInfo object."""

        # TODO: inject path to downloaded files

        fileName: pathlib.Path = pathlib.Path(fileInfo.name)

        if self.storageClient.exists(filepath=fileName):
            log.debug(f"File already exists: {str(fileName)}", path=fileName.as_posix())

        else:
            ftpPath = f'{self.dataUrl}/{fileInfo.initTime().strftime("%Y/%m/%d")}/{fileInfo.name}'
            log.debug(f"Requesting download of {fileInfo.name}", item=fileInfo.name, path=ftpPath)
            url: str = f'{self.__ftpBase}/{ftpPath}'
            try:
                # Fetch the file from CEDA
                response = urllib.request.urlopen(url=url)
                if not response.status == 200:
                    raise ConnectionError(f"Error response code {response.status} for url {url}: {response.read()}")
                with self.storageClient.open(path=fileName) as f:
                    f.write(response.read())
            except Exception as e:
                raise ConnectionError(f"Error calling url {url} for {fileInfo.name}: {e}")

            log.debug(f"Downloaded item: {fileInfo.name}", item=fileInfo.name, path=fileName)

        return fileName

    def _getFileInfosForInitTime(self, initTime: dt.datetime) -> list[CEDAFileInfo]:
        """Get a list of FileInfo objects according to the input initTime."""

        initDate = initTime.date()

        # Fetch info for all files available on the input date
        response: requests.Response = requests.request(
            method="GET",
            url=f"{self.httpsBase}/{self.dataUrl}/{initDate.strftime('%Y')}/"
                f"{initDate.strftime('%m')}/{initDate.strftime('%d')}"
                f"?json"
        )
        if not response.ok:
            raise AssertionError(f"Non-okay status for {response.url}: {response.status_code}")

        # Map the response to a CedaResponse object
        try:
            responseObj: CEDAResponse = CEDAResponse.Schema().load(response.json())
        except Exception as e:
            raise TypeError(
                f"Error marshalling json to CedaResponse object: {e}, response: {response.json()}"
            )

        # Filter the file infos for the desired init time
        wantedFileInfos: list[CEDAFileInfo] = [
            fo for fo in responseObj.items if _isWantedFile(fileInfo=fo, desiredInitTime=initTime)
        ]

        return wantedFileInfos

    def _splitRawGribPerParameter(self, gribFilePath: pathlib.Path) -> list[pathlib.Path]:
        """Splits a multi-parameter GRIB file into several single-parameter GRIB files."""

        # Create a GRIB index based on the 'shortName' and 'step' keys
        indexID = eccodes.codes_index_new_from_file(
            filename=gribFilePath.as_posix(),
            keys=["shortName", "step"]
        )
        parameters = list(eccodes.codes_index_get(indexid=indexID, key="shortName"))
        steps = list(eccodes.codes_index_get(indexid=indexID, key="step"))
        log.debug(
            f"Wholesale file contains {len(parameters)} parameter(s) and {len(steps)} step(s)",
            file=gribFilePath.as_posix(), parameters=parameters
        )

        fileDirectory: pathlib.Path = gribFilePath.with_suffix("")

        parameterFilePaths: list[pathlib.Path] = []

        for parameter in parameters:
            # Ignore parameters that are not currently available form MetOffice or are unknown
            if parameter in PARAMETER_IGNORE_LIST:
                continue

            # Create a new grib file for the current parameter in the wholesale file
            filePath: pathlib.Path = (fileDirectory / parameter).with_suffix(".grib")
            parameterFile = self.storageClient.open(path=filePath)
            multiGribID = eccodes.codes_grib_multi_new()
            log.debug(f"Creating individual file for parameter: {parameter}", path=filePath.as_posix(),
                      steps=len(steps))

            # Select the message subset for the current parameter's shortname on the wholesale GRIB's index
            eccodes.codes_index_select(indexID, "shortName", parameter)

            # Add each step for this parameter to the single-parameter file
            for step in steps:
                # Select the message subset for the current step on the index
                eccodes.codes_index_select(indexID, "step", step)
                # Create an in-memory GRIB file from the selected subset
                gribID = eccodes.codes_new_from_index(indexID)

                # Modify the incorrectly-valued 'sourceOfGridDefinition' and 'scanningMode' keys
                # scanningMode should specify negative y, positive x: see
                # https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/grib2_table3-4.shtml
                eccodes.codes_set_key_vals(gribID, f"sourceOfGridDefinition=0,scanningMode={0b00000000}")
                assert (eccodes.codes_get(gribID, 'sourceOfGridDefinition', int) == 0)
                assert (eccodes.codes_get(gribID, 'scanningMode', int) == 0)
                # Add in the missing 'scaleFactor' key, based on the value of the axis scale factors
                majorAxisScaleFactor: int = eccodes.codes_get(gribID, 'scaleFactorOfEarthMajorAxis', int)
                minorAxisScaleFactor: int = eccodes.codes_get(gribID, 'scaleFactorOfEarthMajorAxis', int)
                assert (majorAxisScaleFactor == minorAxisScaleFactor)
                eccodes.codes_set_key_vals(gribID, f"scaleFactorAtReferencePoint={majorAxisScaleFactor}")
                assert (eccodes.codes_get(gribID, 'scaleFactorAtReferencePoint', int) == minorAxisScaleFactor)

                # Append the in-memory GRIB file to the single-parameter GRIB file
                eccodes.codes_grib_multi_append(ingribid=gribID, startsection=3, multigribid=multiGribID)
                # Release the in-memory GRIB file
                eccodes.codes_release(gribID)

            # Write the multiGrib file to the parameterFile buffer
            eccodes.codes_grib_multi_write(multiGribID, parameterFile)
            eccodes.codes_grib_multi_release(multiGribID)
            parameterFile.close()

            log.debug(f"Saved individual file for parameter: {parameter}", path=filePath.as_posix(), steps=len(steps))

            parameterFilePaths.append(filePath)

        return parameterFilePaths


def _isWantedFile(fileInfo: CEDAFileInfo, desiredInitTime: dt.datetime) -> bool:
    """Checks if the input FileInfo corresponds to a wanted GRIB file."""

    if fileInfo.initTime().date() != desiredInitTime.date() or fileInfo.initTime().time() != desiredInitTime.time():
        return False
    # False if item doesn't correspond to Wholesale1 or Wholesale2 files
    if not any([setname in fileInfo.name for setname in ["Wholesale1.grib", "Wholesale2.grib"]]):
        return False

    log.debug(f"Found wanted file from CEDA", file=fileInfo.name)

    return True
