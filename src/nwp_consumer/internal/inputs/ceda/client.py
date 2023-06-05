import datetime as dt
import tempfile
import typing
import urllib.parse
import urllib.request

import cfgrib
import numpy as np
import requests
import structlog
import xarray as xr

from nwp_consumer import internal

from ._models import CEDAFileInfo, CEDAResponse

log = structlog.stdlib.get_logger()

# Defines parameters in CEDA that are not available from MetOffice
PARAMETER_IGNORE_LIST: typing.Sequence[str] = (
    "unknown", "h", "hcct", "cdcb", "dpt", "prmsl",
)

COORDINATE_IGNORE_LIST: typing.Sequence[str] = (
    "height", "pressure", "meanSea", "level", "atmosphere", "cloudBase", "heightAboveGround", "heightAboveGroundLayer",
)

# Defines the mapping from CEDA parameter names to OCF parameter names
PARAMETER_RENAME_MAP: dict[str, str] = {
    "10wdir": internal.OCFShortName.WindDirectionFromWhichBlowingSurfaceAdjustedAGL.value,
    "10si": internal.OCFShortName.WindSpeedSurfaceAdjustedAGL.value,
    "prate": internal.OCFShortName.RainPrecipitationRate.value,
    "r": internal.OCFShortName.RelativeHumidityAGL.value,
    "t": internal.OCFShortName.TemperatureAGL.value,
    "vis": internal.OCFShortName.VisibilityAGL.value,
    "dswrf": internal.OCFShortName.DownwardShortWaveRadiationFlux.value,
    "dlwrf": internal.OCFShortName.DownwardLongWaveRadiationFlux.value,
    "hcc": internal.OCFShortName.HighCloudCover.value,
    "mcc": internal.OCFShortName.MediumCloudCover.value,
    "lcc": internal.OCFShortName.LowCloudCover.value,
    "sde": internal.OCFShortName.SnowDepthWaterEquivalent.value,
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

    def __init__(self, ftpUsername: str, ftpPassword: str):
        self.__username: str = urllib.parse.quote(ftpUsername)
        self.__password: str = urllib.parse.quote(ftpPassword)
        self.__ftpBase: str = f'ftp://{self.__username}:{self.__password}@ftp.ceda.ac.uk'

    def fetchRawFileBytes(self, fileInfo: internal.FileInfoModel) -> tuple[internal.FileInfoModel, bytes]:
        """Download a GRIB file corresponding to the input relative path."""

        anonUrl: str = f"{self.dataUrl}/{fileInfo.initTime():%Y/%m/%d}/{fileInfo.fname()}"
        log.debug(f"Requesting download of {fileInfo.fname()}", path=anonUrl)
        url: str = f'{self.__ftpBase}/{anonUrl}'
        try:
            response = urllib.request.urlopen(url=url)
        except Exception as e:
            raise ConnectionError(f"Error calling url {url} for {fileInfo.fname()}: {e}")

        filedata = response.read()
        log.debug(f"Fetched all data from: {fileInfo.fname()}", path=anonUrl)

        return fileInfo, filedata

    def listRawFilesForInitTime(self, initTime: dt.datetime) -> list[internal.FileInfoModel]:
        """Get a list of FileInfo objects according to the input initTime."""

        # Fetch info for all files available on the input date
        # * CEDA has a HTTPS JSON API for this purpose
        response: requests.Response = requests.request(
            method="GET",
            url=f"{self.httpsBase}/{self.dataUrl}/{initTime:%Y/%m/%d}?json"
        )
        if not response.ok:
            raise AssertionError(f"Non-okay status for {response.url}: {response.status_code}")

        # Map the response to a CEDAResponse object to ensure it looks as expected
        try:
            responseObj: CEDAResponse = CEDAResponse.Schema().load(response.json())
        except Exception as e:
            raise TypeError(
                f"Error marshalling json to CedaResponse object: {e}, response: {response.json()}"
            )

        # Filter the files for the desired init time
        wantedFiles: list[CEDAFileInfo] = [
            fileInfo for fileInfo in responseObj.items
            if _isWantedFile(fileInfo=fileInfo, desiredInitTime=initTime)
        ]

        return wantedFiles

    def loadRawInitTimeDataAsOCFDataset(self, fileBytesList: list[bytes]) -> xr.Dataset:
        """Create an xarray dataset from the given raw files."""
        # Load the wholesale files as OCF datasets
        wholesaleDatasets: list[xr.Dataset] = [
            _loadWholesaleFileAsDataset(data=bd) for bd in fileBytesList
        ]

        # Merge the wholesale datasets into one
        # TODO: Enable concatenation of datasets for different step sets
        wholesaleDataset: xr.Dataset = xr.merge(wholesaleDatasets, compat='identical', combine_attrs='drop_conflicts')

        # Load the new dataset into memory
        # * Enables deletion of the old datasets
        wholesaleDataset.load()
        del wholesaleDatasets

        # Add in x and y coordinates
        wholesaleDataset = _reshapeTo2DGrid(dataset=wholesaleDataset)

        # Add the init time as a coordinate
        wholesaleDataset = wholesaleDataset \
            .rename({"time": "init_time"}) \
            .expand_dims("init_time") \
            .chunk("auto") \
            .load()

        return wholesaleDataset


def _loadWholesaleFileAsDataset(data: bytes) -> xr.Dataset:
    """Loads a multi-parameter GRIB file as an OCF-compliant Dataset."""
    # Cfgrib is built upon eccodes which needs an in-memory file to read from
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".grib2") as tempParameterFile:
        # Copy the raw file to a local temp file
        tempParameterFile.write(data)
        tempParameterFile.seek(0)

        # Load the wholesale file as a dataset
        try:
            datasets: list[xr.Dataset] = cfgrib.open_datasets(
                tempParameterFile.name, backend_kwargs={"indexpath": ""})
        except Exception as e:
            raise Exception(f"Error loading wholesale file as dataset: {e}")

        for i in range(len(datasets)):
            ds = datasets[i]

            # Rename the parameters to the OCF names
            # * Only do so if they exist in the dataset
            for oldParamName, newParamName in PARAMETER_RENAME_MAP.items():
                if oldParamName in ds:
                    ds = ds.rename({oldParamName: newParamName})

            # We want the temperature at 1 meter above ground, not at 0 meters above ground.
            # * In the early NWPs (definitely in the 2016-03-22 NWPs), `heightAboveGround` only has
            # * 1 entry ("1" meter above ground) and `heightAboveGround` isn't set as a dimension for `t`.
            # * In later NWPs, 'heightAboveGround' has 2 values (0, 1) is a dimension for `t`.
            if "t" in ds and "heightAboveGround" in ds["t"].dims:
                ds = ds.sel(heightAboveGround=1)

            # Snow depth is in `m` from CEDA, but OCF expects `kg m-2`. A scaling factor of 1000 converts between
            # the two. See "Snow Depth" entry in https://gridded-data-ui.cda.api.metoffice.gov.uk/glossary
            if "sde" in ds:
                ds = ds.assign(sde=ds["sde"] * 1000)

            # Delete unnecessary data variables
            for var_name in PARAMETER_IGNORE_LIST:
                if var_name in ds:
                    del ds[var_name]

            # Delete unwanted coordinates
            ds = ds.drop_vars(COORDINATE_IGNORE_LIST, errors="ignore")

            # Replace the dataset in the list with the modified one
            # * Delete the modified dataset to free up memory
            datasets[i] = ds
            del ds

        wholesaleDataset = xr.merge(datasets, compat='override', combine_attrs='drop_conflicts')
        wholesaleDataset.load()
        del datasets

    return wholesaleDataset


def _isWantedFile(fileInfo: CEDAFileInfo, desiredInitTime: dt.datetime) -> bool:
    """Checks if the input FileInfo corresponds to a wanted GRIB file."""
    if fileInfo.initTime().date() != desiredInitTime.date() or fileInfo.initTime().time() != desiredInitTime.time():
        return False
    # False if item doesn't correspond to Wholesale1 or Wholesale2 files
    if not any([setname in fileInfo.name for setname in ["Wholesale1.grib", "Wholesale2.grib"]]):
        return False

    return True


def _reshapeTo2DGrid(dataset: xr.Dataset) -> xr.Dataset:
    """Convert 1D into 2D array.

    In the grib files, the pixel values are in a flat 1D array (indexed by the `values` dimension).
    The ordering of the pixels in the grib are left to right, bottom to top.

    This function replaces the `values` dimension with an `x` and `y` dimension,
    and, for each step, reshapes the images to be 2D.
    """
    # Adapted from https://stackoverflow.com/a/62667154 and
    # https://github.com/SciTools/iris-grib/issues/140#issuecomment-1398634288

    # Define geographical domain for UKV. Taken from page 4 of https://zenodo.org/record/7357056
    dx = dy = 2000
    maxY = 1223000
    minY = -185000
    minX = -239000
    maxX = 857000
    # * Note that the UKV NWPs y is top-to-bottom, hence step is negative.
    northing = np.arange(start=maxY, stop=minY, step=-dy, dtype=np.int32)
    easting = np.arange(start=minX, stop=maxX, step=dx, dtype=np.int32)

    if dataset.dims['values'] != len(northing) * len(easting):
        raise ValueError(
            f"Dataset has {dataset.dims['values']} values, but expected {len(northing) * len(easting)}"
        )

    # Create new coordinates,
    # which give the `x` and `y` position for each position in the `values` dimension:
    dataset = dataset.assign_coords(
        {
            "x": ("values", np.tile(easting, reps=len(northing))),
            "y": ("values", np.repeat(northing, repeats=len(easting))),
        }
    )

    # Now set `values` to be a MultiIndex, indexed by `y` and `x`:
    dataset = dataset.set_index(values=("y", "x"))

    # Now unstack. This gets rid of the `values` dimension and indexes
    # the data variables using `y` and `x`.
    return dataset.unstack("values")
