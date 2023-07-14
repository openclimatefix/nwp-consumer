"""Client adapting CEDA API to internal Fetcher port."""

import datetime as dt
import pathlib
import typing
import urllib.parse
import urllib.request

import cfgrib
import numpy as np
import requests
import structlog
import xarray as xr
from typeid import TypeID

from nwp_consumer import internal

from ._models import CEDAFileInfo, CEDAResponse

log = structlog.getLogger()

# Defines parameters in CEDA that are not available from MetOffice
PARAMETER_IGNORE_LIST: typing.Sequence[str] = (
    "unknown", "h", "hcct", "cdcb", "dpt", "prmsl",
)

COORDINATE_IGNORE_LIST: typing.Sequence[str] = (
    "height", "pressure", "meanSea", "level", "atmosphere", "cloudBase",
    "heightAboveGround", "heightAboveGroundLayer", "valid_time", "surface",
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

    def downloadToTemp(self, *, fi: internal.FileInfoModel) \
            -> tuple[internal.FileInfoModel, pathlib.Path]:

        anonUrl: str = f"{self.dataUrl}/{fi.initTime():%Y/%m/%d}/{fi.fname()}"
        log.debug(f"Requesting download of {fi.fname()}", path=anonUrl)
        url: str = f'{self.__ftpBase}/{anonUrl}'
        try:
            response = urllib.request.urlopen(url=url)
        except Exception as e:
            raise ConnectionError(
                f"Error calling url {url} for {fi.fname()}: {e}"
            ) from e

        # Stream the filedata into a temporary file
        tfp: pathlib.Path = pathlib.Path(f"/tmp/{str(TypeID(prefix='nwpc'))}")
        with tfp.open("wb") as f:
            for chunk in iter(lambda: response.read(16 * 1024), b''):
                f.write(chunk)

        log.debug(f"Fetched all data from: {fi.fname()}", path=anonUrl)

        return fi, tfp

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:

        # Fetch info for all files available on the input date
        # * CEDA has a HTTPS JSON API for this purpose
        response: requests.Response = requests.request(
            method="GET",
            url=f"{self.httpsBase}/{self.dataUrl}/{it:%Y/%m/%d}?json"
        )

        if response.status_code == 404:
            # No data available for this init time. Fail soft
            log.warn(
                event=f"No data available for init time {it:%Y/%m/%d %H:%M}",
                url=response.url
            )
            return []
        if not response.ok:
            # Something else has gone wrong. Fail hard
            raise ConnectionError(
                f"Error calling url {response.url}: {response.status_code}"
            ) from None

        # Map the response to a CEDAResponse object to ensure it looks as expected
        try:
            responseObj: CEDAResponse = CEDAResponse.Schema().load(response.json())
        except Exception as e:
            raise TypeError(
                f"Error marshalling json to CedaResponse object: {e}, response: {response.json()}"
            ) from e

        # Filter the files for the desired init time
        wantedFiles: list[CEDAFileInfo] = [
            fileInfo for fileInfo in responseObj.items
            if _isWantedFile(
                fi=fileInfo,
                dit=it
            )
        ]

        return wantedFiles

    def mapTemp(self, *, p: pathlib.Path) -> xr.Dataset:

        # Load the wholesale file as a list of datasets
        # * cfgrib loads multiple hypercubes for a single multi-parameter grib file
        try:
            datasets: list[xr.Dataset] = cfgrib.open_datasets(
                path=p.as_posix(),
                backend_kwargs={"indexpath": ""}
            )
        except Exception as e:
            raise Exception(f"Error loading wholesale file as dataset: {e}") from e

        for i, ds in enumerate(datasets):
            # Rename the parameters to the OCF names
            # * Only do so if they exist in the dataset
            for oldParamName, newParamName in PARAMETER_RENAME_MAP.items():
                if oldParamName in ds:
                    ds = ds.rename({oldParamName: newParamName})

            # Ensure the temperature is defined at 1 meter above ground level
            # * In the early NWPs (definitely in the 2016-03-22 NWPs):
            #   - `heightAboveGround` only has one entry ("1" meter above ground)
            #   - `heightAboveGround` isn't set as a dimension for `t`.
            # * In later NWPs, 'heightAboveGround' has 2 values (0, 1) and is a dimension for `t`.
            if "t" in ds and "heightAboveGround" in ds["t"].dims:
                ds = ds.sel(heightAboveGround=1)

            # Snow depth is in `m` from CEDA, but OCF expects `kg m-2`.
            # * A scaling factor of 1000 converts between the two.
            # * See "Snow Depth" entry in https://gridded-data-ui.cda.api.metoffice.gov.uk/glossary
            if "sde" in ds:
                ds = ds.assign(sde=ds["sde"] * 1000)

            # Delete unnecessary data variables
            for var_name in PARAMETER_IGNORE_LIST:
                if var_name in ds:
                    del ds[var_name]

            # Delete unwanted coordinates
            ds = ds.drop_vars(
                names=COORDINATE_IGNORE_LIST,
                errors="ignore"
            )

            # Put the modified dataset back in the list
            datasets[i] = ds

        # Merge the datasets back into one
        wholesaleDataset = xr.merge(
            objects=datasets,
            compat='override',
            combine_attrs='drop_conflicts'
        )

        # Add in x and y coordinates
        wholesaleDataset = _reshapeTo2DGrid(
            ds=wholesaleDataset
        )

        # Create a chunked Dask Dataset from the input multi-variate Dataset.
        # *  Converts the input multivariate DataSet (with different DataArrays for
        #     each NWP variable) to a single DataArray with a `variable` dimension.
        # * This allows each Zarr chunk to hold multiple variables (useful for loading
        #     many/all variables at once from disk).
        # * The chunking is done in such a way that each chunk is a single time step
        #     for a single variable.
        # * Transpose the Dataset so that the dimensions are correctly ordered
        wholesaleDataset = wholesaleDataset \
            .rename({"time": "init_time"}) \
            .expand_dims("init_time") \
            .to_array(dim="variable", name="UKV") \
            .to_dataset() \
            .transpose("init_time", "step", "variable", "y", "x") \
            .sortby("step") \
            .sortby("variable") \
            .chunk({
                "init_time": 1,
                "step": 1,
                "variable": -1,
                "y": len(wholesaleDataset.y) // 2,
                "x": len(wholesaleDataset.x) // 2,
            })

        del datasets

        return wholesaleDataset


def _isWantedFile(*, fi: CEDAFileInfo, dit: dt.datetime) -> bool:
    """Check if the input FileInfo corresponds to a wanted GRIB file.

    :param fi: The File Info object describing the file to check
    :param dit: The desired init time
    """
    if fi.initTime().date() != dit.date() or \
            fi.initTime().time() != dit.time():
        return False
    # False if item doesn't correspond to Wholesale1 or Wholesale2 files
    if not any([setname in fi.name for setname in ["Wholesale1.grib", "Wholesale2.grib"]]):
        return False

    return True


def _reshapeTo2DGrid(*, ds: xr.Dataset) -> xr.Dataset:
    """Convert 1D into 2D array.

    In the grib files, the pixel values are in a flat 1D array (indexed by the `values` dimension).
    The ordering of the pixels in the grib are left to right, bottom to top.

    This function replaces the `values` dimension with an `x` and `y` dimension,
    and, for each step, reshapes the images to be 2D.

    :param ds: The dataset to reshape
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

    if ds.dims['values'] != len(northing) * len(easting):
        raise ValueError(
            f"Dataset has {ds.dims['values']} values, "
            f"but expected {len(northing) * len(easting)}"
        )

    # Create new coordinates,
    # which give the `x` and `y` position for each position in the `values` dimension:
    ds = ds.assign_coords(
        {
            "x": ("values", np.tile(easting, reps=len(northing))),
            "y": ("values", np.repeat(northing, repeats=len(easting))),
        }
    )

    # Now set `values` to be a MultiIndex, indexed by `y` and `x`:
    ds = ds.set_index(values=("y", "x"))

    # Now unstack. This gets rid of the `values` dimension and indexes
    # the data variables using `y` and `x`.
    return ds.unstack("values")
