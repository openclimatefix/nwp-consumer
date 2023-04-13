"""Contains both ports and domain models for the nwp_consumer package."""

import datetime as dt
import io

import pathlib
import xarray as xr
from io import BytesIO

from enum import Enum

import abc


# ------- Interfaces ------- #
# Represent ports in the hexagonal architecture pattern

class FetcherInterface(abc.ABC):
    """Generic interface for fetching and converting API data, used for dependency injection."""

    @abc.abstractmethod
    def getDatasetForInitTime(self, initTime: dt.datetime) -> xr.Dataset:
        """Download a dataset for the given init_time."""
        pass

    @abc.abstractmethod
    def loadSingleParameterGRIBAsOCFDataArray(self, path: pathlib.Path, initTime: dt.datetime) -> xr.DataArray:
        """Combine many single-parameter GRIB files as a single xarray dataset."""
        pass


# TODO: The internal zarr/grib logic in the implementations of these abstract methods may be
# TODO: better suited to exposure as top level StorageInterface functions, but I'm not sure yet.
class StorageInterface(abc.ABC):
    """Generic interface for storing data, used for dependency injection."""

    @abc.abstractmethod
    def exists(self, filepath: pathlib.Path) -> bool:
        """Check if the given path exists.

        This should check for files with the .grib extension in the directory given by the
        RAW_GRIB_DIR_PATH environment variable, and files with the .zarr extension in the
        directory given by the ZARR_DIR_PATH environment variable."""
        pass

    @abc.abstractmethod
    def open(self, path: pathlib.Path) -> io.BufferedWriter:
        """Open a file, returning a file-like object.

        This should open files with the .grib extension in the directory given by the
        RAW_GRIB_DIR_PATH environment variable, and files with the .zarr extension in
        the directory given by the ZARR_DIR_PATH environment variable."""
        pass

    @abc.abstractmethod
    def saveDataset(self, dataset: xr.Dataset, filepath: pathlib.Path) -> None:
        """Store the given dataset as a Zarr file.

        This should save the zarr file in the directory given by the ZARR_DIR_PATH environment variable."""
        pass

    @abc.abstractmethod
    def appendDataset(self, dataset: xr.Dataset, filepath: pathlib.Path) -> None:
        """Append the given dataset to the existing dataset at the given path.

        This should append to a file in the directory given by the ZARR_DIR_PATH environment variable."""
        pass


# ------- Domain models ------- #

class OCFShortName(str, Enum):
    """Short names for the OCF parameters."""
    LowCloudCover = "lcc"
    MediumCloudCover = "mcc"
    HighCloudCover = "hcc"
    VisibilityAGL = "vis"
    RelativeHumidityAGL = "r"
    RainPrecipitationRate = "prate"
    SnowDepthWaterEquivalent = "sde"
    DownwardShortWaveRadiationFlux = "dswrf"
    DownwardLongWaveRadiationFlux = "dlwrf"
    TemperatureAGL = "t"
    WindSpeedSurfaceAdjustedAGL = "si10"
    WindDirectionFromWhichBlowingSurfaceAdjustedAGL = "wdir10"
