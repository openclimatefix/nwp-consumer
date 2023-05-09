"""Contains both ports and domain models for the nwp_consumer package."""

import abc
import datetime as dt
import pathlib
from enum import Enum

import xarray as xr

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


class StorageInterface(abc.ABC):
    """Generic interface for storing data, used for dependency injection."""

    @abc.abstractmethod
    def existsInRawDir(self, relativePath: pathlib.Path) -> bool:
        """Check if a file exists in the raw directory."""
        pass

    @abc.abstractmethod
    def existsInZarrDir(self, relativePath: pathlib.Path) -> bool:
        """Check if a file exists in the zarr directory."""
        pass

    @abc.abstractmethod
    def writeBytesToRawDir(self, relativePath: pathlib.Path, data: bytes) -> pathlib.Path:
        """Write the given bytes to the raw directory."""
        pass

    @abc.abstractmethod
    def readBytesFromRawDir(self, relativePath: pathlib.Path) -> bytes:
        """Read the given bytes from the raw directory."""
        pass

    @abc.abstractmethod
    def removeFromRawDir(self, relativePath: pathlib.Path) -> None:
        """Remove a file from the raw dir."""
        pass

    @abc.abstractmethod
    def saveDataset(self, dataset: xr.Dataset, relativePath: pathlib.Path) -> None:
        """Store the given dataset as a Zarr file."""
        pass

    @abc.abstractmethod
    def appendDataset(self, dataset: xr.Dataset, relativePath: pathlib.Path) -> None:
        """Append the given dataset to the existing dataset at the given path."""
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
