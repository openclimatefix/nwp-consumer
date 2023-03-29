"""Contains both ports and domain models for the nwp_consumer package."""

import datetime as dt

import pathlib
import xarray as xr

from enum import Enum

import abc


class ClientInterface(abc.ABC):
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
    def saveFile(self, dataset: xr.Dataset) -> None:
        """Store the given dataset."""
        pass

    def loadFile(self, path: pathlib.Path) -> xr.Dataset:
        """Load the given dataset."""
        pass


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


