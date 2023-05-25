"""Contains both ports and domain models for the nwp_consumer package."""

import abc
import dataclasses
import datetime as dt
import pathlib
from enum import Enum

import numpy.typing as npt

import xarray as xr


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


class FileInfoModel(abc.ABC):
    """Information about a remote file."""

    @abc.abstractmethod
    def fname(self) -> str:
        """Returns the file name."""
        pass

    @abc.abstractmethod
    def initTime(self) -> dt.datetime:
        """Returns the init time of the file."""
        pass


# ------- Interfaces ------- #
# Represent ports in the hexagonal architecture pattern

class FetcherInterface(abc.ABC):
    """Generic interface for fetching and converting NWP data from an API.

     Used for dependency injection. NWP data from any source shares common properties:
        - It is presented in one or many files for a given init_time
        - These files can be read as raw bytes

    The following functions define generic transforms based around these principals.
     """

    @abc.abstractmethod
    def listRawFilesForInitTime(self, initTime: dt.datetime) -> list[FileInfoModel]:
        """List the relative path of all files available from source for the given init_time."""
        pass

    @abc.abstractmethod
    def fetchRawFileBytes(self, fileInfo: FileInfoModel) -> tuple[FileInfoModel, bytes]:
        """Fetch the bytes of a single raw file from source given its relative path."""
        pass

    @abc.abstractmethod
    def loadRawInitTimeDataAsOCFDataset(self, fileBytesList: list[bytes], initTime: dt.datetime) -> xr.Dataset:
        """Create an xarray dataset from the given RAW file bytedata."""
        pass


class StorageInterface(abc.ABC):
    """Generic interface for storing data, used for dependency injection."""

    @abc.abstractmethod
    def existsInRawDir(self, fileName: str, initTime: dt.datetime) -> bool:
        """Check if a file exists in the raw directory."""
        pass

    @abc.abstractmethod
    def existsInZarrDir(self, relativePath: pathlib.Path) -> bool:
        """Check if a file exists in the zarr directory."""
        pass

    @abc.abstractmethod
    def listFilesInRawDir(self) -> list[pathlib.Path]:
        """List all files in the raw directory."""
        pass

    @abc.abstractmethod
    def writeBytesToRawDir(self, fileName: str, initTime: dt.datetime, data: bytes) -> pathlib.Path:
        """Write the given bytes to the raw directory."""
        pass

    @abc.abstractmethod
    def readBytesFromRawDir(self, fileName: str, initTIme: dt.datetime) -> bytes:
        """Read the given bytes from the raw directory."""
        pass

    @abc.abstractmethod
    def removeFromRawDir(self, fileName: str, initTime: dt.datetime) -> None:
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
