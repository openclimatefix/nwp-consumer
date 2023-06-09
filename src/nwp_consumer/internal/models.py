"""Contains both ports and domain models for the nwp_consumer package."""

import abc
import datetime as dt
import pathlib
from enum import Enum

import xarray as xr

# ------- Global constants ------- #

# The folder pattern format string for the raw data's init time
RAW_FOLDER_PATTERN_FMT_STRING = "%Y/%m/%d/%H%M"


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
        - There is an expected number of files per init_time which correspond to an equivalent
            number of variables and steps in the dataset

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
    def loadRawInitTimeDataAsOCFDataset(self, fileBytesList: list[bytes]) -> xr.Dataset:
        """Create an xarray dataset from the given RAW file bytedata."""
        pass


class StorageInterface(abc.ABC):
    """Generic interface for storing data, used for dependency injection."""

    @abc.abstractmethod
    def existsInRawDir(self, fileName: str, initTime: dt.datetime) -> bool:
        """Check if a file exists in the raw directory."""
        pass

    @abc.abstractmethod
    def writeBytesToRawDir(self, fileName: str, initTime: dt.datetime, data: bytes) -> pathlib.Path:
        """Write the given bytes to the raw directory."""
        pass

    @abc.abstractmethod
    def listInitTimesInRawDir(self) -> list[dt.datetime]:
        """List all initTime folders in the raw directory."""
        pass

    @abc.abstractmethod
    def existsInZarrDir(self, fileName: str, initTime: dt.datetime) -> bool:
        """Check if a file exists in the zarr directory."""
        pass

    @abc.abstractmethod
    def readBytesForInitTime(self, initTime: dt.datetime) -> tuple[dt.datetime, list[bytes]]:
        """Read bytes for all files for the given initTime."""
        pass

    @abc.abstractmethod
    def writeDatasetToZarrDir(self, fileName: str, initTime: dt.datetime, data: xr.Dataset) -> pathlib.Path:
        """Write the given dataset to the zarr directory."""
        pass


