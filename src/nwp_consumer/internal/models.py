"""Contains both ports and domain models for the nwp_consumer package."""

import abc
import datetime as dt
import pathlib
from enum import Enum

import typing
import xarray as xr

# ------- Global constants ------- #

# The folder pattern format string for the raw data's init time
IT_FOLDER_FMTSTR = "%Y/%m/%d/%H%M"


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
        """Return the file name."""
        pass

    @abc.abstractmethod
    def initTime(self) -> dt.datetime:
        """Return the init time of the file."""
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
    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[FileInfoModel]:
        """List the relative path of all files available from source for the given init_time.

        :param it: Init Time to list files for
        """
        pass

    @abc.abstractmethod
    def fetchRawFileBytes(self, *, fi: FileInfoModel) -> tuple[FileInfoModel, bytes]:
        """Fetch the bytes of a single raw file from source given its relative path.

        :param fi: File Info object describing the file to fetch
        """
        pass

    @abc.abstractmethod
    def convertRawFileToDataset(self, *, b: bytes) -> xr.Dataset:
        """Create an xarray dataset from the given RAW file bytedata.

        :param b: Bytes of raw file
        """
        pass


class StorageInterface(abc.ABC):
    """Generic interface for storing data, used for dependency injection."""

    @abc.abstractmethod
    def rawFileExistsForInitTime(self, *, name: str, it: dt.datetime) -> bool:
        """Check if a file exists in the raw directory.

        :param name: Name of the file to check
        :param it: Init Time of the model data within the file
        """
        pass

    @abc.abstractmethod
    def writeBytesToRawFile(self, *, name: str, it: dt.datetime, b: bytes) -> pathlib.Path:
        """Write the given bytes to the raw directory.

        :param name: Name of the file to write
        :param it: Init Time of the model data within the file
        :param b: Bytes to write
        """
        pass

    @abc.abstractmethod
    def listInitTimesInRawDir(self) -> list[dt.datetime]:
        """List all initTime folders in the raw directory."""
        pass

    @abc.abstractmethod
    def zarrExistsForInitTime(self, *, name: str, it: dt.datetime) -> bool:
        """Check if a file exists in the zarr directory.

        :param name: Name of the file to check
        :param it: Init Time of the model data within the file
        """
        pass

    @abc.abstractmethod
    def readRawFilesForInitTime(self, *, it: dt.datetime) -> tuple[dt.datetime, list[bytes]]:
        """Read bytes for all files for the given initTime.

        :param it: Init Time to read files for
        """
        pass

    @abc.abstractmethod
    def writeDatasetAsZarr(self, *, name: str, it: dt.datetime, ds: xr.Dataset) -> pathlib.Path:
        """Write the given dataset to the zarr directory.

        :param name: Name of the file to write
        :param it: Init Time of the model data within the Dataset
        :param ds: Dataset to write
        """
        pass

    @abc.abstractmethod
    def deleteZarrForInitTime(self, *, name: str, it: dt.datetime) -> None:
        """Delete the zarr file for the given initTime.

        :param name: Name of the file to delete
        :param it: Init Time of the model data within the file
        """
        pass
