"""Contains both ports and domain models for the nwp_consumer package."""

import abc
import datetime as dt
import pathlib
from enum import Enum

import xarray as xr


# ------- Domain models ------- #


class OCFParameter(str, Enum):
    """Short names for the OCF parameters."""

    LowCloudCover = "lcc"
    MediumCloudCover = "mcc"
    HighCloudCover = "hcc"
    TotalCloudCover = "clt"
    VisibilityAGL = "vis"
    RelativeHumidityAGL = "r"
    RainPrecipitationRate = "prate"
    SnowDepthWaterEquivalent = "sde"
    DownwardShortWaveRadiationFlux = "dswrf"
    DownwardLongWaveRadiationFlux = "dlwrf"
    TemperatureAGL = "t"
    WindSpeedSurfaceAdjustedAGL = "si10"
    WindDirectionFromWhichBlowingSurfaceAdjustedAGL = "wdir10"
    WindUComponentAGL = "u10"
    WindVComponentAGL = "v10"
    WindUComponent100m = "u100"
    WindVComponent100m = "v100"
    WindUComponent200m = "u200"
    WindVComponent200m = "v200"
    DirectSolarRadiation = "sr"
    DownwardUVRadiationAtSurface = "duvrs"


class FileInfoModel(abc.ABC):
    """Information about a raw file.

    FileInfoModel assumes the following properties exist for all
    raw NWP files that may be encountered in a provider's archive:

    1. The file has a name
    2. The file has a path
    3. The file corresponds to a single forecast init time
    4. The file corresponds to one or more time steps
    5. The file corresponds to one or more variables

    These assumptions are reflected in the abstract methods of this class.
    """

    @abc.abstractmethod
    def filename(self) -> str:
        """Return the file name including extension."""
        pass

    @abc.abstractmethod
    def filepath(self) -> str:
        """Return the remote file path, not including protocols and TLDs."""
        pass

    @abc.abstractmethod
    def it(self) -> dt.datetime:
        """Return the init time of the file."""
        pass

    @abc.abstractmethod
    def steps(self) -> list[int]:
        """Return the time steps of the file."""
        pass

    @abc.abstractmethod
    def variables(self) -> list[str]:
        """Return the variables of the file."""
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
    def downloadToCache(self, *, fi: FileInfoModel) -> pathlib.Path:
        """Fetch the bytes of a single raw file from source and save to a cache file.

        :param fi: File Info object describing the file to fetch
        :return: Path to the local cache file, or pathlib.Path() if the file was not fetched
        """
        pass

    @abc.abstractmethod
    def mapCachedRaw(self, *, p: pathlib.Path) -> xr.Dataset:
        """Create an xarray dataset from the given RAW data in a cache file.

        :param p: Path to cached file holding raw data
        :return: Dataset created from the raw data
        """
        pass

    @abc.abstractmethod
    def getInitHours(self) -> list[int]:
        """Get the forecast init hours available from the source.

        :return: List of forecast init hours
        """
        pass

    @abc.abstractmethod
    def parameterConformMap(self) -> dict[str, OCFParameter]:
        """The mapping from the source's parameter names to the OCF short names.

        :return: Dictionary of parameter mappings
        """
        pass

    @abc.abstractmethod
    def datasetName(self) -> str:
        """Return the name of the dataset.

        :return: Name of the dataset
        """
        pass


class StorageInterface(abc.ABC):
    """Generic interface for storing data, used for dependency injection."""

    @abc.abstractmethod
    def exists(self, *, dst: pathlib.Path) -> bool:
        """Check if the given path exists.

        :param dst: Path to check
        :return: True if the path exists, False otherwise
        """
        pass

    @abc.abstractmethod
    def store(self, *, src: pathlib.Path, dst: pathlib.Path) -> pathlib.Path:
        """Move a file to the store.

        :param src: Path to file to store
        :param dst: Desired path in store
        :return: Location in raw store
        """
        pass

    @abc.abstractmethod
    def listInitTimes(self, *, prefix: pathlib.Path) -> list[dt.datetime]:
        """List all initTime folders in the given prefix.

        :param prefix: Path to prefix to list initTimes for
        :return: List of initTimes
        """
        pass

    @abc.abstractmethod
    def copyITFolderToCache(self, *, prefix: pathlib.Path, it: dt.datetime) \
            -> list[pathlib.Path]:
        """Copy all files in given folder to cache.

        :param prefix: Path of folder in which to find initTimes
        :param it: InitTime to copy files for
        :return: List of paths to cached files
        """
        pass

    @abc.abstractmethod
    def delete(self, *, p: pathlib.Path) -> None:
        """Delete the given path.

        :param p: Path to delete
        """
        pass

    @abc.abstractmethod
    def name(self) -> str:
        """Return the name of the storage provider.

        :return: Name of the storage provider
        """
        pass
