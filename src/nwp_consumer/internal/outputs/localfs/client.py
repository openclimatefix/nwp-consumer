import datetime as dt
import pathlib
import shutil

import numpy as np
import structlog
import xarray as xr
from ocf_blosc2 import Blosc2

from nwp_consumer import internal

log = structlog.getLogger()


class LocalFSClient(internal.StorageInterface):
    """Client for local filesystem."""

    # Location to save raw (Usually GRIB) files
    __rawDir: pathlib.Path

    # Location to save Zarr files
    __zarrDir: pathlib.Path

    def __init__(self, rawDir: str, zarrDir: str, createDirs: bool = False):
        """Create a new LocalFSClient."""
        rawPath: pathlib.Path = pathlib.Path(rawDir)
        zarrPath: pathlib.Path = pathlib.Path(zarrDir)

        if createDirs:
            # If the directories do not exist, create them
            rawPath.mkdir(parents=True, exist_ok=True)
            zarrPath.mkdir(parents=True, exist_ok=True)

        self.__rawDir = rawPath
        self.__zarrDir = zarrPath

    def rawFileExistsForInitTime(self, *, name: str, it: dt.datetime) -> bool:
        """Check if a file exists in the raw directory.

        :param name: The name of the file to check for
        :param it: The init time of the data within the file
        """
        path = pathlib.Path(
            f"{self.__rawDir}/{it.strftime(internal.IT_FOLDER_FMTSTR)}/{name}")
        return path.exists()

    def writeBytesToRawFile(self, name: str, it: dt.datetime, b: bytes) -> pathlib.Path:
        """Write the given bytes to the raw directory.

        :param name: The name of the file to write
        :param it: The init time of the data within the file
        :param b: The bytes to write
        """
        path = pathlib.Path(f"{self.__rawDir}/{it.strftime(internal.IT_FOLDER_FMTSTR)}/{name}")

        # Create the path to the file if the folders do not exist
        path.parent.mkdir(parents=True, exist_ok=True)

        path.write_bytes(b)
        return path

    def listInitTimesInRawDir(self) -> list[dt.datetime]:
        """List all initTimes in the raw directory."""
        # List all the YYYY/MM/DD/INITTIME folders in the raw directory
        dirs = [f.relative_to(self.__rawDir) for f in self.__rawDir.glob('*/*/*/*') if f.suffix == ""]

        initTimes = set()
        for dir in dirs:
            try:
                # Try to parse the dir as a datetime
                ddt: dt.datetime = dt.datetime.strptime(
                    dir.as_posix(),
                    internal.IT_FOLDER_FMTSTR
                ).replace(tzinfo=None)
                # Add the initTime to the set
                initTimes.add(ddt)
            except ValueError:
                log.debug(f"Invalid folder name found in raw directory: {dir}. Ignoring")

        sortedInitTimes = sorted(initTimes)
        log.debug(
            event=f"Found {len(initTimes)} init times in raw directory",
            earliest=sortedInitTimes[0],
            latest=sortedInitTimes[-1]
        )

        return sortedInitTimes

    def readRawFilesForInitTime(self, it: dt.datetime) -> tuple[dt.datetime, list[bytes]]:
        """Read all files from the raw dir as bytes for the given init time.

        :param it: The init time to read files for
        """
        initTimeDirPath = pathlib.Path(f"{self.__rawDir}/{it.strftime(internal.IT_FOLDER_FMTSTR)}")

        if not initTimeDirPath.exists():
            raise FileNotFoundError(f"Folder does not exist for init time {it} at {initTimeDirPath.as_posix()}")

        paths: list[pathlib.Path] = list(initTimeDirPath.iterdir())

        # TODO: Filter unwanted filenames

        # Read all files as bytes
        fileByteList: list[bytes] = [path.read_bytes() for path in paths]
        return it, fileByteList

    def zarrExistsForInitTime(self, name: str, it: dt.datetime) -> bool:
        """Check if a file exists in the zarr directory.

        :param name: The name of the file to check for
        :param it: The init time of the data within the Dataset
        """
        path = pathlib.Path(f"{self.__zarrDir}/{name}")
        return path.exists()

    def writeDatasetAsZarr(self, name: str, it: dt.datetime, ds: xr.Dataset) -> pathlib.Path:
        """Write the given Dataset to the zarr directory.

        :param name: Name of the file to write
        :param it: Init time of the data within the Dataset
        :param ds: Dataset to write
        """
        path = pathlib.Path(f"{self.__zarrDir}/{name}")

        # Ensure the zarr path doesn't already exist
        if self.zarrExistsForInitTime(name=name, it=it):
            raise FileExistsError(f"Zarr path already exists: {path}")

        # Create new Zarr store.
        ds["UKV"] = ds.astype(np.float16)["UKV"]
        ds.to_zarr(
            store=path,
            encoding={
                "init_time": {"units": "nanoseconds since 1970-01-01"},
                "UKV": {
                    "compressor": Blosc2(cname="zstd", clevel=5),
                },
            }
        )
        del ds
        return path

    def deleteZarrForInitTime(self, *, name: str, it: dt.datetime) -> None:
        """Delete the Zarr file for the given init time."""
        path = pathlib.Path(f"{self.__zarrDir}/{name}")

        if not path.exists():
            raise FileNotFoundError(f"Zarr file does not exist: {path}")

        shutil.rmtree(path.as_posix())
        return

