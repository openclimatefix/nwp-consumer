import datetime as dt
import pathlib
import tempfile

import botocore.client
import botocore.exceptions
import numpy as np
import s3fs
import structlog
import xarray as xr
from ocf_blosc2 import Blosc2

from nwp_consumer import internal

log = structlog.getLogger()


class S3Client(internal.StorageInterface):
    """Client for AWS S3."""

    # S3 Bucket
    __bucket: str

    # Location to save raw (Usually GRIB) files
    __rawDir: pathlib.Path

    # Location to save Zarr files
    __zarrDir: pathlib.Path

    # S3 Accessor
    __s3: botocore.client

    def __init__(self, key: str, secret: str, rawDir: str, zarrDir: str, bucket: str, region: str,
                 endpointURL: str = None):
        """Create a new S3Client."""
        rawPath: pathlib.Path = pathlib.Path(rawDir)
        zarrPath: pathlib.Path = pathlib.Path(zarrDir)

        self.__fs: s3fs.S3FileSystem = s3fs.S3FileSystem(
            key=key,
            secret=secret,
            client_kwargs={
                'region_name': region,
                'endpoint_url': endpointURL,
            }
        )

        self.__rawDir = rawPath
        self.__zarrDir = zarrPath
        self.__bucket = bucket

    def rawFileExistsForInitTime(self, name: str, it: dt.datetime) -> bool:
        """Check if a file exists in the raw directory.

        :param name: The name of the file to check for
        :param it: The init time of the data within the file
        """
        path = self.__bucket \
               / self.__rawDir \
               / it.strftime(internal.IT_FOLDER_FMTSTR) \
               / name
        return self.__fs.exists(path.as_posix())

    def writeBytesToRawFile(self, name: str, it: dt.datetime, f: tempfile.NamedTemporaryFile) -> pathlib.Path:
        """Write the given bytes to the raw directory.

        :param name: The name of the file to write
        :param it: The init time of the data within the file
        :param f: The bytes to write
        """
        path = self.__bucket / self.__rawDir \
               / it.strftime(internal.IT_FOLDER_FMTSTR) / name

        with self.__fs.open(path.as_posix(), 'wb') as o:
            for chunk in iter(lambda: f.read(16 * 1024), b""):
                o.write(chunk)

        return path

    def listInitTimesInRawDir(self) -> list[dt.datetime]:
        """List all initTimes in the raw directory."""
        allDirs = [
            pathlib.Path(d).relative_to(f'{self.__bucket}/{self.__rawDir}')
            for d in self.__fs.glob(f'{self.__bucket}/{self.__rawDir}/*/*/*/*')
            if self.__fs.isdir(d)
        ]

        # Get the initTime from the folder pattern
        initTimes = set()
        for dir in allDirs:
            if dir.match('*/*/*/*'):
                try:
                    # Try to parse the folder name as a datetime
                    ddt = dt.datetime.strptime(
                        dir.as_posix(),
                        internal.IT_FOLDER_FMTSTR
                    ).replace(tzinfo=None)
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

    def readRawFilesForInitTime(self, it: dt.datetime) -> tuple[dt.datetime, list[tempfile.NamedTemporaryFile]]:
        """Read bytes of all files from the raw dir for the given initTime.

        :param it: The init time to read for
        """
        initTimeDirPath = self.__bucket / self.__rawDir \
                          / it.strftime(internal.IT_FOLDER_FMTSTR)
        files = self.__fs.ls(initTimeDirPath.as_posix())

        # Read all files as temporary files
        tempfiles: list[tempfile.NamedTemporaryFile] = []
        for file in files:
            with tempfile.NamedTemporaryFile("w+b", delete=False) as outfile:
                with self.__fs.open(file, "rb") as infile:
                    for chunk in iter(lambda: infile.read(16 * 1024), b""):
                        outfile.write(chunk)
                        outfile.flush()
                    tempfiles.append(outfile)

        return it, tempfiles

    def writeDatasetAsZarr(self, name: str, it: dt.datetime, ds: xr.Dataset) -> pathlib.Path:
        """Write the given Dataset to the zarr directory.

        :param name: The name of the file to write
        :param it: The init time of the data within the Dataset
        :param ds: The Dataset to write
        """
        path: pathlib.Path = (self.__bucket / self.__zarrDir / name).with_suffix('.zarr')

        # Ensure the zarr path doesn't already exist
        if self.zarrExistsForInitTime(name=name, it=it):
            raise FileExistsError(f"Zarr path already exists: {path}")

        # Create new Zarr store.
        ds["UKV"] = ds.astype(np.float16)["UKV"]
        ds.to_zarr(
            store=s3fs.S3Map(root="s3://" + path.as_posix(), s3=self.__fs, check=False),
            encoding={
                "init_time": {"units": "nanoseconds since 1970-01-01"},
                "UKV": {
                    "compressor": Blosc2(cname="zstd", clevel=5),
                },
            },
        )
        del ds
        return pathlib.Path(path)

    def zarrExistsForInitTime(self, name: str, it: dt.datetime) -> bool:
        """Check if a file exists in the zarr directory.

        :param name: The name of the file to check for
        :param it: The init time of the data within the file
        """
        path = self.__bucket / self.__zarrDir / name
        return self.__fs.exists(path.as_posix())

    def deleteZarrForInitTime(self, *, name: str, it: dt.datetime) -> None:
        """Delete the Zarr file for the given init time."""
        self.__fs.rm((self.__bucket / self.__zarrDir / name).as_posix(), recursive=True)
