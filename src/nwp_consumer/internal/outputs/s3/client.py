import datetime as dt
import pathlib

import boto3
import botocore.client
import botocore.exceptions
import numpy as np
import xarray as xr
from ocf_blosc2 import Blosc2
import structlog

from nwp_consumer import internal


log = structlog.stdlib.get_logger()

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

    def __init__(self, key: str, secret: str, rawDir: str, zarrDir: str, bucket: str, region: str):
        """Create a new S3Client."""
        rawPath: pathlib.Path = pathlib.Path(rawDir)
        zarrPath: pathlib.Path = pathlib.Path(zarrDir)

        self.__s3 = boto3.client(
            's3',
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            region_name=region,
        )

        self.__rawDir = rawPath
        self.__zarrDir = zarrPath
        self.__bucket = bucket

    def rawFileExistsForInitTime(self, name: str, it: dt.datetime) -> bool:
        """Check if a file exists in the raw directory.
        
        :param name: The name of the file to check for
        :param it: The init time of the data within the file
        """
        path = self.__rawDir / it.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING) / name
        try:
            self.__s3.head_object(Bucket=self.__bucket, Key=path.as_posix())
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The key does not exist.
                return False
            else:
                # Something else has gone wrong.
                raise e
        return True

    def writeBytesToRawFile(self, name: str, it: dt.datetime, b: bytes) -> pathlib.Path:
        """Write the given bytes to the raw directory.
        
        :param name: The name of the file to write
        :param it: The init time of the data within the file
        :param b: The bytes to write
        """
        path = self.__rawDir / it.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING) / name

        self.__s3.put_object(Bucket=self.__bucket, Key=path.as_posix(), Body=b)
        return path

    def listInitTimesInRawDir(self) -> list[dt.datetime]:
        """List all initTimes in the raw directory."""
        paginator = self.__s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.__bucket, Prefix=self.__rawDir.as_posix() + "/")

        # Get a list of all the Prefixes in the bucket
        # * Follow the pattern 'rawDir/{PREFIX/MAY/HAVE/SLASHES}/filename.ext'
        allDirs = set()
        for page in pages:
            for obj in page['Contents']:
                allDirs.add(pathlib.Path(obj['Key']).relative_to(self.__rawDir).parent)

        # Get the initTime from the folder pattern
        initTimes = set()
        for dir in allDirs:
            if dir.match('*/*/*/*'):
                try:
                    # Try to parse the folder name as a datetime
                    ddt = dt.datetime.strptime(
                        dir.as_posix(),
                        internal.RAW_FOLDER_PATTERN_FMT_STRING
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

    def readRawFilesForInitTime(self, it: dt.datetime) -> tuple[dt.datetime, list[bytes]]:
        """Read bytes of all files from the raw dir for the given initTime.

        :param it: The init time to read for
        """
        initTimeDirPath = self.__rawDir / it.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING)
        response = self.__s3.list_objects_v2(Bucket=self.__bucket, Prefix=initTimeDirPath.as_posix() + "/")

        return it, [self.__s3.get_object(Bucket=self.__bucket, Key=obj['Key'])['Body'].read()
                    for obj in response['Contents']]

    def writeDatasetAsZarr(self, name: str, it: dt.datetime, ds: xr.Dataset) -> pathlib.Path:
        """Write the given Dataset to the zarr directory.
        
        :param name: The name of the file to write
        :param it: The init time of the data within the Dataset
        :param ds: The Dataset to write
        """
        path: pathlib.Path = self.__bucket / self.__zarrDir / name

        # Ensure the zarr path doesn't already exist
        if self.zarrExistsForInitTime(name=name, it=it):
            raise FileExistsError(f"Zarr path already exists: {path}")

        # Create a chunked Dask Dataset from the input multi-variate Dataset.
        # *  Converts the input multivariate DataSet (with different DataArrays for
        #     each NWP variable) to a single DataArray with a `variable` dimension.
        # * This allows each Zarr chunk to hold multiple variables (useful for loading
        #     many/all variables at once from disk).

        # Create single-variate dataarray from dataset, with new "variable" dimension
        da = ds \
            .to_array(dim="variable", name="UKV") \
            .compute()
        del ds

        # Convert back to dataset, order dimensions, and chunk
        chunkedDataset = da.to_dataset() \
            .transpose("init_time", "step", "variable", "y", "x") \
            .sortby("step") \
            .sortby("variable") \
            .chunk({
                "init_time": 1,
                "step": 1,
                "variable": -1,
                "y": len(da.y) // 2,
                "x": len(da.x) // 2,
            }) \
            .compute()
        del da

        # Create new Zarr store.
        chunkedDataset["UKV"] = chunkedDataset.astype(np.float16)["UKV"]
        chunkedDataset.to_zarr(
            store="s3://" + path.as_posix(),
            encoding={
                "init_time": {"units": "nanoseconds since 1970-01-01"},
                "UKV": {
                    "compressor": Blosc2(cname="zstd", clevel=5),
                },
            },
        )
        del chunkedDataset
        return pathlib.Path(path)

    def zarrExistsForInitTime(self, name: str, it: dt.datetime) -> bool:
        """Check if a file exists in the zarr directory.
        
        :param name: The name of the file to check for
        :param it: The init time of the data within the file
        """
        path = self.__zarrDir / name
        try:
            self.__s3.head_object(
                Bucket=self.__bucket,
                Key=path.as_posix()
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The key does not exist.
                return False
            else:
                # Something else has gone wrong.
                raise e
        return True

    def deleteZarrForInitTime(self, *, name: str, it: dt.datetime) -> None:
        """Delete the Zarr file for the given init time."""

        paginator = self.__s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.__bucket, Prefix=(self.__zarrDir / name).as_posix() + "/")

        # Get a list of all the files in the zarr folder
        allDirs = set()
        for page in pages:
            print(page)
            for obj in page['Contents']:
                print(obj)
