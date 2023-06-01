import datetime as dt
import pathlib

import boto3
import xarray as xr

from nwp_consumer import internal


class S3Client(internal.StorageInterface):
    """Client for AWS S3."""

    # S3 Bucket
    __bucket: str

    # Location to save raw (Usually GRIB) files
    __rawDir: pathlib.Path

    # Location to save Zarr files
    __zarrDir: pathlib.Path

    # S3 Accessor
    __s3: boto3.session.Session.resource

    def __init__(self, key: str, secret: str, rawDir: str, zarrDir: str, bucket: str):
        """Create a new S3Client."""
        rawPath: pathlib.Path = pathlib.Path(rawDir)
        zarrPath: pathlib.Path = pathlib.Path(zarrDir)

        self.__s3 = boto3.resource(
            's3',
            region_name='eu-west-1',
            aws_access_key_id=key,
            aws_secret_access_key=secret,
        )

        self.__rawDir = rawPath
        self.__zarrDir = zarrPath
        self.__bucket = bucket

    def existsInRawDir(self, fileName: str, initTime: dt.datetime) -> bool:
        """Check if a file exists in the raw directory."""
        path = self.__rawDir / initTime.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING) / fileName
        return self.__s3.Object(bucket_name=self.__bucket, key=path.as_posix()).exists()

    def writeBytesToRawDir(self, fileName: str, initTime: dt.datetime, data: bytes) -> pathlib.Path:
        """Write the given bytes to the raw directory."""
        path = self.__rawDir / initTime.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING) / fileName

        self.__s3.Object(bucket_name=self.__bucket, key=path.as_posix()).put(Body=data)
        return path

    def listInitTimesInRawDir(self) -> list[dt.datetime]:
        raise NotImplementedError()

    def readBytesForInitTime(self, initTime: dt.datetime) -> tuple[dt.datetime, list[bytes]]:
        """Read a file from the raw dir, returning a file-like object."""
        initTimeDirPath = self.__rawDir / initTime.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING)
        raise NotImplementedError()

    def writeDatasetToZarrDir(self, fileName: str, initTime: dt.datetime, data: xr.Dataset) -> pathlib.Path:
        raise NotImplementedError()

    def existsInZarrDir(self, fileName: str, initTime: dt.datetime) -> bool:
        """Check if a file exists in the zarr directory."""
        path = self.__zarrDir / fileName
        return self.__s3.Object(bucket_name=self.__bucket, key=path.as_posix()).exists()
