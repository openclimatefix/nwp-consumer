import pathlib
import tempfile

import boto3
import io
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

    def existsInRawDir(self, relativePath: pathlib.Path) -> bool:
        """Check if a file exists in the raw directory."""
        path = self.__rawDir / relativePath
        return self.__s3.Object(bucket_name=self.__bucket, key=path.as_posix()).exists()

    def existsInZarrDir(self, relativePath: pathlib.Path) -> bool:
        """Check if a file exists in the zarr directory."""
        path = self.__zarrDir / relativePath
        return self.__s3.Object(bucket_name=self.__bucket, key=path.as_posix()).exists()

    def readBytesFromRawDir(self, relativePath: pathlib.Path) -> bytes:
        """Read a file from the raw dir, returning a file-like object."""
        path = self.__rawDir / relativePath

        if self.existsInRawDir(relativePath=relativePath):
            return self.__s3.Object(bucket_name=self.__bucket, key=path.as_posix()).get()['Body'].read()
        else:
            raise FileNotFoundError(f"File not found in raw dir: {path}")

    def writeBytesToRawDir(self, relativePath: pathlib.Path, data: bytes) -> pathlib.Path:
        """Write the given bytes to the raw directory."""
        path = self.__rawDir / relativePath

        self.__s3.Object(bucket_name=self.__bucket, key=path.as_posix()).put(Body=data)
        return path

    def removeFromRawDir(self, relativePath: pathlib.Path) -> None:
        """Remove a file from the raw dir."""
        path = self.__rawDir / relativePath
        self.__s3.Object(bucket_name=self.__bucket, key=path.as_posix()).delete()

    def saveDataset(self, dataset: xr.Dataset, relativePath: pathlib.Path) -> None:
        # TODO
        pass
