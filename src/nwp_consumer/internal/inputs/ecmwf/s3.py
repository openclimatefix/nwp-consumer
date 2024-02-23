"""Input covering an OCF-specific use case of pulling ECMWF data from an s3 bucket.

The input files are extensionless, but are in fact grib files containing data for
multiple regions.
"""

import datetime as dt
import pathlib

import s3fs
import structlog

from nwp_consumer import internal

from ._models import ECMWFLiveFileInfo

log = structlog.getLogger()


class S3Client(internal.FetcherInterface):
    """Implements a client to fetch ECMWF data from S3."""

    area: str
    desired_params: list[str]
    bucket: pathlib.Path

    __fs: s3fs.S3FileSystem

    bucketPath: str = "ecmwf"

    def __init__(
        self,
        bucket: str,
        region: str,
        area: str = "uk",
        key: str | None = "",
        secret: str | None = "",
        endpointURL: str = "",
    ) -> None:
        """Creates a new ECMWF S3 client.

        Exposes a client for fetching ECMWF data from an S3 bucket conforming to the
        FetcherInterface. ECMWF S3 data is order-based, so parameters and steps cannot be
        requested by this client.

        Args:
            bucket: The name of the S3 bucket to fetch data from.
            region: The AWS region to connect to.
            key: The AWS access key to use for authentication.
            secret: The AWS secret key to use for authentication.
            area: The area for which to fetch data.
            hours: The number of hours to fetch data for.
        """
        if (key, secret) == ("", ""):
            log.info(
                event="attempting AWS connection using default credentials",
            )
            key, secret = None, None

        self.__fs: s3fs.S3FileSystem = s3fs.S3FileSystem(
            key=key,
            secret=secret,
            client_kwargs={
                "region_name": region,
                "endpoint_url": None if endpointURL == "" else endpointURL,
            },
        )
        self.area = area
        self.bucket = pathlib.Path(bucket)

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:
        """Overrides the corresponding method in the parent class."""
        allFiles: list[str] = self.__fs.ls((self.bucket / self.bucketPath).as_posix())
        initTimeFiles: list[internal.FileInfoModel] = [
            ECMWFLiveFileInfo(fname=file) for file in allFiles if it.strftime("A1D%m%d%H") in file
        ]
        return initTimeFiles

    def downloadToCache(
        self, *, fi: internal.FileInfoModel
    ) -> tuple[internal.FileInfoModel, pathlib.Path]:
        """Overrides the corresponding method in the parent class."""
        cfp: pathlib.Path = internal.rawCachePath(it=fi.it(), filename=fi.filename())
        with open(cfp, "wb") as f, self.__fs.open(fi.filename(), "rb") as s:
            for chunk in iter(lambda: s.read(12 * 1024), b""):
                f.write(chunk)
                f.flush()

        if not cfp.exists():
            log.warn(event="Failed to download file", filepath=fi.filepath())
            return pathlib.Path(), cfp

        # Check the sizes are the same
        s3size = self.__fs.info((self.bucket / fi.filepath()).as_posix())["size"]
        if cfp.stat().st_size != s3size:
            log.warn(
                event="Downloaded file size does not match expected size",
                expected=s3size,
                actual=cfp.stat().st_size,
            )
            return pathlib.Path(), cfp

        return fi, cfp
