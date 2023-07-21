import datetime as dt
import pathlib

import botocore.client
import botocore.exceptions
import s3fs
import structlog

from nwp_consumer import internal

log = structlog.getLogger()


class S3Client(internal.StorageInterface):
    """Client for AWS S3."""

    # S3 Bucket
    __bucket: pathlib.Path

    # S3 Accessor
    __s3: botocore.client

    def __init__(self, key: str, secret: str, bucket: str, region: str,
                 endpointURL: str = None):
        """Create a new S3Client."""
        self.__fs: s3fs.S3FileSystem = s3fs.S3FileSystem(
            key=key,
            secret=secret,
            client_kwargs={
                'region_name': region,
                'endpoint_url': endpointURL,
            }
        )

        self.__bucket = pathlib.Path(bucket)

    def exists(self, *, dst: pathlib.Path) -> bool:
        return self.__fs.exists((self.__bucket / dst).as_posix())

    def store(self, *, src: pathlib.Path, dst: pathlib.Path) -> int:
        self.__fs.put(lpath=src.as_posix(), rpath=(self.__bucket / dst).as_posix(), recursive=True)
        src.unlink()
        return self.__fs.du((self.__bucket / dst).as_posix())

    def listInitTimes(self, *, prefix: pathlib.Path) -> list[dt.datetime]:
        """List all initTimes in the raw directory."""
        allDirs = [
            pathlib.Path(d).relative_to(self.__bucket / prefix)
            for d in self.__fs.glob(f'{self.__bucket}/{prefix}/*/*/*/*')
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
                    log.debug(f"ignoring invalid folder name", name=dir.as_posix(), within=prefix.as_posix())

        sortedInitTimes = sorted(initTimes)
        log.debug(
            event=f"found {len(initTimes)} init times in raw directory",
            earliest=sortedInitTimes[0],
            latest=sortedInitTimes[-1]
        )
        return sortedInitTimes

    def copyITFolderToTemp(self, *, prefix: pathlib.Path, it: dt.datetime) -> tuple[dt.datetime, list[pathlib.Path]]:
        initTimeDirPath = self.__bucket / prefix / it.strftime(internal.IT_FOLDER_FMTSTR)
        paths = [pathlib.Path(p) for p in self.__fs.ls(initTimeDirPath.as_posix())]

        # Read all files into temporary files
        tempPaths: list[pathlib.Path] = []
        for path in paths:
            if path.exists() is False or path.stat().st_size == 0:
                log.warn(
                    event="temporary file is empty",
                    filepath=path.as_posix()
                )
                continue
            with self.__fs.open(path=path.as_posix(), mode="rb") as infile:
                tfp: pathlib.Path = internal.TMP_DIR / path.name
                with tfp.open("wb") as tmpfile:
                    for chunk in iter(lambda: infile.read(16 * 1024), b""):
                        tmpfile.write(chunk)
                        tmpfile.flush()
                tempPaths.append(tfp)

        log.debug(
            event="copied it folder to temporary files",
            nbytes=[p.stat().st_size for p in tempPaths]
        )

        return it, tempPaths

    def delete(self, *, p: pathlib.Path) -> None:
        if self.__fs.isdir((self.__bucket / p).as_posix()):
            self.__fs.rm((self.__bucket / p).as_posix(), recursive=True)
        else:
            self.__fs.rm((self.__bucket / p).as_posix())
