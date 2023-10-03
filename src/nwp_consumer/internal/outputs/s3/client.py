import datetime as dt
import pathlib

import s3fs
import structlog

from nwp_consumer import internal

log = structlog.getLogger()


class Client(internal.StorageInterface):
    """Client for AWS S3."""

    # S3 Bucket
    __bucket: pathlib.Path

    # S3 Filesystem
    __fs: s3fs.S3FileSystem

    def __init__(self, key: str, secret: str, bucket: str, region: str,
                 endpointURL: str = None) -> None:
        """Create a new S3Client."""
        (key, secret) = (None, None) if (key, secret) == ("", "") else (key, secret)
        if key is None and secret is None:
            log.info(
                event="attempting AWS connection using default credentials",
            )

        # S3FileSystem will attempt connection via default credentials if key and secret are None
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
        log.debug(
            event="storing file in s3",
            src=src.as_posix(),
            dst=(self.__bucket / dst).as_posix(),
        )
        self.__fs.put(lpath=src.as_posix(), rpath=(self.__bucket / dst).as_posix(), recursive=True)
        # Don't delete temp file as user may want to do further processing locally.
        # All temp files are deleted at the end of the program.
        nbytes = self.__fs.du((self.__bucket / dst).as_posix())
        log.debug(
            event="stored file in s3",
            src=src.as_posix(),
            dst=(self.__bucket / dst).as_posix(),
            nbytes=nbytes
        )
        return nbytes

    def listInitTimes(self, *, prefix: pathlib.Path) -> list[dt.datetime]:
        allDirs = [
            pathlib.Path(d).relative_to(self.__bucket / prefix)
            for d in self.__fs.glob(f'{self.__bucket}/{prefix}/{internal.IT_FOLDER_GLOBSTR}')
            if self.__fs.isdir(d)
        ]

        # Get the initTime from the folder pattern
        initTimes = set()
        for dir in allDirs:
            if dir.match(internal.IT_FOLDER_GLOBSTR):
                try:
                    # Try to parse the folder name as a datetime
                    ddt = dt.datetime.strptime(
                        dir.as_posix(),
                        internal.IT_FOLDER_FMTSTR
                    ).replace(tzinfo=None)
                    initTimes.add(ddt)
                except ValueError:
                    log.debug(
                        event="ignoring invalid folder name",
                        name=dir.as_posix(),
                        within=prefix.as_posix()
                    )

        sortedInitTimes = sorted(initTimes)
        log.debug(
            event=f"found {len(initTimes)} init times in raw directory",
            earliest=sortedInitTimes[0],
            latest=sortedInitTimes[-1]
        )
        return sortedInitTimes

    def copyITFolderToTemp(self, *, prefix: pathlib.Path, it: dt.datetime) -> list[pathlib.Path]:
        initTimeDirPath = self.__bucket / prefix / it.strftime(internal.IT_FOLDER_FMTSTR)
        paths = [
            pathlib.Path(p).relative_to(self.__bucket)
            for p in self.__fs.ls(initTimeDirPath.as_posix())
        ]

        log.debug(
            event="copying it folder to temporary files",
            inittime=it.strftime(internal.IT_FOLDER_FMTSTR),
            numfiles=len(paths)
        )

        # Read all files into temporary files
        tempPaths: list[pathlib.Path] = []
        for path in paths:
            tfp: pathlib.Path = internal.TMP_DIR / path.name

            # Use existing temp file if it already exists in the temp dir
            if tfp.exists() and tfp.stat().st_size > 0:
                log.debug(
                    event="file already exists in temporary directory, skipping",
                    filepath=path.as_posix(),
                    temppath=tfp.as_posix()
                )
                tempPaths.append(tfp)
                continue

            # Don't copy file from the store if it is empty
            if self.exists(dst=path) is False \
                    or self.__fs.du(path=(self.__bucket / path).as_posix()) == 0:
                log.warn(
                    event="file in store is empty",
                    filepath=path.as_posix(),
                )
                continue

            # Copy the file from the store to a temporary file
            with self.__fs.open(path=(self.__bucket / path).as_posix(), mode="rb") as infile:
                with tfp.open("wb") as tmpfile:
                    for chunk in iter(lambda: infile.read(16 * 1024), b""):
                        tmpfile.write(chunk)
                        tmpfile.flush()
                tempPaths.append(tfp)

        log.debug(
            event="copied it folder to temporary files",
            nbytes=[p.stat().st_size for p in tempPaths],
            inittime=it.strftime("%Y-%m-%d %H:%M")
        )

        return tempPaths

    def delete(self, *, p: pathlib.Path) -> None:
        if self.__fs.isdir((self.__bucket / p).as_posix()):
            self.__fs.rm((self.__bucket / p).as_posix(), recursive=True)
        else:
            self.__fs.rm((self.__bucket / p).as_posix())
