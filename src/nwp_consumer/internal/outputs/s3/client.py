"""Client for AWS S3."""

import datetime as dt
import pathlib

import s3fs
import structlog

from nwp_consumer import internal

log = structlog.getLogger()


class Client(internal.StorageInterface):
    """Storage Interface client for AWS S3."""

    # S3 Bucket
    __bucket: pathlib.Path

    # S3 Filesystem
    __fs: s3fs.S3FileSystem

    def __init__(
        self,
        *,
        bucket: str,
        region: str,
        key: str | None = "",
        secret: str| None = "",
        endpointURL: str = "",
    ) -> None:
        """Create a new S3Client.

        Exposes a client that conforms to the StorageInterface.
        Provide credentials either explicitly via key and secret
        or fallback to default credentials if not provided or empty.

        Args:
            bucket: S3 bucket name to use for storage.
            region: S3 region the bucket is in.
            key: Use this access key, if specified.
            secret: Use this secret, if specified.
            endpointURL: Use this endpoint URL, if specified.
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

        self.__bucket = pathlib.Path(bucket)

    def name(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return "s3"

    def exists(self, *, dst: pathlib.Path) -> bool:
        """Overrides the corresponding method in the parent class."""
        return self.__fs.exists((self.__bucket / dst).as_posix())

    def store(self, *, src: pathlib.Path, dst: pathlib.Path) -> pathlib.Path:
        """Overrides the corresponding method in the parent class."""
        log.debug(
            event="storing file in s3",
            src=src.as_posix(),
            dst=(self.__bucket / dst).as_posix(),
        )

        # If file already exists in store and is of the same size, skip the upload
        if self.exists(dst=dst) and self.__fs.du((self.__bucket / dst).as_posix()) == src.stat().st_size:
            log.debug(
                event="file of same size already exists in s3, skipping",
                src=src.as_posix(),
                dst=(self.__bucket / dst).as_posix(),
            )
            return dst

        # Upload the file to the store
        self.__fs.put(lpath=src.as_posix(), rpath=(self.__bucket / dst).as_posix(), recursive=True)
        # Don't delete cached file as user may want to do further processing locally.
        remote_size_bytes: int = self.__fs.du((self.__bucket / dst).as_posix())
        local_size_bytes: int = src.stat().st_size
        if src.is_dir():
            local_size_bytes: int = sum(
                f.stat().st_size
                for f in src.rglob("*")
                if f.is_file()
            )
        if remote_size_bytes != local_size_bytes:
            log.warn(
                event="file size mismatch",
                src=src.as_posix(),
                dst=(self.__bucket / dst).as_posix(),
                srcsize=src.stat().st_size,
                dstsize=remote_size_bytes,
            )
        else:
            log.debug(
                event="stored file in s3",
                src=src.as_posix(),
                dst=(self.__bucket / dst).as_posix(),
                remote_size_bytes=remote_size_bytes,
            )
        return dst

    def listInitTimes(self, *, prefix: pathlib.Path) -> list[dt.datetime]:
        """Overrides the corresponding method in the parent class."""
        allDirs = [
            pathlib.Path(d).relative_to(self.__bucket / prefix)
            for d in self.__fs.glob(f"{self.__bucket}/{prefix}/{internal.IT_FOLDER_GLOBSTR_RAW}")
            if self.__fs.isdir(d)
        ]

        # Get the initTime from the folder pattern
        initTimes = set()
        for dir in allDirs:
            if dir.match(internal.IT_FOLDER_GLOBSTR_RAW):
                try:
                    # Try to parse the folder name as a datetime
                    ddt = dt.datetime.strptime(dir.as_posix(), internal.IT_FOLDER_STRUCTURE_RAW).replace(
                        tzinfo=dt.UTC,
                    )
                    initTimes.add(ddt)
                except ValueError:
                    log.debug(
                        event="ignoring invalid folder name",
                        name=dir.as_posix(),
                        within=prefix.as_posix(),
                    )

        sortedInitTimes = sorted(initTimes)
        log.debug(
            event=f"found {len(initTimes)} init times in raw directory",
            earliest=sortedInitTimes[0],
            latest=sortedInitTimes[-1],
        )
        return sortedInitTimes

    def copyITFolderToCache(self, *, prefix: pathlib.Path, it: dt.datetime) -> list[pathlib.Path]:
        """Overrides the corresponding method in the parent class."""
        initTimeDirPath = self.__bucket / prefix / it.strftime(internal.IT_FOLDER_STRUCTURE_RAW)

        if not self.__fs.exists(initTimeDirPath.as_posix()) or not self.__fs.isdir(initTimeDirPath.as_posix()):
            log.warn(
                event="init time folder does not exist in store",
                path=it.strftime(internal.IT_FOLDER_STRUCTURE_RAW),
            )
            return []

        paths = [
            pathlib.Path(p).relative_to(self.__bucket)
            for p in self.__fs.ls(initTimeDirPath.as_posix())
        ]

        log.debug(
            event="copying it folder to cache",
            inittime=it.strftime(internal.IT_FOLDER_STRUCTURE_RAW),
            numfiles=len(paths),
        )

        # Read all files into cache
        cachedPaths: list[pathlib.Path] = []
        for path in paths:
            cfp: pathlib.Path = internal.rawCachePath(it=it, filename=path.name)

            # Use existing cached file if it exists and is not empty
            if cfp.exists() and cfp.stat().st_size > 0:
                log.debug(
                    event="file already exists in cache, skipping",
                    filepath=path.as_posix(),
                    cachepath=cfp.as_posix(),
                )
                cachedPaths.append(cfp)
                continue

            # Don't copy file from the store if it is empty
            if (
                self.exists(dst=path) is False
                or self.__fs.du(path=(self.__bucket / path).as_posix()) == 0
            ):
                log.warn(
                    event="file in store is empty",
                    filepath=path.as_posix(),
                )
                continue

            # Copy the file from the store to cache
            with self.__fs.open(path=(self.__bucket / path).as_posix(), mode="rb") as infile:
                with cfp.open("wb") as tmpfile:
                    for chunk in iter(lambda: infile.read(16 * 1024), b""):
                        tmpfile.write(chunk)
                        tmpfile.flush()
                cachedPaths.append(cfp)

        log.debug(
            event="copied it folder to cache",
            nbytes=[p.stat().st_size for p in cachedPaths],
            inittime=it.strftime("%Y-%m-%d %H:%M"),
        )

        return cachedPaths

    def delete(self, *, p: pathlib.Path) -> None:
        """Overrides the corresponding method in the parent class."""
        if self.__fs.isdir((self.__bucket / p).as_posix()):
            self.__fs.rm((self.__bucket / p).as_posix(), recursive=True)
        else:
            self.__fs.rm((self.__bucket / p).as_posix())
