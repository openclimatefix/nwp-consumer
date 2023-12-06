"""Client for local filesystem."""

import datetime as dt
import os
import pathlib
import shutil

import structlog

from nwp_consumer import internal

log = structlog.getLogger()


class Client(internal.StorageInterface):
    """Client for local filesystem.

    This class implements the StorageInterface for the local filesystem.
    """

    def exists(self, *, dst: pathlib.Path) -> bool:  # noqa: D102
        return dst.exists()

    def store(self, *, src: pathlib.Path, dst: pathlib.Path) -> pathlib.Path:  # noqa: D102
        if src == dst:
            return dst

        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(src=src, dst=dst)
        # Do delete temp file here to avoid local duplication of file.
        src.unlink(missing_ok=True)
        nbytes = os.stat(dst).st_size
        if nbytes != dst.stat().st_size:
            log.warn(
                event="file size mismatch",
                src=src.as_posix(),
                dst=dst.as_posix(),
                srcbytes=src.stat().st_size,
                dstbytes=nbytes,
            )
        else:
            log.debug(
                event="stored file locally",
                src=src.as_posix(),
                dst=dst.as_posix(),
                nbytes=nbytes,
            )
        return dst

    def listInitTimes(self, *, prefix: pathlib.Path) -> list[dt.datetime]:  # noqa: D102
        # List all the inittime folders in the given directory
        dirs = [
            f.relative_to(prefix) for f in prefix.glob(internal.IT_FOLDER_GLOBSTR) if f.suffix == ""
        ]

        initTimes = set()
        for dir in dirs:
            try:
                # Try to parse the dir as a datetime
                ddt: dt.datetime = dt.datetime.strptime(
                    dir.as_posix(),
                    internal.IT_FOLDER_FMTSTR,
                ).replace(tzinfo=dt.timezone.utc)
                # Add the initTime to the set
                initTimes.add(ddt)
            except ValueError:
                log.debug(
                    event="ignoring invalid folder name",
                    name=dir.as_posix(),
                    within=prefix.as_posix(),
                )

        if len(initTimes) == 0:
            log.debug(
                event="no init times found in raw directory",
                within=prefix.as_posix(),
            )
            return []

        sortedInitTimes = sorted(initTimes)
        log.debug(
            event=f"found {len(initTimes)} init times in raw directory",
            earliest=sortedInitTimes[0],
            latest=sortedInitTimes[-1],
        )

        return sortedInitTimes

    def copyITFolderToTemp(  # noqa: D102
        self, *, prefix: pathlib.Path, it: dt.datetime,
    ) -> list[pathlib.Path]:
        # Local FS already has access to files, so just return the paths
        initTimeDirPath = prefix / it.strftime(internal.IT_FOLDER_FMTSTR)
        paths: list[pathlib.Path] = list(initTimeDirPath.iterdir())

        return paths

    def delete(self, *, p: pathlib.Path) -> None:  # noqa: D102
        if not p.exists():
            raise FileNotFoundError(f"file does not exist: {p}")
        if p.is_file():
            p.unlink()
        elif p.is_dir():
            shutil.rmtree(p.as_posix())
        else:
            raise ValueError(f"path is not a file or directory: {p}")
        return
