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

    def name(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return "localfilesystem"

    def exists(self, *, dst: pathlib.Path) -> bool:
        """Overrides the corresponding method in the parent class."""
        return dst.exists()

    def store(self, *, src: pathlib.Path, dst: pathlib.Path) -> pathlib.Path:
        """Overrides the corresponding method in the parent class."""
        if src == dst:
            return dst

        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            shutil.copytree(src=src, dst=dst)
        else:
            shutil.copy(src=src, dst=dst)

        if src.stat().st_size != dst.stat().st_size:
            log.warn(
                event="file size mismatch",
                src=src.as_posix(),
                dst=dst.as_posix(),
                srcbytes=src.stat().st_size,
                dstbytes=dst.stat().st_size,
            )
        else:
            log.debug(
                event="stored file locally",
                src=src.as_posix(),
                dst=dst.as_posix(),
                nbytes=dst.stat().st_size,
            )

        # Delete the cache to avoid double storage
        try:
            src.unlink()
        except:
            log.warn(
                event="could not delete source file. Will be cleaned up at end of run",
                src=src.as_posix(),
            )

        return dst

    def listInitTimes(self, *, prefix: pathlib.Path) -> list[dt.datetime]:
        """Overrides the corresponding method in the parent class."""
        # List all the inittime folders in the given directory
        dirs = [
            f.relative_to(prefix)
            for f in prefix.glob(internal.IT_FOLDER_GLOBSTR_RAW)
            if f.suffix == ""
        ]

        initTimes = set()
        for dir in dirs:
            try:
                # Try to parse the dir as a datetime
                ddt: dt.datetime = dt.datetime.strptime(
                    dir.as_posix(),
                    internal.IT_FOLDER_STRUCTURE_RAW,
                ).replace(tzinfo=dt.UTC)
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

    def copyITFolderToCache(self, *, prefix: pathlib.Path, it: dt.datetime) -> list[pathlib.Path]:
        """Overrides the corresponding method in the parent class."""
        # Check if the folder exists
        if not (prefix / it.strftime(internal.IT_FOLDER_STRUCTURE_RAW)).exists():
            log.debug(
                event="Init time folder not present",
                path=(prefix / it.strftime(internal.IT_FOLDER_STRUCTURE_RAW)).as_posix(),
            )
            return []
        filesInFolder = list((prefix / it.strftime(internal.IT_FOLDER_STRUCTURE_RAW)).iterdir())

        cfps: list[pathlib.Path] = []
        for file in filesInFolder:
            # Copy the file to the cache if it isn't already there
            dst: pathlib.Path = internal.rawCachePath(it=it, filename=file.name)
            if not dst.exists():
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src=file, dst=dst)
            cfps.append(dst)

        return cfps

    def delete(self, *, p: pathlib.Path) -> None:
        """Overrides the corresponding method in the parent class."""
        if not p.exists():
            raise FileNotFoundError(f"file does not exist: {p}")
        if p.is_file():
            p.unlink()
        elif p.is_dir():
            shutil.rmtree(p.as_posix())
        else:
            raise ValueError(f"path is not a file or directory: {p}")
        return
