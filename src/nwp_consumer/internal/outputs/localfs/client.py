import datetime as dt
import os
import pathlib
import shutil

import structlog
from typeid import TypeID

from nwp_consumer import internal

log = structlog.getLogger()


class LocalFSClient(internal.StorageInterface):
    """Client for local filesystem."""

    def exists(self, *, dst: pathlib.Path) -> bool:
        return dst.exists()

    def store(self, *, src: pathlib.Path, dst: pathlib.Path) -> int:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(src=src, dst=dst)
        src.unlink(missing_ok=True)
        return os.stat(dst).st_size

    def listInitTimes(self, *, prefix: pathlib.Path) -> list[dt.datetime]:
        # List all the YYYY/MM/DD/INITTIME folders in the given directory
        dirs = [f.relative_to(prefix) for f in prefix.glob('*/*/*/*') if f.suffix == ""]

        initTimes = set()
        for dir in dirs:
            try:
                # Try to parse the dir as a datetime
                ddt: dt.datetime = dt.datetime.strptime(
                    dir.as_posix(),
                    internal.IT_FOLDER_FMTSTR
                ).replace(tzinfo=None)
                # Add the initTime to the set
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

    def copyITFolderToTemp(self, *, prefix: pathlib.Path, it: dt.datetime) \
            -> tuple[dt.datetime, list[pathlib.Path]]:
        initTimeDirPath = prefix / it.strftime(internal.IT_FOLDER_FMTSTR)

        if not initTimeDirPath.exists():
            raise FileNotFoundError(
                f"folder does not exist for init time {it} at {initTimeDirPath.as_posix()}"
            )

        paths: list[pathlib.Path] = list(initTimeDirPath.iterdir())

        # TODO: Filter unwanted filenames

        # Read all files into temporary files
        tempPaths: list[pathlib.Path] = []
        for path in paths:
            with path.open("rb") as infile:
                tfp: pathlib.Path = internal.TMP_DIR / str(TypeID(prefix='nwpc'))
                with tfp.open("wb") as tmpfile:
                    for chunk in iter(lambda: infile.read(16 * 1024), b""):
                        tmpfile.write(chunk)
                tempPaths.append(tfp)

        return it, tempPaths

    def delete(self, *, p: pathlib.Path) -> None:
        if not p.exists():
            raise FileNotFoundError(f"file does not exist: {p}")
        if p.is_file():
            p.unlink()
        elif p.is_dir():
            shutil.rmtree(p.as_posix())
        else:
            raise ValueError(f"path is not a file or directory: {p}")
        return
