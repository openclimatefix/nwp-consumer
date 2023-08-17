import datetime as dt
import pathlib

from huggingface_hub import HfFileSystem
import structlog

from nwp_consumer import internal

log = structlog.getLogger()


class HuggingFaceClient(internal.StorageInterface):
    """Client for HuggingFace."""

    # HuggingFace Filesystem
    __fs: HfFileSystem

    def __init__(self, token: str | None):
        """Create a new HuggingFaceClient."""
        self.__fs = HfFileSystem(token=token)

    def exists(self, *, dst: pathlib.Path) -> bool:
        return self.__fs.exists(dst.as_posix())

    def store(self, *, src: pathlib.Path, dst: pathlib.Path) -> int:
        self.__fs.put(lpath=src.as_posix(), rpath=dst.as_posix(), recursive=True)
        return self.__fs.du(path=dst.as_posix())

    def listInitTimes(self, *, prefix: pathlib.Path) -> list[dt.datetime]:
        allDirs = [
            pathlib.Path(d).relative_to(prefix)
            for d in self.__fs.glob(f'{prefix}/*/*/*/*')
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

    def copyITFolderToTemp(self, *, prefix: pathlib.Path, it: dt.datetime) -> list[pathlib.Path]:
        initTimeDirPath = prefix / it.strftime(internal.IT_FOLDER_FMTSTR)
        paths = [pathlib.Path(p) for p in self.__fs.ls(initTimeDirPath.as_posix())]

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
            if self.exists(dst=path) is False or self.__fs.du(path=path.as_posix()) == 0:
                log.warn(
                    event="file in store is empty",
                    filepath=path.as_posix(),
                )
                continue

            # Copy the file from the store to a temporary file
            with self.__fs.open(path=path.as_posix(), mode="rb") as infile:
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
        if self.__fs.isdir(p.as_posix()):
            self.__fs.rm(p.as_posix(), recursive=True)
        else:
            self.__fs.rm(p.as_posix())
