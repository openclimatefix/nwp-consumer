"""Client for HuggingFace."""

import datetime as dt
import pathlib

import huggingface_hub
import structlog

from nwp_consumer import internal

log = structlog.getLogger()


class Client(internal.StorageInterface):
    """Client for HuggingFace."""

    # HuggingFace Filesystem
    __fs: huggingface_hub.HfFileSystem

    # Path prefix
    datasetPath: pathlib.Path

    def __init__(self, repoID: str, token: str | None = None, endpoint: str | None = None) -> None:
        """Create a new client for HuggingFace.

        Exposes a client for the HuggingFace filesystem APIthat conforms to the StorageInterface.

        Args:
            repoID: The ID of the repo to use for the dataset.
            token: The HuggingFace authentication token.
            endpoint: The HuggingFace endpoint to use.
        """
        self.__fs = huggingface_hub.HfFileSystem(token=token, endpoint=endpoint)
        # See https://huggingface.co/docs/huggingface_hub/guides/hf_file_system#integrations
        self.datasetPath = pathlib.Path(f"datasets/{repoID}")

        try:
            self.__fs._api.dataset_info(repo_id=self.datasetPath.relative_to("datasets").as_posix())
        except Exception as e:
            log.warn(
                event="failed to authenticate with huggingface for given repo",
                repo_id=self.datasetPath.relative_to("datasets").as_posix(),
                error=e,
            )

    def exists(self, *, dst: pathlib.Path) -> bool:  # noqa: D102
        return self.__fs.exists(path=self.datasetPath / dst.as_posix())

    def store(self, *, src: pathlib.Path, dst: pathlib.Path) -> pathlib.Path:  # noqa: D102
        self.__fs.put(
            lpath=src.as_posix(),
            rpath=(self.datasetPath / dst).as_posix(),
            recursive=True,
        )
        nbytes = self.__fs.du(path=(self.datasetPath / dst).as_posix())
        if nbytes != src.stat().st_size:
            log.warn(
                event="stored file size does not match source file size",
                src=src.as_posix(),
                dst=dst.as_posix(),
                srcsize=src.stat().st_size,
                dstsize=nbytes,
            )
        else:
            log.debug(
                event="stored file",
                filepath=dst.as_posix(),
                nbytes=nbytes,
            )
        return dst

    def listInitTimes(self, *, prefix: pathlib.Path) -> list[dt.datetime]:  # noqa: D102
        allDirs = [
            pathlib.Path(d).relative_to(self.datasetPath / prefix)
            for d in self.__fs.glob(
                path=self.datasetPath / f"{prefix}/{internal.IT_FOLDER_GLOBSTR}",
            )
            if self.__fs.isdir(path=d)
        ]

        # Get the initTime from the folder pattern
        initTimes = set()
        for dir in allDirs:
            if dir.match(internal.IT_FOLDER_GLOBSTR):
                try:
                    # Try to parse the folder name as a datetime
                    ddt = dt.datetime.strptime(
                        dir.as_posix(),
                        internal.IT_FOLDER_FMTSTR,
                    ).replace(tzinfo=dt.UTC)
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

    def copyITFolderToTemp(self, *, prefix: pathlib.Path, it: dt.datetime) -> list[pathlib.Path]:  # noqa: D102
        initTimeDirPath = self.datasetPath / prefix / it.strftime(internal.IT_FOLDER_FMTSTR)
        paths = [pathlib.Path(p) for p in self.__fs.ls(path=initTimeDirPath.as_posix())]

        log.debug(
            event="copying it folder to temporary files",
            inittime=it.strftime(internal.IT_FOLDER_FMTSTR),
            numfiles=len(paths),
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
                    temppath=tfp.as_posix(),
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
            inittime=it.strftime("%Y-%m-%d %H:%M"),
        )

        return tempPaths

    def delete(self, *, p: pathlib.Path) -> None:  # noqa: D102
        if self.__fs.isdir(path=self.datasetPath / p.as_posix()):
            self.__fs.rm(path=self.datasetPath / p.as_posix(), recursive=True)
        else:
            self.__fs.rm(path=self.datasetPath / p.as_posix())
