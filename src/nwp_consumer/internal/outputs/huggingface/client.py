"""Client for HuggingFace."""

import datetime as dt
import pathlib

import huggingface_hub as hfh
import structlog
from huggingface_hub.hf_api import (
    RepoFile,
    RepoFolder,
    RevisionNotFoundError,
)

from nwp_consumer import internal

log = structlog.getLogger()


class Client(internal.StorageInterface):
    """Client for HuggingFace."""

    # HuggingFace API
    __api: hfh.HfApi

    # DatasetURL
    dsURL: str

    def __init__(self, repoID: str, token: str | None = None, endpoint: str | None = None) -> None:
        """Create a new client for HuggingFace.

        Exposes a client for the HuggingFace filesystem API that conforms to the StorageInterface.

        Args:
            repoID: The ID of the repo to use for the dataset.
            token: The HuggingFace authentication token.
            endpoint: The HuggingFace endpoint to use.
        """
        self.__api = hfh.HfApi(token=token, endpoint=endpoint)
        # Get the URL to the dataset, e.g. https://huggingface.co/datasets/username/dataset
        self.dsURL = hfh.hf_hub_url(
            endpoint=endpoint,
            repo_id=repoID,
            repo_type="dataset",
            filename="",
        )
        # Repo ID
        self.repoID = repoID

        try:
            self.__api.dataset_info(
                repo_id=repoID,
            )
        except Exception as e:
            log.warn(
                event="failed to authenticate with huggingface for given repo",
                repo_id=repoID,
                error=e,
            )

    def name(self) -> str:
        """Overrides the corresponding method of the parent class."""
        return "huggingface"

    def exists(self, *, dst: pathlib.Path) -> bool:
        """Overrides the corresponding method of the parent class."""
        try:
            path_infos: list[RepoFile | RepoFolder] = self.__api.get_paths_info(
                repo_id=self.repoID,
                repo_type="dataset",
                paths=[dst.as_posix()],
            )
            if len(path_infos) == 0:
                return False
        except RevisionNotFoundError:
            return False
        return True

    def store(self, *, src: pathlib.Path, dst: pathlib.Path) -> pathlib.Path:
        """Overrides the corresponding method of the parent class."""
        # Remove any leading slashes as they are not allowed in huggingface
        dst = dst.relative_to("/") if dst.is_absolute() else dst

        # Get the hash of the latest commit
        sha: str = self.__api.dataset_info(repo_id=self.repoID).sha
        # Handle the case where we are trying to upload a folder
        if src.is_dir():
            # Upload the folder using the huggingface API
            future = self.__api.upload_folder(
                repo_id=self.repoID,
                repo_type="dataset",
                folder_path=src.as_posix(),
                path_in_repo=dst.as_posix(),
                parent_commit=sha,
                run_as_future=True,
            )
        # Handle the case where we are trying to upload a file
        else:
            # Upload the file using the huggingface API
            future = self.__api.upload_file(
                repo_id=self.repoID,
                repo_type="dataset",
                path_or_fileobj=src.as_posix(),
                path_in_repo=dst.as_posix(),
                parent_commit=sha,
                run_as_future=True,
            )

        # Block until the upload is complete to prevent overlapping commits
        url = future.result(timeout=120)
        log.info("Uploaded to huggingface", commiturl=url)

        # Perform a check on the size of the file
        size = self._get_size(p=dst)
        if size != src.stat().st_size and future.done():
            log.warn(
                event="stored file size does not match source file size",
                src=src.as_posix(),
                dst=dst.as_posix(),
                srcsize=src.stat().st_size,
                dstsize=size,
            )
        else:
            log.debug(
                event=f"stored file {dst.name}",
                filepath=dst.as_posix(),
                nbytes=size,
            )
        return dst

    def listInitTimes(self, *, prefix: pathlib.Path) -> list[dt.datetime]:
        """Overrides the corresponding method of the parent class."""
        # Remove any leading slashes as they are not allowed in huggingface
        prefix = prefix.relative_to("/") if prefix.is_absolute() else prefix
        # Get the path relative to the prefix of every folder in the repo
        allDirs: list[pathlib.Path] = [
            pathlib.Path(f.path).relative_to(prefix)
            for f in self.__api.list_repo_tree(
                repo_id=self.repoID,
                repo_type="dataset",
                path_in_repo=prefix.as_posix(),
                recursive=True,
            )
            if isinstance(f, RepoFolder)
        ]

        # Get the initTime from the folder pattern
        initTimes = set()
        for d in allDirs:
            if d.match(internal.IT_FOLDER_GLOBSTR):
                try:
                    # Try to parse the folder name as a datetime
                    ddt = dt.datetime.strptime(
                        d.as_posix(),
                        internal.IT_FOLDER_FMTSTR,
                    ).replace(tzinfo=dt.UTC)
                    initTimes.add(ddt)
                except ValueError:
                    log.debug(
                        event="ignoring invalid folder name",
                        name=d.as_posix(),
                        within=prefix.as_posix(),
                    )

        sortedInitTimes = sorted(initTimes)
        log.debug(
            event=f"found {len(initTimes)} init times in raw directory",
            earliest=sortedInitTimes[0],
            latest=sortedInitTimes[-1],
        )
        return sortedInitTimes

    def copyITFolderToTemp(self, *, prefix: pathlib.Path, it: dt.datetime) -> list[pathlib.Path]:
        """Overrides the corresponding method of the parent class."""
        # Remove any leading slashes as they are not allowed in huggingface
        prefix = prefix.relative_to("/") if prefix.is_absolute() else prefix

        # Get the paths of all files in the folder
        paths: list[pathlib.Path] = [
            pathlib.Path(p.path)
            for p in self.__api.list_repo_tree(
                repo_id=self.repoID,
                repo_type="dataset",
                path_in_repo=(prefix / it.strftime(internal.IT_FOLDER_FMTSTR)).as_posix(),
                recursive=True,
            )
            if isinstance(p, RepoFile)
        ]

        log.debug(
            event="copying it folder to temporary files",
            inittime=it.strftime(internal.IT_FOLDER_FMTSTR),
            numfiles=len(paths),
        )

        # Read all files into temporary files
        tempPaths: list[pathlib.Path] = []
        for path in paths:
            # Huggingface replicates the full path from repo root on download
            # to local directory.
            tfp: pathlib.Path = internal.TMP_DIR / path.as_posix()

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
            if self.exists(dst=path) is False:
                log.warn(
                    event="file does not exist in store, skipping",
                    filepath=path.as_posix(),
                )
                continue

            # Copy the file from the store to a temporary file
            self.__api.hf_hub_download(
                repo_id=self.repoID,
                repo_type="dataset",
                filename=path.as_posix(),
                local_dir=internal.TMP_DIR.as_posix(),
                local_dir_use_symlinks=False,
            )

            # Check that the file was copied correctly
            if tfp.stat().st_size != self._get_size(p=path) or tfp.stat().st_size == 0:
                log.warn(
                    event="copied file size does not match source file size",
                    src=path.as_posix(),
                    dst=tfp.as_posix(),
                    srcsize=self._get_size(p=path),
                    dstsize=tfp.stat().st_size,
                )
            else:
                tempPaths.append(tfp)

        log.debug(
            event="copied it folder to temporary files",
            nbytes=[p.stat().st_size for p in tempPaths],
            inittime=it.strftime("%Y-%m-%d %H:%M"),
        )

        return tempPaths

    def delete(self, *, p: pathlib.Path) -> None:
        """Overrides the corresponding method of the parent class."""
        # Remove any leading slashes as they are not allowed in huggingface
        p = p.relative_to("/") if p.is_absolute() else p

        # Determine if the path corresponds to a file or a folder
        info: RepoFile | RepoFolder = self.__api.get_paths_info(
            repo_id=self.repoID,
            repo_type="dataset",
            paths=[p.as_posix()],
            recursive=False,
        )[0]
        # Call the relevant delete function using the huggingface API
        if isinstance(info, RepoFolder):
            self.__api.delete_folder(
                repo_id=self.repoID,
                repo_type="dataset",
                path_in_repo=p.as_posix(),
            )
        else:
            self.__api.delete_file(
                repo_id=self.repoID,
                repo_type="dataset",
                path_in_repo=p.as_posix(),
            )

    def _get_size(self, *, p: pathlib.Path) -> int:
        """Gets the size of a file or folder in the huggingface dataset."""
        # Remove any leading slashes as they are not allowed in huggingface
        p = p.relative_to("/") if p.is_absolute() else p

        size: int = 0
        # Get the info of the path
        path_info: RepoFile | RepoFolder = self.__api.get_paths_info(
            repo_id=self.repoID,
            repo_type="dataset",
            paths=[p.as_posix()],
        )

        if len(path_info) == 0:
            # The path in question doesn't exist
            log.warn(
                event="path does not exist in huggingface dataset",
                path=p.as_posix(),
            )
            return size

        # Calculate the size of the file or folder
        if isinstance(path_info[0], RepoFolder):
            size = sum(
                [
                    f.size
                    for f in self.__api.list_repo_tree(
                        repo_id=self.repoID,
                        repo_type="dataset",
                        path_in_repo=p.as_posix(),
                        recursive=True,
                    )
                    if isinstance(f, RepoFile)
                ],
            )
        elif isinstance(path_info[0], RepoFile):
            size = path_info[0].size

        return size
