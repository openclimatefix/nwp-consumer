import pathlib
from typing import Literal
import structlog

from result import Result

from .. import domain, ports

log = structlog.get_logger()


class GriddedConsumer(ports.NWPConsumerService):
    """Service for archiving NWP data implementing the NWPConsumerService interface."""

    _source_repository: ports.SourceRepository
    _zarr_repository: ports.ZarrRepository

    def __init__(
            self,
            src_repo: ports.SourceRepository,
            zarr_repo: ports.ZarrRepository,
    ) -> None:
        """Create a new instance."""
        self._source_repository = src_repo
        self._zarr_repository = zarr_repo

    def consume(self, request: domain.DataRequest) -> Result[pathlib.Path, str]:  # noqa: D102

        # Create the store for the processed data
        store = pathlib.Path("~/.local/cache/nwp") \
                / self._source_repository.metadata().name \
                / request.init_time.strftime("%Y%m%d%H.zarr")
        request.as_dataset(resolution_degrees=0.1).to_zarr(store, compute=False)

        # Get each file available to download
        files: Result[list[domain.SourceFileMetadata], str] = self._source_repository.list_fileset(
            it=request.init_time,
            request=request,
        )
        if isinstance(files, Result.Err):
            return files

        # In parallel, download each file and write it to the appropriate region in the zarr store
        for file in files.ok_value:
            cfile = self._source_repository.download_file(file=file)
            if isinstance(cfile, Result.Err):
                log.warn("Failed to download file", file=file.name, error=cfile.err_value)
                continue
            log.debug("Downloaded file", file=file.name)


        pass

    def append_to_archive(self, archive_period: Literal["yearly", "monthly"]) -> Result[str, str]:
        pass

    def postprocess(self, options: domain.PostProcessOptions) -> Result[str, str]:
        pass
