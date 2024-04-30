import pathlib
from typing import Literal

from result import Result

from .. import domain, ports


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

    def consume(self, source: str, request: domain.DataRequest) -> pathlib.Path:

        # Get each file available to download
        files = self._source_repository.list_fileset(request.init_time, request)
        pass

    def append_to_archive(self, archive_period: Literal["yearly", "monthly"]) -> Result[str, str]:
        pass

    def postprocess(self, options: domain.PostProcessOptions) -> Result[str, str]:
        pass

