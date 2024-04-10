"""Defines interfaces used by actors."""

import abc
import datetime as dt
import pathlib

from ..domain import CachedFileMetadata, RawRepositoryMetadata


class NWPConsumerService(abc.ABC):
    """Interface for a service that consumes NWP data."""

    @abc.abstractmethod
    def consume(
        self,
        init_time: dt.datetime,
        zarr_filename: str,
        parameters: list[str],
        steps: list[int],
        area: str,
    ) -> pathlib.Path:
        """Consume NWP data to Zarr format for desired init time."""
        pass

class RawRepository(abc.ABC):
    """Interface for a repository that stores raw NWP data."""

    @abc.abstractmethod
    def metadata(self) -> RawRepositoryMetadata:
        """Get metadata about the raw repository."""
        pass

    @abc.abstractmethod
    def initialize_store(
        self,
        init_time: dt.datetime,
        parameters: list[str],
        steps: list[int],
    ) -> pathlib.Path:
        """Initialize an empty Zarr file for a given init time, parameters, and steps."""
        pass

    @abc.abstractmethod
    def download_to_cache(
        self, it: dt.datetime, parameters: list[str], steps: list[int], area: str,
    ) -> list[CachedFileMetadata]:
        """Download NWP data for a given init time, parameters, steps, and area to cache."""
        pass

    @abc.abstractmethod
    def process(self, cached_file: CachedFileMetadata, store_path: pathlib.Path) -> pathlib.Path:
        """Process NWP data into a single zipped zarr file."""
        pass

class ZarrRepository(abc.ABC):
    """Interface for a repository that stores Zarr NWP data."""

    @abc.abstractmethod
    def store(self, src: pathlib.Path, dst: pathlib.Path) -> str:
        """Store processed NWP data in the repository."""
        pass


