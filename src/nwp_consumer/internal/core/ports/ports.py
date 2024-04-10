"""Defines interfaces used by actors."""

import abc
import datetime as dt
import pathlib

from ..domain import RawFileMetadata, RawRepositoryMetadata

class NWPConsumerService(abc.ABC):
    """Interface for a service that consumes NWP data."""

    @abc.abstractmethod()
    def consume(self, it: dt.datetime, zarr_filename: str) -> pathlib.Path:
        """Consume NWP data to Zarr format for desired init time."""
        pass

class RawRepository(abc.ABC):
    """Interface for a repository that stores raw NWP data."""

    @abc.abstractmethod()
    def metadata(self) -> RawRepositoryMetadata:
        """Get metadata about the raw repository."""
        pass

    @abc.abstractmethod()
    def download(self, it: dt.datetime, parameters: list[str], steps: list[int], area: str) -> list[RawFileMetadata]:
        """Download NWP data for a given init time, parameters, steps, and area."""
        pass

    @abc.abstractmethod()
    def process(self, list[pathlib.Path], zarr_filename: str) -> pathlib.Path:
        """Process NWP data into a single zipped zarr file."""
        pass

class ZarrRepository(abc.ABC):
    """Interface for a repository that stores Zarr NWP data."""

    @abc.abstractmethod()
    def store(self, zarr_filename: str) -> str:
        """Store NWP data in the repository."""
        pass


