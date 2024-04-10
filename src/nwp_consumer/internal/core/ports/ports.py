"""Defines interfaces used by actors."""

import abc
import datetime as dt
import pathlib
from result import Result

from ..domain import SourceFileMetadata, ProducerRepositoryMetadata


class NWPConsumerService(abc.ABC):
    """Interface for a service that consumes NWP data.

    Defines the business-critiacl methods that an NWP service must implement.
    """

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

class SourceRepository(abc.ABC):
    """Interface for a repository that produces raw NWP data.

    Since different producers of NWP data have different data storage implementations,
    a SourceRepository needs to define its own download and processing methods.

    A source may provide one or more files for a given init time.
    To keep memory usage at a minimum, the data from each file is persisted to disk on conversion.
    In this manner, writes can be done in parallel.

    As such, an important distinction is made between:
        - the *fileset*: Raw store data for an init time
        - the *store*: The Zarr store containing the processed data
    """

    @abc.abstractmethod
    def metadata(self) -> ProducerRepositoryMetadata:
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
    def list_fileset(
        self, it: dt.datetime, parameters: list[str], steps: list[int], area: str,
    ) -> Result[list[SourceFileMetadata], str]:
        """List available NWP files for a given init time, parameters, steps, and area."""
        pass

    @abc.abstractmethod
    def download_file(self, file: SourceFileMetadata) -> Result[SourceFileMetadata, str]:
        """Download a single source NWP file."""
        pass

    @abc.abstractmethod
    def map_file(
        self, cached_file: SourceFileMetadata, store_path: pathlib.Path,
    ) -> Result[SourceFileMetadata, str]:
        """Process cached source NWP data, persisting into the store file."""
        pass

class ZarrRepository(abc.ABC):
    """Interface for a repository that stores Zarr NWP data."""

    @abc.abstractmethod
    def save(self, src: pathlib.Path, dst: pathlib.Path) -> Result[str, str]:
        """Save NWP store data in the repository."""
        pass

class CacheRepository(abc.ABC):
    """Interface for a repository that manages cache."""

    @abc.abstractmethod
    def create(self, filename: str) -> b


