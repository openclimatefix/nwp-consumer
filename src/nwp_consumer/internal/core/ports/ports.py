"""Defines interfaces used by actors."""

import abc
import datetime as dt
import pathlib

from result import Result

from ..domain import (
    DataRequest,
    ProducerRepositoryMetadata,
    SourceFileMetadata,
)


class NWPConsumerService(abc.ABC):
    """Interface for a service that consumes NWP data.

    Defines the business-critiacl methods that an NWP service must implement.
    """

    @abc.abstractmethod
    def consume(
        self,
        zarr_filename: str,
        request: DataRequest,
    ) -> pathlib.Path:
        """Consume NWP data to Zarr format for desired init time."""
        pass


class SourceRepository(abc.ABC):
    """Interface for a repository that produces raw NWP data.

    Since different producers of NWP data have different data storage implementations,
    a SourceRepository needs to define its own download and processing methods.

    A source may provide one or more files for a given init time.
    To keep memory usage at a minimum, when converting raw data to zarr,
    converted data is persisted to disk in a store.
    In this manner, writes can be done in parallel, but a schema needs to be known in advance.

    As such, an important distinction is made between:
        - the *fileset*: Raw store data for an init time
        - the *store*: The Zarr store containing the processed data
    """

    @abc.abstractmethod
    def metadata(self) -> ProducerRepositoryMetadata:
        """Get metadata about the raw repository."""
        pass

    @abc.abstractmethod
    def validate_request(
        self,
        request: DataRequest,
    ) -> Result[DataRequest, str]:
        """Validate requested data is available from source."""
        pass

    @abc.abstractmethod
    def initialize_store(
        self,
        request: DataRequest,
    ) -> Result[pathlib.Path, str]:
        """Initialize an empty Zarr file for a given request."""
        pass

    @abc.abstractmethod
    def list_fileset(
        self,
        it: dt.datetime,
        request: DataRequest,
    ) -> Result[list[SourceFileMetadata], str]:
        """List available NWP files for a given init time, parameters, steps, and area."""
        pass

    @abc.abstractmethod
    def download_file(
        self,
        file: SourceFileMetadata,
    ) -> Result[SourceFileMetadata, str]:
        """Download a single source NWP file."""
        pass

    @abc.abstractmethod
    def map_file(
        self,
        cached_file: SourceFileMetadata,
        store_path: pathlib.Path,
    ) -> Result[SourceFileMetadata, str]:
        """Process cached source NWP data, persisting into the store file."""
        pass


class ZarrRepository(abc.ABC):
    """Interface for a repository that stores Zarr NWP data."""

    @abc.abstractmethod
    def save(self, src: pathlib.Path, dst: pathlib.Path) -> Result[str, str]:
        """Save NWP store data in the repository."""
        pass
