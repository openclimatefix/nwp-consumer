import abc
import datetime as dt
import pathlib

from result import Result

from nwp_consumer.internal.core import domain


class SourceRepository(abc.ABC):
    """Interface for a repository that produces raw NWP data.

    Class methods are used to distinguish between calls that require authentication
    and those that do not.

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

    @classmethod
    @abc.abstractmethod
    def from_env(cls) -> "SourceRepository":
        """Create a class instance, configuring from environment variables."""
        pass

    @classmethod
    @abc.abstractmethod
    def metadata(cls) -> domain.SourceRepositoryMetadata:
        """Get metadata about the raw repository."""
        pass

    @classmethod
    @abc.abstractmethod
    def map_file(
        cls,
        cached_file: domain.SourceFileMetadata,
        store_path: pathlib.Path,
    ) -> Result[domain.SourceFileMetadata, str]:
        """Process cached source NWP data, persisting into the store file."""
        pass

    @abc.abstractmethod
    def validate_request(
        self,
        request: domain.DataRequest,
    ) -> Result[domain.DataRequest, str]:
        """Validate requested data is available from source."""
        pass

    @abc.abstractmethod
    def list_fileset(
        self,
        it: dt.datetime,
        request: domain.DataRequest,
    ) -> Result[list[domain.SourceFileMetadata], str]:
        """List available NWP files for a given init time, parameters, steps, and area."""
        pass

    @abc.abstractmethod
    def download_file(
        self,
        file: domain.SourceFileMetadata,
    ) -> Result[domain.SourceFileMetadata, str]:
        """Download a single source NWP file."""
        pass



class ZarrRepository(abc.ABC):
    """Interface for a repository that stores Zarr NWP data."""

    @abc.abstractmethod
    def save(self, src: pathlib.Path, dst: pathlib.Path) -> Result[str, str]:
        """Save NWP store data in the repository."""
        pass
