"""Defines interfaces used by actors."""

import abc
import pathlib

from nwp_consumer.internal.core import domain
from result import Result


class NWPConsumerService(abc.ABC):
    """Interface for a service that consumes NWP data.

    Defines the business-critical methods that an NWP service must implement.
    """

    @abc.abstractmethod
    def initialize_archive(
        self,
        request: domain.DataRequest,
        archive_type: str,
    ) -> Result[str, str]:
        """Initialize a Zarr archive to store data for the given request."""
        pass

    @abc.abstractmethod
    def consume(
        self,
        source: str,
        request: domain.DataRequest,
    ) -> pathlib.Path:
        """Consume NWP data to Zarr format for desired init time."""
        pass

    @abc.abstractmethod
    def postprocess_archive(
        self,
        zarr_filename: str,
        request: domain.DataRequest,
    ) -> Result[str, str]:
        """Postprocess the Zarr archive to make it ready for use."""
        pass

