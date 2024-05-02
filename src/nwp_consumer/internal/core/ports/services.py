"""Defines interfaces used by actors."""

import abc
import pathlib
from typing import Literal

from nwp_consumer.internal.core import domain
from result import Result


class NWPConsumerService(abc.ABC):
    """Interface for a service that consumes NWP data.

    Defines the business-critical methods that an NWP service must implement.
    """

    @abc.abstractmethod
    def consume(
        self,
        source: str,
        request: domain.DataRequest,
    ) -> Result[pathlib.Path, str]:
        """Consume NWP data to Zarr format for desired init time."""
        pass

    @abc.abstractmethod
    def postprocess(
        self,
        options: domain.PostProcessOptions,
    ) -> Result[str, str]:
        """Postprocess the produced Zarr according to given options."""
        pass

    @abc.abstractmethod
    def append_to_archive(
        self,
        archive_period: Literal["yearly", "monthly"],
    ) -> Result[str, str]:
        """Append the Zarr archive to a larger archive."""
        pass
