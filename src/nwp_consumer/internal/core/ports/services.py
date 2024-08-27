"""Service interfaces for consumer services.

These interfaces define the signatures that *driving* actors must conform to
in order to interact with the core.
"""

import abc
import datetime as dt
import pathlib

from returns.result import ResultE

from nwp_consumer.internal.core import domain


class NWPConsumerService(abc.ABC):
    """Interface for a service that consumes NWP data.

    Defines the business-critical methods that an NWP service must implement.
    """

    @abc.abstractmethod
    def consume(self, it: dt.datetime) -> ResultE[pathlib.Path]:
        """Consume NWP data to Zarr format for desired init time."""
        pass

    @abc.abstractmethod
    def postprocess(self, options: domain.PostProcessOptions) -> ResultE[str]:
        """Postprocess the produced Zarr according to given options."""
        pass
