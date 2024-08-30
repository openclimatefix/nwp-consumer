"""Service interfaces for consumer services.

These interfaces define the signatures that *driving* actors must conform to
in order to interact with the core.

Also sometimes referred to as *primary ports*.
"""

import abc
import datetime as dt
import pathlib

from returns.result import ResultE

from nwp_consumer.internal import entities


class ConsumerUseCase(abc.ABC):
    """Interface for the consumer use case.

    Defines the business-critical methods for the following use case:

    'A user should be able to consume NWP data for a given initialization time.'
    """

    @abc.abstractmethod
    def info(self) -> str:
        """Return information about the model repository."""
        pass

    @abc.abstractmethod
    def consume(self, it: dt.datetime | None = None) -> ResultE[pathlib.Path]:
        """Consume NWP data to Zarr format for desired init time.

        Where possible the implementation should be as memory-efficient as possible.
        The designs of the repository methods also enable parallel processing within
        the implementation.

        Args:
            it: The initialization time for which to consume data.
                If None, the latest available forecast should be consumed.

        Returns:
            The path to the produced Zarr store.

        See Also:
            - `repositories.ModelRepository.fetch_init_data`
            - `tensorstore.TensorStore.write_to_region`
            - https://joblib.readthedocs.io/en/stable/auto_examples/parallel_generator.html
        """
        pass

    @abc.abstractmethod
    def postprocess(self, options: entities.PostProcessOptions) -> ResultE[str]:
        """Postprocess the produced Zarr according to given options."""
        pass
