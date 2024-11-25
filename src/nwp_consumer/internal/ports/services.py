"""Service interfaces for consumer services.

These interfaces define the signatures that *driving* actors must conform to
in order to interact with the core.

Also sometimes referred to as *primary ports*.
"""

import abc
import datetime as dt

from returns.result import ResultE

from nwp_consumer.internal import entities


class ConsumeUseCase(abc.ABC):
    """Interface for the consumer use case.

    Defines the business-critical methods for the following use cases:

    - 'A user should be able to consume NWP data for a given initialization time.'
    """


    @abc.abstractmethod
    def consume(self, it: dt.datetime | None = None) -> ResultE[str]:
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


class ArchiveUseCase(abc.ABC):
    """Interface for the archive use case.

    Defines the business-critical methods for the following use cases:

    - 'A user should be able to archive NWP data for a given time period.'
    """

    @abc.abstractmethod
    def archive(self, year: int, month: int) -> ResultE[str]:
        """Archive NWP data to Zarr format for the given month.

        Args:
            year: The year for which to archive data.
            month: The month for which to archive data.

        Returns:
            The path to the produced Zarr store.
        """
        pass

class InfoUseCase(abc.ABC):
    """Interface for the notification use case.

    Defines the business-critical methods for the following use cases:

    - 'A user should be able to retrieve information about the service.'
    """

    @abc.abstractmethod
    def available_models(self) -> list[str]:
        """Get a list of available models."""
        pass

    @abc.abstractmethod
    def model_repository_info(self) -> str:
        """Get information about the model repository."""
        pass

    @abc.abstractmethod
    def model_info(self) -> str:
        """Get information about the model."""
        pass
