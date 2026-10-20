"""Service interfaces for consumer services.

These interfaces define the signatures that *driving* actors must conform to
in order to interact with the core.

Sometimes referred to as *primary ports*.
"""

import abc
import datetime as dt

from returns.result import ResultE


class ConsumeUseCase(abc.ABC):
    """Interface for the consumer use case.

    Defines the business-critical methods for the following use cases:

    - 'A user should be able to consume NWP data for a given initialization time.'
    - 'A user should be able to remove unneeded data from the store for a given month.'
    """

    @abc.abstractmethod
    def consume(
        self,
        period: dt.datetime | dt.date | None = None,
    ) -> ResultE[str]:
        """Consume NWP data to the configured store for desired time period.

        Where possible the implementation should be as memory-efficient as possible.
        The designs of the repository methods also enable parallel processing within
        the implementation.

        Args:
            period: The period for which to gather init time data.

        Returns:
            The path to the store written to. Depending on the STORE_BACKEND, this is
            either a Zarr store holding only the init times of the period, or an
            Icechunk repository holding the whole month.

        See Also:
            - `repositories.RawRepository.fetch_init_data`
            - `tensorstore.TensorStore.write_to_region`
            - https://joblib.readthedocs.io/en/stable/auto_examples/parallel_generator.html
        """
        pass

    @abc.abstractmethod
    def archive(self, period: dt.date) -> ResultE[str]:
        """Archive NWP data to the configured store for desired time period.

        Args:
            period: The period for which to gather init time data.

        Returns:
            The path to the store written to. Depending on the STORE_BACKEND, this is
            either a Zarr store holding only the init times of the period, or an
            Icechunk repository holding the whole month.
        """
        pass

    @abc.abstractmethod
    def maintain(
        self,
        period: dt.date,
        expire_snapshots_older_than: dt.timedelta | None = None,
    ) -> ResultE[str]:
        """Delete unneeded objects from the monthly store for the given month.

        Args:
            period: The month of the store to maintain.
            expire_snapshots_older_than: If set, also expire the store's history
                older than this, so that objects only it references are deleted.

        Returns:
            The path to the maintained store.
        """
        pass
