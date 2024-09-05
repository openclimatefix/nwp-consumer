"""Repository interfaces for NWP data sources and stores.

These interfaces define the signatures that *driven* actors must conform to
in order to interact with the core. These interfaces include providers of
NWP data (`ModelRepository`) and stores for processed data (`ZarrRepository`).

Also sometimes referred to as *secondary ports*.

All NWP providers use some kind of model_repositories to generate their data. This model_repositories
can be physically based, such as ERA5, or a machine learning model_repositories, such as
Google's GraphCast. The `ModelRepository` interface is used to abstract the
differences between these models, allowing the core to interact with them
in a uniform way.
"""

import abc
import datetime as dt
import pathlib
from collections.abc import Callable, Iterator

import xarray as xr
from returns.result import ResultE

from nwp_consumer.internal import entities


class ModelRepository(abc.ABC):
    """Interface for a repository that produces raw NWP data.

    Since different producers of NWP data have different data storage
    implementations, a ModelRepository needs to define its own download
    and processing  methods.

    A source may provide one or more files for a given init time.
    To keep memory usage at a minimum, when converting raw data to zarr,
    converted data is persisted to disk in a store.
    In this manner, writes can be done in parallel, but a schema needs to be known
    in advance.

    As such, an important distinction is made between:
        - the *fileset*: Raw store data for an init time
        - the *store*: The Zarr store containing the processed data
    """

    @abc.abstractmethod
    def fetch_init_data(self, it: dt.datetime) -> Iterator[Callable[..., ResultE[xr.DataArray]]]:
        """Fetch raw data files for an init time as xarray datasets.

        As per the typing, the return value is a generator of functions that
        may produce one or more xarray datasets. This is done to allow for lazy evaluation:
        by returning a generator of delayed objects, joblib can parallelize
        the download and the results can be accumulated in a low-memory fashion (see
        `the JobLib documentation on parallel generators
        <https://joblib.readthedocs.io/en/stable/auto_examples/parallel_generator.html>`_).

        An example psuedocode implementation is shown below:

        >>> from joblib import Parallel, delayed
        >>> import xarray as xr
        >>> import nwp_consumer.internal.entities as entities
        >>> from returns.result import ResultE, Result
        >>> import datetime as dt
        >>>
        >>> # Pseudocode for a model_repositories repository
        >>> class MyModelRepository(entities.ModelRepository):
        ...     def fetch_init_data(self, it: dt.datetime) -> Iterator[Callable[..., ResultE[xr.DataArray]]]:
        ...         '''Overrides the abstract method.'''
        ...         for file in ["raw_file1.grib", "raw_file2.grib"]:
        ...             yield delayed(self._download_and_convert)(file)
        ...
        ...     def _download_and_convert(self, file: str) -> ResultE[xr.DataArray]:
        ...         '''Download and convert a raw file to an xarray dataset.'''
        ...         return Result.from_value(xr.open_dataset(file).to_dataarray())

        .. important:: No downloading or processing should be done in this method. All of that
          should be handled in the function that is yielded by the generator -
          ``_download_and_convert`` in the example above.
          This is to allow for parallelization of the download and processing.

        Args:
            it: The initialization time for which to fetch data.

        Returns:
            A generator of delayed xarray dataarrays for the init time.
        """
        pass


    @property
    @abc.abstractmethod
    def metadata(self) -> entities.ModelRepositoryMetadata:
        """Metadata about the model repository.

        See Also:
            - `entities.ModelRepositoryMetadata`.
        """
        pass


class ZarrRepository(abc.ABC):
    """Interface for a repository that stores Zarr NWP data."""

    @abc.abstractmethod
    def save(self, src: pathlib.Path, dst: pathlib.Path) -> ResultE[str]:
        """Save NWP store data in the repository."""
        pass


class NotificationRepository(abc.ABC):
    """Interface for a repository that sends notifications.

    Adaptors for this port enable sending notifications to
    a desired notification_repositories channel.
    """

    @abc.abstractmethod
    def notify(
            self,
            message: entities.StoreAppendedNotification | entities.StoreCreatedNotification,
    ) -> ResultE[str]:
        """Send a notification_repositories."""
        pass
