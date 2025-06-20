"""Repository interfaces for NWP data sources and stores.

These interfaces define the signatures that *driven* actors must conform to
in order to interact with the core.
Also sometimes referred to as *secondary ports*.

All NWP providers use some kind of model to generate their data. This model
can be physics-based, such as ERA5, or a machine learning model, such as
Google's GraphCast. The `ModelMetadata` object is used to abstract the
differences between these models, allowing the core to interact with them
in a uniform way, via the `RawRepository` interface.
"""

import abc
import datetime as dt
import logging
from collections.abc import Callable, Iterator

import xarray as xr
from returns.result import ResultE

from nwp_consumer.internal import entities

log = logging.getLogger("nwp-consumer")


class RawRepository(abc.ABC):
    """Interface for a repository that produces raw NWP data.

    Since different producers of NWP data have different data storage
    implementations, a RawRepository needs to define its own download
    and processing methods.

    A source may provide one or more files for a given init time.
    To keep memory usage at a minimum, when converting raw data to zarr,
    converted data is persisted to disk in a store.
    In this manner, writes can be done in parallel, but a schema needs to be known
    in advance.

    As such, an important distinction is made between:
        - the *fileset*: Raw store data for an init time
        - the *store*: The Zarr store containing the processed data
    """

    @classmethod
    @abc.abstractmethod
    def authenticate(cls) -> ResultE["RawRepository"]:
        """Create a new authenticated instance of the class."""
        pass

    @abc.abstractmethod
    def fetch_init_data(
        self, it: dt.datetime,
    ) -> Iterator[Callable[..., ResultE[list[xr.DataArray]]]]:
        """Fetch raw data files for an init time as xarray datasets.

        As per the typing, the return value is a generator of functions that
        may produce one or more xarray datasets.
        The generator-of-functions approach (typed here as ``Iterator[Callable...]``)
        is important, as it allows for lazy evaluation:
        by returning a generator of delayed objects, joblib can parallelize
        the download and the results can be accumulated in a low-memory fashion (see
        `the JobLib documentation on parallel generators
        <https://joblib.readthedocs.io/en/stable/auto_examples/parallel_generator.html>`_).

        An example psuedocode implementation is shown below:

        >>> from joblib import delayed
        >>> from returns.result import Result, ResultE
        >>> from typing import override
        >>> from collections.abc import Callable, Iterator
        >>> import xarray as xr
        >>> import datetime as dt
        >>>
        >>> # Pseudocode for a raw repository
        >>> class MyRawRepository(RawRepository):
        ...     @override
        ...     def fetch_init_data(self, it: dt.datetime) \
        ...             -> Iterator[Callable[..., ResultE[list[xr.DataArray]]]]:
        ...         for file in ["raw_file1.grib", "raw_file2.grib"]:
        ...             yield delayed(self._download_and_convert)(file)
        ...
        ...     def _download_and_convert(self, file: str) -> ResultE[list[xr.DataArray]]:
        ...         '''Download and convert a raw file to an xarray dataset.'''
        ...         return Success([xr.open_dataset(file).to_dataarray()])

        .. warning:: No downloading or processing should be done in this method*. All of that
          should be handled in the function that is yielded by the generator -
          ``_download_and_convert`` in the example above.
          This is to allow for parallelization of the download and processing.

        .. note:: It is however, worth considering the most efficient way to download and process
          the data. The above assumes that the data comes in many files, but there is a possibility
          of the case where the source provides one large file with many underlying datasets within.
          In this case, it may be more efficient to download the large file in the
          `fetch_init_data` method and then process the datasets within via the yielded functions.

        .. note:: For the moment, this returns a list of ``xarray.DataArray`` objects. It may be
          more efficient to return a generator here to avoid reading all the datasets into
          memory at once, however, often the source of these datasets is ``cfgrib.open_datasets``
          which has no option for returning a generator, hence the current choice of ``list``.
          This may be revisited in the future, for instance by recreating the ``open_datasets``
          function in a manner which returns a generator of datasets.

        Args:
            it: The initialization time for which to fetch data.

        Returns:
            A generator of delayed xarray dataarrays for the init time.
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def repository() -> entities.RawRepositoryMetadata:
        """Metadata about the model repository."""
        pass

    @staticmethod
    @abc.abstractmethod
    def model() -> entities.ModelMetadata:
        """Metadata about the model."""
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
