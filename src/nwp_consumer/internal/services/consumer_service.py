"""Implementation of the NWP consumer service."""

import dataclasses
import datetime as dt
import functools
import logging
import os
import pathlib
from collections.abc import Callable, Iterator
from typing import override

import xarray as xr
from joblib import Parallel, cpu_count
from returns.methods import partition
from returns.pipeline import flow
from returns.result import Failure, ResultE, Success

from nwp_consumer.internal import entities, ports

log = logging.getLogger("nwp-consumer")

class ConsumerService(ports.ConsumeUseCase):
    """Service implementation for the NWP Consumer.

    Defines the business-critical methods and logic.
    """

    mr: ports.RawRepository
    nr: ports.NotificationRepository

    def __init__(
            self,
            model_repository: ports.RawRepository,
            notification_repository: ports.NotificationRepository,
        ) -> None:
        """Create a new instance of the service."""
        self.mr = model_repository
        self.nr = notification_repository


    @classmethod
    def from_adaptors(
            cls,
            model_adaptor: type[ports.RawRepository],
            notification_adaptor: type[ports.NotificationRepository],
        ) -> ResultE["ConsumerService"]:
        """Create a new instance of the service from adaptors."""
        notification_repository = notification_adaptor()
        model_repository_result = model_adaptor.authenticate()
        return model_repository_result.do(
            cls(
                model_repository=model_repository,
                notification_repository=notification_repository,
            )
            for model_repository in model_repository_result
        )

    @staticmethod
    def _fold_dataarrays_generator(
            generator: Iterator[ResultE[list[xr.DataArray]]],
            store: entities.TensorStore,
        ) -> ResultE[int]:
        """Process data from data generator.

        Args:
            generator: A generator of ResultE objects containing either a list data arrays
                or a Failure object.
            store: The store to write the data to.

        Returns:
            A ResultE object containing the sum of the write results or a Failure object.
        """
        results: list[ResultE[int]] = []
        for value in generator:
            if isinstance(value, Failure):
                results.extend([value])
            else:
                results.extend([store.write_to_region(da=da) for da in value.unwrap()])
        successes, failures = partition(results)
        # TODO: Define the failure threshold for number of write attempts properly
        log.info(f"Processed {len(successes)} DataArrays successfully with {len(failures)} errors.")
        if len(failures) > 0:
            for i, exc in enumerate(failures):
                if i < 5:
                    log.error(str(exc))
                else:
                    break
            return Failure(OSError(
                "Error threshold exceeded: "
                f"{len(failures)} errors (>0) occurred during processing.",
            ))
        else:
            return Success(sum(successes))

    @staticmethod
    def _parallelize_generator[T](
            delayed_generator: Iterator[Callable[..., T]],
            max_connections: int,
    ) -> Iterator[T]:
        """Parallelize a generator of delayed functions.

        Args:
            delayed_generator: An iterable of delayed items.
                The creation of these items must be delayed, either via joblib.delayed
                or functools.partial, so they can be executed lazily.
            max_connections: The maximum number of connections to use.
        """
        # TODO: Change this based on threads instead of CPU count
        # TODO: Enable choosing between threads and processes?
        n_jobs: int = max(cpu_count() - 1, max_connections)
        prefer = "threads"

        if os.getenv("CONCURRENCY", "True").capitalize() == "False":
            n_jobs = 1

        log.debug(f"Using {n_jobs} concurrent {prefer}")

        return Parallel(  # type: ignore
            n_jobs=n_jobs,
            prefer=prefer,
            verbose=0,
            return_as="generator_unordered",
        )(delayed_generator)

    @staticmethod
    def _create_suitable_store(
            repository_metadata: entities.RawRepositoryMetadata,
            model_metadata: entities.ModelMetadata,
            period: dt.datetime | dt.date | None = None,
        ) -> ResultE[entities.TensorStore]:
        """Create a store for the data with the relevant init time coordinates.

        Args:
            repository_metadata: The metadata for the repository.
            model_metadata: The metadata for the model.
            period: The period for which to gather init time data.
        """
        its: list[dt.datetime] = []
        match period:
            case _ if period is None:
                its = [repository_metadata.determine_latest_it_from(dt.datetime.now(tz=dt.UTC))]
            case single_it if isinstance(period, dt.datetime):
                its = [single_it] # type: ignore
            case multiple_its if isinstance(period, dt.date):
                its = repository_metadata.month_its(
                    year=multiple_its.year,
                    month=multiple_its.month,
                )

        # Create a store for the data with the relevant init time coordinates
        return entities.TensorStore.initialize_empty_store(
            model=model_metadata.name,
            repository=repository_metadata.name,
            coords=dataclasses.replace(
                model_metadata.expected_coordinates,
                init_time=its,
            ),
            chunks=model_metadata.expected_coordinates.chunking(
                chunk_count_overrides=model_metadata.chunk_count_overrides,
            ),
        )

    @override
    def consume(
            self,
            period: dt.datetime | dt.date | None = None,
        ) -> ResultE[str]:
        """Consume NWP data to Zarr format for desired time period.

        Where possible the implementation should be as memory-efficient as possible.
        The designs of the repository methods also enable parallel processing within
        the implementation.

        Args:
            period: The period for which to gather init time data.

        Returns:
            The path to the produced Zarr store.

        See Also:
            - `repositories.RawRepository.fetch_init_data`
            - `tensorstore.TensorStore.write_to_region`
            - https://joblib.readthedocs.io/en/stable/auto_examples/parallel_generator.html
        """
        monitor = entities.PerformanceMonitor()
        with monitor:
            init_store_result = self._create_suitable_store(
                repository_metadata=self.mr.repository(),
                model_metadata=self.mr.model(),
                period=period,
            )
            if isinstance(init_store_result, Failure):
                return Failure(OSError(
                    f"Failed to initialize store for init time: {init_store_result!s}",
                ))
            store = init_store_result.unwrap()

            missing_times_result = store.missing_times()
            if isinstance(missing_times_result, Failure):
                return missing_times_result

            for n, it in enumerate(missing_times_result.unwrap()):
                log.info(
                    f"Consuming data from {self.mr.repository().name} for {it:%Y-%m-%d %H:%M} "
                    f"(time {n + 1}/{len(missing_times_result.unwrap())})",
                )
                process_result = flow(
                    self._parallelize_generator(
                        self.mr.fetch_init_data(it),
                        max_connections=self.mr.repository().max_connections,
                    ),
                    functools.partial(self._fold_dataarrays_generator, store=store),
                )
                if isinstance(process_result, Failure):
                    return process_result

        notification_message = entities.StoreCreatedNotification(
            filename=pathlib.Path(store.path).name,
            size_mb=store.size_kb // 1024,
            performance=entities.PerformanceMetadata(
                duration_seconds=monitor.get_runtime(),
                memory_mb=monitor.max_memory_mb(),
            ),
        )
        notify_result = self.nr.notify(message=notification_message)
        if isinstance(notify_result, Failure):
            log.error(
                "Failed to notify of store creation: "
                f"{notify_result.failure()}. "
                f"Notification: {notification_message}",
            )

        log.info(f"Successfully processed data to '{store.path}'")
        return Success(store.path)

    @override
    def archive(self, period: dt.date) -> ResultE[str]:
        return self.consume(period=period)

    @staticmethod
    def info(
            model_adaptor: type[ports.RawRepository],
            notification_adaptor: type[ports.NotificationRepository],
        ) -> str:
        """Get information about the service."""
        raise NotImplementedError("Not yet implemented")

