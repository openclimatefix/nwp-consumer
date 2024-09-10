"""Implementation of the NWP consumer services."""

import dataclasses
import datetime as dt
import logging
import pathlib
from typing import TYPE_CHECKING, override

from joblib import Parallel
from returns.result import Failure, Result, ResultE, Success

from nwp_consumer.internal import entities, ports

from ._performance import PerformanceMonitor

if TYPE_CHECKING:
    from ..entities import TensorStore

log = logging.getLogger("nwp-consumer")


class ArchiverService(ports.ArchiveUseCase):
    """Service implementation of the consumer use case.

    This services contains the business logic required to enact
    the consumer use case. It is responsible for consuming NWP data
    and writing it to a Zarr store.
    """

    _mr: ports.ModelRepository
    _nr: ports.NotificationRepository

    def __init__(
        self,
        model_repository: ports.ModelRepository,
        notification_repository: ports.NotificationRepository,
    ) -> None:
        """Create a new instance."""
        self._mr = model_repository
        self._nr = notification_repository

    @override
    def archive(self, year: int, month: int) -> ResultE[pathlib.Path]:
        monitor = PerformanceMonitor()

        init_times = self._mr.metadata.month_its(year=year, month=month)

        # Create a store for the archive
        init_store_result: ResultE[TensorStore] = entities.TensorStore.initialize_empty_store(
            name=self._mr.metadata.name,
            coords=dataclasses.replace(
                self._mr.metadata.expected_coordinates,
                init_time=init_times,
            ),
        )

        match init_store_result:
            case Failure(e):
                monitor.join()  # TODO: Make this a context manager instead
                return Result.from_failure(OSError(
                    f"Failed to initialize store for {year}-{month}: {e}"),
                )
            case Success(store):
                failed_times: list[dt.datetime] = []
                for it in init_times:
                    log.info(
                        f"Consuming data from {self._mr.metadata.name} for {it:%Y-%m-%d %H:%M}",
                    )

                    # Create a generator to fetch and process raw data
                    da_result_generator = Parallel(
                        n_jobs=self._mr.metadata.max_connections - 1,
                        prefer="threads",
                        return_as="generator_unordered",
                    )(self._mr.fetch_init_data(it=it))

                    # Regionally write the results of the generator as they are ready
                    for da_result in da_result_generator:
                        write_result = da_result.bind(store.write_to_region)
                        # Fail soft if a region fails to write
                        if isinstance(write_result, Failure):
                            log.error(f"Failed to write time {it:%Y-%m-%d %H:%M}: {write_result}")
                            failed_times.append(it)

                    del da_result_generator

                monitor.join()
                notify_result = self._nr.notify(
                    entities.StoreCreatedNotification(
                        filename=store.path.name,
                        size_mb=store.size_mb,
                        performance=entities.PerformanceMetadata(
                            duration_seconds=monitor.get_runtime(),
                            memory_mb=max(monitor.memory_buffer) / 1e6,
                        ),
                    ),
                )
                if isinstance(notify_result, Failure):
                    log.error("Failed to notify of store creation")
                    return notify_result

                return Result.from_value(store.path)

            case _:
                return Result.from_failure(
                    TypeError(f"Unexpected result type: {type(init_store_result)}"),
                )
