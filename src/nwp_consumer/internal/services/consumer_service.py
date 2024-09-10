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


class ConsumerService(ports.ConsumeUseCase):
    """Service implementation of the consumer use case.

    This services contains the business logic required to enact
    the consumer use case. It is responsible for consuming NWP data
    and writing it to a Zarr store.
    """

    _mr: ports.ModelRepository
    _zr: ports.ZarrRepository
    _nr: ports.NotificationRepository

    def __init__(
        self,
        model_repository: ports.ModelRepository,
        zarr_repository: ports.ZarrRepository,
        notification_repository: ports.NotificationRepository,
    ) -> None:
        """Create a new instance."""
        self._mr = model_repository
        self._zr = zarr_repository
        self._nr = notification_repository

    @override
    def consume(self, it: dt.datetime | None = None) -> ResultE[pathlib.Path]:
        monitor = PerformanceMonitor()

        if it is None:
            it = self._mr.metadata.determine_latest_it_from(dt.datetime.now(tz=dt.UTC))
        log.info(f"Consuming data from {self._mr.metadata.name} for {it:%Y-%m-%d %H:%M}")

        # Create a store for the init time
        init_store_result: ResultE[TensorStore] = entities.TensorStore.initialize_empty_store(
            name=self._mr.metadata.name,
            coords=dataclasses.replace(self._mr.metadata.expected_coordinates, init_time=[it]),
        )

        match init_store_result:
            case Failure(e):
                monitor.join()  # TODO: Make this a context manager instead
                return Result.from_failure(OSError(f"Failed to initialize store for init time: {e}"))
            case Success(store):

                # Create a generator to fetch and process raw data
                da_result_generator = Parallel(
                    n_jobs=self._mr.metadata.max_connections - 1,
                    prefer="threads",
                    return_as="generator_unordered",
                )(self._mr.fetch_init_data(it=it))

                # Regionally write the results of the generator as they are ready
                for da_result in da_result_generator:
                    write_result = da_result.bind(store.write_to_region)
                    # Fail hard if any of the writes failed
                    # * TODO: Consider just how hard we want to fail in this instance
                    if isinstance(write_result, Failure):
                        monitor.join() # TODO: Make this a context manager instead
                        return Result.from_failure(write_result.failure())

                del da_result_generator

                # Postprocess the dataset as required
                postprocess_result = store.postprocess(self._mr.metadata.postprocess_options)
                if isinstance(postprocess_result, Failure):
                    monitor.join() # TODO: Make this a context manager instead
                    return Result.from_failure(postprocess_result.failure())

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

    @override
    def postprocess(self, options: entities.PostProcessOptions) -> ResultE[str]:
        return Result.from_failure(NotImplementedError("Postprocessing not yet implemented"))
