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


class ConsumerService(ports.ConsumerUseCase):
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
        """See parent class."""
        monitor = PerformanceMonitor()

        if it is None:
            it = self._mr.metadata.determine_latest_it_from(dt.datetime.now(tz=dt.UTC))
        log.info(f"Consuming data from {self._mr.metadata.name} for {it:%Y-%m-%d %H:%M}")

        # Create a store for the init time
        create_store_result: ResultE[TensorStore] = entities.TensorStore.initialize_empty_store(
            name=self._mr.metadata.name,
            coords=dataclasses.replace(self._mr.metadata.expected_coordinates, init_time=[it]),
        )

        match create_store_result:
            case Failure(e):
                return Result.from_failure(OSError(f"Failed to create store for init time: {e}"))
            case Success(tensor_store):
                # Get datasets from the model_repositories repository and write to their appropriate
                # regions in the store. Due to the blank dataset and region-based writing,
                # this can be done in parallel. See
                #
                # Note that increasing the parallelism increases the RAM usage.
                da_result_generator = Parallel(
                    n_jobs=self._mr.metadata.max_connections - 1,
                    prefer="threads",
                    return_as="generator_unordered",
                )(self._mr.fetch_init_data(it=it))
                # Handle the results of the generator as they are ready
                for da_result in da_result_generator:
                    write_result = da_result.bind(tensor_store.write_to_region)
                    # Fail hard if any of the writes failed
                    # * TODO: Consider just how hard we want to fail in this instance
                    if isinstance(write_result, Failure):
                        return Result.from_failure(write_result.failure())
                rechunk_result = tensor_store.rechunk()
                if isinstance(rechunk_result, Failure):
                    return Result.from_failure(rechunk_result.failure())

                del da_result_generator
                # TODO: Validation is very memory intensive
                # TODO: Possible to iterator over data array values?
                # validation_result = tensor_store.validate_store()
                # if isinstance(validation_result, Failure):
                #    log.error("Validation failed for store")
                #    return Result.from_failure(validation_result.failure())

                monitor.join()
                notify_result = self._nr.notify(
                    entities.StoreCreatedNotification(
                        filename=tensor_store.path.name,
                        size_mb=tensor_store.size_mb,
                        performance=entities.PerformanceMetadata(
                            duration_seconds=monitor.get_runtime(),
                            memory_mb=max(monitor.memory_buffer) / 1e6,
                        ),
                    ),
                )
                if isinstance(notify_result, Failure):
                    log.error("Failed to notify of store creation")
                    return notify_result

                return Result.from_value(tensor_store.path)

            case _:
                return Result.from_failure(
                    TypeError(f"Unexpected result type: {type(create_store_result)}"),
                )

    @override
    def postprocess(self, options: entities.PostProcessOptions) -> ResultE[str]:
        """See parent class."""
        return Result.from_failure(NotImplementedError("Postprocessing not yet implemented"))

    @override
    def info(self) -> str:
        """See parent class."""
        return str(self._mr.metadata)
