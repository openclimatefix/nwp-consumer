"""Implementation of the NWP consumer service."""

import datetime as dt
import logging
import pathlib
from typing import TYPE_CHECKING

from joblib import Parallel
from returns.result import Failure, Result, ResultE, Success

from nwp_consumer.internal import entities, ports

from .memory import PerformanceMonitor

if TYPE_CHECKING:
    from ..entities import TensorStore

log = logging.getLogger("nwp-consumer")


class ParallelConsumer(ports.NWPConsumerService):
    """Consumer for NWP data that uses parallel processing."""

    def postprocess(self, options: entities.PostProcessOptions) -> ResultE[str]:
        """Overrides the corresponding method in the parent class."""
        return Result.from_failure(NotImplementedError("Postprocessing not yet implemented"))

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

    def consume(self, it: dt.datetime) -> ResultE[pathlib.Path]:
        """Overrides the corresponding method in the parent class."""
        monitor = PerformanceMonitor()

        # Create a store for the init time
        create_store_result: ResultE[TensorStore] = entities.TensorStore.initialize_empty_store(
            name=self._mr.metadata.name,
            coords=self._mr.metadata.expected_coordinates | {
                "init_time": [it],
            },
        )

        match create_store_result:
            case Failure(e):
                return Result.from_failure(OSError(f"Failed to create store for init time: {e}"))
            case Success(tensor_store):
                # Get datasets from the model repository and write to their appropriate
                # regions in the store. Due to the blank dataset and region-based writing,
                # this can be done in parallel. See
                #
                # Note that increasing the parallelism increases the RAM usage.
                result_generator = Parallel(
                    n_jobs=1,
                    prefer="threads",
                    return_as="generator_unordered",
                )(self._mr.fetch_init_data(it=it))
                # Handle the results of the generator as they are ready
                for ds in result_generator:
                    write_result = tensor_store.write_to_region(ds)
                    # Fail hard if any of the writes failed
                    # * TODO: Consider just how hard we want to fail in this instance
                    if isinstance(write_result, Failure):
                        return Result.from_failure(write_result.failure())

                del result_generator
                # TODO: Validation is very memory intensive
                # TODO: Possible to iterator over data array values?
                #validation_result = tensor_store.validate_store()
                #if isinstance(validation_result, Failure):
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
                    return notify_result.failure()

                return Result.from_value(tensor_store.path)

            case _:
                return Result.from_failure(
                    TypeError(f"Unexpected result type: {type(create_store_result)}"),
                )


