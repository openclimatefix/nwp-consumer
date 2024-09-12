"""Implementation of the NWP consumer services."""

import dataclasses
import datetime as dt
import logging
import pathlib
from typing import override

from joblib import Parallel
from returns.result import Failure, ResultE, Success

from nwp_consumer.internal import entities, ports

log = logging.getLogger("nwp-consumer")


class ConsumerService(ports.ConsumeUseCase):
    """Service implementation of the consumer use case.

    This services contains the business logic required to enact
    the consumer use case. It is responsible for consuming NWP data
    and writing it to a Zarr store.
    """

    mr: type[ports.ModelRepository]
    nr: type[ports.NotificationRepository]

    def __init__(
        self,
        # TODO: 2024-10-21 - Work out how to pass none instantiated class values through DI
        model_repository: type[ports.ModelRepository],
        notification_repository: type[ports.NotificationRepository],
    ) -> None:
        """Create a new instance."""
        self.mr = model_repository
        self.nr = notification_repository

    @override
    def consume(self, it: dt.datetime | None = None) -> ResultE[pathlib.Path]:
        monitor = entities.PerformanceMonitor()

        if it is None:
            it = self.mr.repository().determine_latest_it_from(dt.datetime.now(tz=dt.UTC))
        log.info(
            f"Consuming data from repository '{self.mr.repository().name}' "
            f"for the '{self.mr.model().name}' model "
            f"spanning init time '{it:%Y-%m-%d %H:%M}'",
        )

        # Create a store for the init time
        init_store_result: ResultE[entities.TensorStore] = \
            entities.TensorStore.initialize_empty_store(
                name=self.mr.model().name,
                coords=dataclasses.replace(self.mr.model().expected_coordinates, init_time=[it]),
            )

        match init_store_result:
            case Failure(e):
                monitor.join()  # TODO: Make this a context manager instead
                return Failure(OSError(f"Failed to initialize store for init time: {e}"))
            case Success(store):

                # Create a generator to fetch and process raw data
                amr_result = self.mr.authenticate()
                if isinstance(amr_result, Failure):
                    monitor.join()
                    return Failure(OSError(
                        "Unable to authenticate with model repository "
                        f"'{self.mr.repository().name}': "
                        f"{amr_result.failure()}",
                    ))
                amr = amr_result.unwrap()

                fetch_result_generator = Parallel(
                    n_jobs=1, # TODO - fix segfault when using multiple threads
                    prefer="threads",
                    return_as="generator_unordered",
                )(amr.fetch_init_data(it=it))

                # Regionally write the results of the generator as they are ready
                failed_etls: int = 0
                for fetch_result in fetch_result_generator:
                    if isinstance(fetch_result, Failure):
                        log.error(
                            f"Error fetching data for init time '{it:%Y-%m-%d %H:%M}' "
                            f"and model {self.mr.repository().name}: {fetch_result.failure()!s}",
                        )
                        failed_etls += 1
                        continue
                    for da in fetch_result.unwrap():
                        write_result = store.write_to_region(da)
                        if isinstance(write_result, Failure):
                            log.error(
                                f"Error writing data for init time '{it:%Y-%m-%d %H:%M}' "
                                f"and model {self.mr.repository().name}: "
                                f"{write_result.failure()!s}",
                            )
                            failed_etls += 1

                del fetch_result_generator
                # Fail hard if any of the writes failed
                # * TODO: Consider just how hard we want to fail in this instance
                if failed_etls > 0:
                    monitor.join()
                    return Failure(OSError(
                        f"Failed to write {failed_etls} regions "
                        f"for init time '{it:%Y-%m-%d %H:%M}'. "
                        "See error logs for details.",
                    ))

                # Postprocess the dataset as required
                postprocess_result = store.postprocess(self.mr.repository().postprocess_options)
                if isinstance(postprocess_result, Failure):
                    monitor.join() # TODO: Make this a context manager instead
                    return Failure(postprocess_result.failure())

                monitor.join()
                notify_result = self.nr().notify(
                    message=entities.StoreCreatedNotification(
                        filename=store.path.name,
                        size_mb=store.size_mb,
                        performance=entities.PerformanceMetadata(
                            duration_seconds=monitor.get_runtime(),
                            memory_mb=max(monitor.memory_buffer) / 1e6,
                        ),
                    ),
                )
                if isinstance(notify_result, Failure):
                    return Failure(OSError(
                        "Failed to notify of store creation: "
                        f"{notify_result.failure()}",
                    ))

                return Success(store.path)

            case _:
                return Failure(
                    TypeError(f"Unexpected result type: {type(init_store_result)}"),
                )

    @override
    def postprocess(self, options: entities.PostProcessOptions) -> ResultE[str]:
        return Failure(NotImplementedError("Postprocessing not yet implemented"))
