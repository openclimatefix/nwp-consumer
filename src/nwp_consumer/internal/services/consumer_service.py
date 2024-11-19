"""Implementation of the NWP consumer services."""

import dataclasses
import datetime as dt
import logging
import os
import pathlib
from typing import override

from joblib import Parallel, cpu_count
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
        model_repository: type[ports.ModelRepository],
        notification_repository: type[ports.NotificationRepository],
    ) -> None:
        """Create a new instance."""
        self.mr = model_repository
        self.nr = notification_repository

    @override
    def consume(self, it: dt.datetime | None = None) -> ResultE[str]:
        # Note that the usage of the returns here is not in the spirit of
        # 'railway orientated programming', mostly due to to the number of
        # generators involved - it seemed clearer to be explicit. However,
        # it would be much neater to refactor this to be more functional.
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
                model=self.mr.model().name,
                repository=self.mr.repository().name,
                coords=dataclasses.replace(self.mr.model().expected_coordinates, init_time=[it]),
            )

        if isinstance(init_store_result, Failure):
            monitor.join()  # TODO: Make this a context manager instead
            return Failure(OSError(
                f"Failed to initialize store for init time: {init_store_result!s}",
            ))
        store = init_store_result.unwrap()

        amr_result = self.mr.authenticate()
        if isinstance(amr_result, Failure):
            monitor.join()
            store.delete_store()
            return Failure(OSError(
                "Unable to authenticate with model repository "
                f"'{self.mr.repository().name}': "
                f"{amr_result.failure()}",
            ))
        amr = amr_result.unwrap()

        # Create a generator to fetch and process raw data
        n_jobs: int = max(cpu_count() - 1, self.mr.repository().max_connections)
        if os.getenv("CONCURRENCY", "True").capitalize() == "False":
            n_jobs = 1
        log.debug(f"Downloading using {n_jobs} concurrent thread(s)")
        fetch_result_generator = Parallel(
            n_jobs=n_jobs,
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
            store.delete_store()
            return Failure(OSError(
                f"Failed to write {failed_etls} regions "
                f"for init time '{it:%Y-%m-%d %H:%M}'. "
                "See error logs for details.",
            ))

        # Postprocess the dataset as required
        # postprocess_result = store.postprocess(self.mr.repository().postprocess_options)
        # if isinstance(postprocess_result, Failure):
        #     monitor.join() # TODO: Make this a context manager instead
        #     return Failure(postprocess_result.failure())

        monitor.join()
        notify_result = self.nr().notify(
            message=entities.StoreCreatedNotification(
                filename=pathlib.Path(store.path).name,
                size_mb=store.size_kb // 1024,  # TODO: 2024-11-19 check this is right
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

    @override
    def postprocess(self, options: entities.PostProcessOptions) -> ResultE[str]:
        return Failure(NotImplementedError("Postprocessing not yet implemented"))
