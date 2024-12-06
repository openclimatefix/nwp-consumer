"""Implementation of the NWP consumer services."""

import dataclasses
import logging
import os
import pathlib
from typing import TYPE_CHECKING, override

from joblib import Parallel, cpu_count
from returns.result import Failure, ResultE, Success

from nwp_consumer.internal import entities, ports

if TYPE_CHECKING:
    import datetime as dt

log = logging.getLogger("nwp-consumer")


class ArchiverService(ports.ArchiveUseCase):
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
    def archive(self, year: int, month: int) -> ResultE[str]:
        monitor = entities.PerformanceMonitor()
        with monitor:

            init_times = self.mr.repository().month_its(year=year, month=month)

            # Create a store for the archive
            init_store_result: ResultE[entities.TensorStore] = \
                entities.TensorStore.initialize_empty_store(
                    model=self.mr.model().name,
                    repository=self.mr.repository().name,
                    coords=dataclasses.replace(
                        self.mr.model().expected_coordinates,
                        init_time=init_times,
                    ),
                )

            if isinstance(init_store_result, Failure):
                    return Failure(OSError(
                        f"Failed to initialize store for {year}-{month}: {init_store_result!s}"),
                    )
            store = init_store_result.unwrap()

            missing_times_result = store.missing_times()
            if isinstance(missing_times_result, Failure):
                return Failure(missing_times_result.failure())
            log.info(f"{len(missing_times_result.unwrap())} missing init_times in store.")

            failed_times: list[dt.datetime] = []
            for n, it in enumerate(missing_times_result.unwrap()):
                log.info(
                    f"Consuming data from {self.mr.repository().name} for {it:%Y-%m-%d %H:%M} "
                    f"(time {n + 1}/{len(missing_times_result.unwrap())})",
                )

                # Authenticate with the model repository
                amr_result = self.mr.authenticate()
                if isinstance(amr_result, Failure):
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
                for fetch_result in fetch_result_generator:
                    if isinstance(fetch_result, Failure):
                        log.error(
                            f"Error fetching data for init time '{it:%Y-%m-%d %H:%M}' "
                            f"and model {self.mr.model().name}: {fetch_result.failure()!s}",
                        )
                        failed_times.append(it)
                        continue
                    for da in fetch_result.unwrap():
                        write_result = store.write_to_region(da)
                        # Fail soft if a region fails to write
                        if isinstance(write_result, Failure):
                            log.error(f"Failed to write time {it:%Y-%m-%d %H:%M}: {write_result}")
                            failed_times.append(it)

                del fetch_result_generator

            # Add the failed times to the store's metadata
            store.update_attrs({
                "failed_times": ", ".join([t.strftime("Day %d %H:%M") for t in failed_times]),
            })

            if len(failed_times) == len(missing_times_result.unwrap()):
                store.delete_store()
                return Failure(OSError(
                    "Failed to write any regions for all init times. "
                    "Check error logs for details.",
                ))

            # Postprocess the dataset as required
            # postprocess_result = store.postprocess(self._mr.metadata().postprocess_options)
            # if isinstance(postprocess_result, Failure):
            #    return Failure(postprocess_result.failure())

        notify_result = self.nr().notify(
            message=entities.StoreCreatedNotification(
                filename=pathlib.Path(store.path).name,
                size_mb=store.size_kb // 1024,
                performance=entities.PerformanceMetadata(
                    duration_seconds=monitor.get_runtime(),
                    memory_mb=monitor.max_memory_mb(),
                ),
            ),
        )
        if isinstance(notify_result, Failure):
            return Failure(OSError(
                "Failed to notify of store creation: "
                f"{notify_result.failure()}",
            ))

        return Success(store.path)

