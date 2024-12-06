"""Implementation of the NWP consumer services."""

import dataclasses
import datetime as dt
import logging
import os
import pathlib
from typing import override

from joblib import Parallel, cpu_count
from returns.methods import partition
from returns.pipeline import flow
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
        with monitor:
            if it is None:
                it = self.mr.repository().determine_latest_it_from(dt.datetime.now(tz=dt.UTC))
            log.info(
                f"Consuming data from repository '{self.mr.repository().name}' "
                f"for the '{self.mr.model().name}' model "
                f"spanning init time '{it:%Y-%m-%d %H:%M}'",
            )
            it = it.replace(tzinfo=dt.UTC)

            # Create a store for the init time
            init_store_result: ResultE[entities.TensorStore] = \
                entities.TensorStore.initialize_empty_store(
                    model=self.mr.model().name,
                    repository=self.mr.repository().name,
                    coords=dataclasses.replace(
                        self.mr.model().expected_coordinates,
                        init_time=[it],
                    ),
                )

            if isinstance(init_store_result, Failure):
                return Failure(OSError(
                    f"Failed to initialize store for init time: {init_store_result!s}",
                ))
            store = init_store_result.unwrap()

            def accumulate_fetched_data(generator):
                results: list[ResultE[str]] = []
                for value in generator:
                    # Note that we can only use list comprehension in here because the type
                    # yielded by the generator is a list of dataarrays.
                    # It would be more memory efficient to use a generator here, but
                    # open_datasets returns a list.
                    write_results = value.do(
                        [store.write_to_region(da) for da in das]
                        for das in value
                    )
                    results.extend(
                        write_results
                        if isinstance(write_results, list)
                        else [write_results],
                    )
                return results

            n_jobs: int = max(cpu_count() - 1, self.mr.repository().max_connections)
            if os.getenv("CONCURRENCY", "True").capitalize() == "False":
                n_jobs = 1
            log.debug(f"Downloading using {n_jobs} concurrent thread(s)")

            with Parallel(
                n_jobs=n_jobs,
                prefer="threads",
                verbose=10,
                return_as="generator_unordered",
            ) as parallel:

                amr_result = self.mr.authenticate()
                if isinstance(amr_result, Failure):
                    store.delete_store()
                    return Failure(OSError(
                        "Unable to authenticate with model repository "
                        f"'{self.mr.repository().name}': "
                        f"{amr_result.failure()}",
                    ))
                amr = amr_result.unwrap()

                out = flow(
                    parallel(amr.fetch_init_data(it=it)),
                    accumulate_fetched_data,
                    partition,
                    lambda sf: Success(sf[0]) if len(sf[1]) == 0 else Failure(sf[1]),
                )

                # Fail hard if any of the writes failed
                # * TODO: Consider just how hard we want to fail in this instance
                if isinstance(write_result, Failure):
                    store.delete_store()
                    return Failure(OSError(
                        f"Failed to write all regions "
                        f"for init time '{it:%Y-%m-%d %H:%M}'. "
                        f"Error context: {write_result!s}",
                    ))

            # Postprocess the dataset as required
            # postprocess_result = store.postprocess(self.mr.repository().postprocess_options)
            # if isinstance(postprocess_result, Failure):
            #     return Failure(postprocess_result.failure())

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

    @override
    def postprocess(self, options: entities.PostProcessOptions) -> ResultE[str]:
        return Failure(NotImplementedError("Postprocessing not yet implemented"))


