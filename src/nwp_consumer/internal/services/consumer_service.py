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
            A ResultE object containing the number of DataArrays that failed to be
            processed or written, or a Failure object if the proportion of them is too
            high or no DataArrays were produced at all.
        """
        results: list[ResultE[int]] = []
        for value in generator:
            if isinstance(value, Failure):
                results.extend([value])
            elif len(value.unwrap()) == 0:
                continue
            else:
                results.extend([store.write_to_region(da=da) for da in value.unwrap()])
        if len(results) == 0:
            return Failure(
                ValueError(
                    "No data was found for the init time in any of the raw files. "
                    "Ensure the raw files cover the model's expected coordinates.",
                ),
            )
        successes, failures = partition(results)
        # TODO: Define the failure threshold for number of write attempts properly
        # ECMWF Realtime for instance needs a bit of tolerance because of some files not containing
        # data for all geographic regions.
        log.info(f"Processed {len(successes)} DataArrays successfully with {len(failures)} errors.")
        if len(failures) / len(results) > 0.06:
            for i, exc in enumerate(failures):
                if i < 5:
                    log.error(str(exc))
                else:
                    break
            return Failure(
                OSError(
                    "Error threshold exceeded: "
                    f"{len(failures)/len(results)} errors (>6%) occurred during processing.",
                ),
            )
        else:
            return Success(len(failures))

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

        return Parallel(
            n_jobs=n_jobs,
            prefer=prefer,
            verbose=0,
            return_as="generator_unordered",
        )(delayed_generator)

    @staticmethod
    def _determine_init_times(
        repository_metadata: entities.RawRepositoryMetadata,
        model_metadata: entities.ModelMetadata,
        period: dt.datetime | dt.date | None = None,
    ) -> list[dt.datetime]:
        """Determine the init times requested by the given period.

        Args:
            repository_metadata: The metadata for the repository.
            model_metadata: The metadata for the model.
            period: The period for which to gather init time data.
        """
        its: list[dt.datetime] = []
        match period:
            case None:
                its = [
                    repository_metadata.determine_latest_it_from(
                        t=dt.datetime.now(tz=dt.UTC),
                        running_hours=model_metadata.running_hours,
                        delay_minutes=model_metadata.delay_minutes,
                    ),
                ]
            case dt.datetime() as single_it:
                its = [single_it]
            case dt.date() as multiple_its:
                its = model_metadata.month_its(
                    year=multiple_its.year,
                    month=multiple_its.month,
                )
        return [it.replace(tzinfo=dt.UTC) for it in its]

    @staticmethod
    def _store_class() -> ResultE[type[entities.TensorStore]]:
        """Get the store class for the backend chosen via STORE_BACKEND."""
        backend = os.getenv("STORE_BACKEND", "zarr").lower()
        if backend == "icechunk":
            return Success(entities.IcechunkTensorStore)
        elif backend == "zarr":
            return Success(entities.ZarrTensorStore)
        return Failure(
            ValueError(
                f"Unknown STORE_BACKEND '{backend}'. Expected 'zarr' or 'icechunk'.",
            ),
        )

    @staticmethod
    def _create_suitable_store(
        repository_metadata: entities.RawRepositoryMetadata,
        model_metadata: entities.ModelMetadata,
        its: list[dt.datetime],
    ) -> ResultE[entities.TensorStore]:
        """Create a store for the data with the relevant init time coordinates.

        Monthly stores cover every init time of the model in the month of the
        given init times, so that later runs can add to them. Other stores
        cover only the given init times.

        Args:
            repository_metadata: The metadata for the repository.
            model_metadata: The metadata for the model.
            its: The init times the store must cover.
        """
        store_cls_result = ConsumerService._store_class()
        if isinstance(store_cls_result, Failure):
            return store_cls_result
        store_cls = store_cls_result.unwrap()

        store_its = its
        if store_cls.monthly:
            store_its = model_metadata.month_its(year=its[0].year, month=its[0].month)
            if not set(its).issubset(store_its):
                return Failure(
                    ValueError(
                        f"Requested init times {its} are not all running times of model "
                        f"'{model_metadata.name}' in {its[0]:%Y-%m}. Monthly stores hold "
                        "one month of init times at the running hours "
                        f"{model_metadata.running_hours}.",
                    ),
                )

        # Create a store for the data with the relevant init time coordinates
        return store_cls.initialize_empty_store(
            model=model_metadata.name,
            repository=repository_metadata.name,
            coords=dataclasses.replace(
                model_metadata.expected_coordinates,
                init_time=store_its,
            ),
            chunks=model_metadata.expected_coordinates.chunking(
                chunk_count_overrides=model_metadata.chunk_count_overrides,
            ),
        )

    @staticmethod
    def _rollback_store(store: entities.TensorStore) -> None:
        """Undo the uncommitted writes to a store after a failure."""
        rollback_result = store.rollback()
        if isinstance(rollback_result, Failure):
            log.error(
                f"Failed to roll back store after error: {rollback_result}",
            )

    def _consume_init_time(
        self,
        store: entities.TensorStore,
        it: dt.datetime,
        delete_on_failure: bool,
    ) -> ResultE[None]:
        """Write and validate the data for a single init time in one transaction.

        The transaction is only committed if the data is written and validated
        successfully, so readers never see an incomplete init time.

        The init time is only marked as complete if none of its data failed to be
        written. Otherwise the data is committed without being marked, so later runs
        retry it.

        Args:
            store: The store to write the data to.
            it: The init time to consume.
            delete_on_failure: Whether to discard the data if it fails validation.
                If False, data failing validation is committed anyway.
        """
        begin_result = store.begin_transaction()
        if isinstance(begin_result, Failure):
            return begin_result

        process_result = flow(
            self._parallelize_generator(
                self.mr.fetch_init_data(it),
                max_connections=self.mr.repository().max_connections,
            ),
            functools.partial(self._fold_dataarrays_generator, store=store),
        )
        if isinstance(process_result, Failure):
            self._rollback_store(store)
            return process_result

        failed_writes: int = process_result.unwrap()

        validation_result = store.validate_store(init_times=[it])
        if isinstance(validation_result, Failure):
            if delete_on_failure:
                self._rollback_store(store)
                return validation_result
            log.warning(
                f"Committing data for {it:%Y-%m-%d %H:%M} despite failed validation, "
                f"without marking it complete: {validation_result.failure()}",
            )
        elif failed_writes > 0:
            log.warning(
                f"Committing data for {it:%Y-%m-%d %H:%M} without marking it complete, "
                f"as {failed_writes} DataArrays failed to be written. "
                "It will be retried by later runs.",
            )
        else:
            mark_result = store.mark_complete(init_times=[it])
            if isinstance(mark_result, Failure):
                self._rollback_store(store)
                return mark_result

        commit_result = store.commit(message=f"Write init time {it:%Y-%m-%dT%H:%M}")
        if isinstance(commit_result, Failure):
            self._rollback_store(store)
            return commit_result
        return Success(None)

    @override
    def consume(
        self,
        period: dt.datetime | dt.date | None = None,
        delete_on_failure: bool = True,
    ) -> ResultE[str]:
        """Consume NWP data to the configured store for desired time period.

        Where possible the implementation should be as memory-efficient as possible.
        The designs of the repository methods also enable parallel processing within
        the implementation.

        Each init time is written and validated in its own transaction, so on
        transactional stores readers never see an incomplete init time.

        Args:
            period: The period for which to gather init time data.
            delete_on_failure: Whether to discard the data of an init time that
                fails validation.

        Returns:
            The path to the store written to. Depending on the STORE_BACKEND, this is
            either a Zarr store holding only the init times of the period, or an
            Icechunk repository holding the whole month.

        See Also:
            - `repositories.RawRepository.fetch_init_data`
            - `tensorstore.TensorStore.write_to_region`
            - https://joblib.readthedocs.io/en/stable/auto_examples/parallel_generator.html
        """
        monitor = entities.PerformanceMonitor()
        with monitor:
            its = self._determine_init_times(
                repository_metadata=self.mr.repository(),
                model_metadata=self.mr.model(),
                period=period,
            )
            init_store_result = self._create_suitable_store(
                repository_metadata=self.mr.repository(),
                model_metadata=self.mr.model(),
                its=its,
            )
            if isinstance(init_store_result, Failure):
                return Failure(
                    OSError(
                        f"Failed to initialize store for init time: {init_store_result!s}",
                    ),
                )
            store = init_store_result.unwrap()

            missing_times_result = store.missing_times(init_times=its)
            if isinstance(missing_times_result, Failure):
                self._rollback_store(store)
                return missing_times_result
            missing_its = missing_times_result.unwrap()

            for n, it in enumerate(missing_its):
                log.info(
                    f"Consuming data from {self.mr.repository().name} for {it:%Y-%m-%d %H:%M} "
                    f"(time {n + 1}/{len(missing_its)})",
                )
                consume_result = self._consume_init_time(
                    store=store,
                    it=it,
                    delete_on_failure=delete_on_failure,
                )
                if isinstance(consume_result, Failure):
                    return consume_result

        # Stores that already existed, such as monthly stores, are appended to
        notification_cls: type[
            entities.StoreCreatedNotification | entities.StoreAppendedNotification
        ] = (
            entities.StoreCreatedNotification
            if store.created
            else entities.StoreAppendedNotification
        )
        notification_message = notification_cls(
            filename=pathlib.Path(store.path).name,
            init_times=missing_its,
            size_mb=store.size_kb // 1024,
            performance=entities.PerformanceMetadata(
                duration_seconds=monitor.get_runtime(),
                memory_mb=monitor.max_memory_mb(),
            ),
        )
        notify_result = self.nr.notify(message=notification_message)
        if isinstance(notify_result, Failure):
            log.error(
                "Failed to notify of store update: "
                f"{notify_result.failure()}. "
                f"Notification: {notification_message}",
            )

        log.info(f"Successfully processed data to '{store.path}'")
        return Success(store.path)

    @override
    def archive(self, period: dt.date) -> ResultE[str]:
        return self.consume(period=period)

    @override
    def maintain(
        self,
        period: dt.date,
        expire_snapshots_older_than: dt.timedelta | None = None,
    ) -> ResultE[str]:
        store_cls_result = self._store_class()
        if isinstance(store_cls_result, Failure):
            return store_cls_result
        if store_cls_result.unwrap() is not entities.IcechunkTensorStore:
            return Failure(
                ValueError(
                    "Maintenance is only supported for monthly stores. "
                    "Set STORE_BACKEND to 'icechunk'.",
                ),
            )
        return entities.IcechunkTensorStore.maintain(
            model=self.mr.model().name,
            repository=self.mr.repository().name,
            month=period,
            expire_snapshots_older_than=expire_snapshots_older_than,
        )

    @staticmethod
    def info(
        model_adaptor: type[ports.RawRepository],
        notification_adaptor: type[ports.NotificationRepository],
    ) -> str:
        """Get information about the service."""
        raise NotImplementedError("Not yet implemented")
