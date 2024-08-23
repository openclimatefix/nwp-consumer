import datetime as dt
import logging
import pathlib
from typing import TYPE_CHECKING
from multiprocessing import Pool, cpu_count

import numpy as np
from returns.result import Failure, Result, Success

from .. import domain, ports

if TYPE_CHECKING:
    import xarray as xr
    from xarray import Dataset

log = logging.getLogger("nwp-consumer")


class ParallelConsumer(ports.NWPConsumerService):
    """Consumer for NWP data that uses parallel processing."""

    def postprocess(self, options: domain.PostProcessOptions) -> Result[str, Exception]:
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

    def consume(self, it: dt.datetime) -> Result[pathlib.Path, Exception]:
        """Overrides the corresponding method in the parent class."""
        _start: dt.datetime = dt.datetime.now(tz=dt.UTC)

        # Create a store for the init time
        create_result = self.create_store_for_init_time(it=it)

        match create_result:
            case Failure(e):
                return Result.from_failure(OSError(f"Failed to create store for init time: {e}"))
            case Success(smd):
                # Get datasets from the model repository
                partial_datasets: list[xr.Dataset] = self._mr.fetch_init_data(it=it)

                # Write datasets to their appropriate regions in the store
                # * Due to the blank dataset and region-based writing,
                #   this can be done in parallel.
                pool = Pool(max(cpu_count(), 8))
                results: list[Result[int, Exception]] = pool.map(smd.write_to_region, partial_datasets)
                pool.close()
                pool.join()

                # Fail hard if any of the writes failed
                # * TODO: Consider just how hard we want to fail in this instance
                for result in results:
                    if result.is_failure:
                        return Result.from_failure(result.failure())

                notify_result = self._nr.notify(
                    domain.StoreCreatedNotification(
                        filename=smd.path.name,
                        size_kb=smd.size_kb,
                        performance=domain.PerformanceMetadata(
                            duration_seconds=(dt.datetime.now(tz=dt.UTC) - _start).total_seconds(),
                            memory_mb=0,  # TODO: Add memory usage
                        ),
                    ),
                )

                return Result.from_value(smd.path)

            case _:
                return Result.from_failure(
                    TypeError(f"Unexpected result type: {type(create_result)}")
                )

    def create_store_for_init_time(self, it: dt.datetime) -> Result[domain.StoreMetadata, Exception]:
        """Create a store for a given init time.

        This store is used to hold the processed data for a given init time.
        As such, no data is written by this function; only the metadata.
        The shape and coordinates of the store are determined by the model repository,
        and the 'init_time' coordinate is overwritten with the provided init time.

        Args:
            it: The init time value to specify as the coordinate
                of the 'init_time' dimension in the store.
        """
        store_path = pathlib.Path(
            f"~/.local/cache/nwp/{self._mr.metadata.name}/{it:%Y%m%d%H}.zarr",
        )
        store_coordinates = self._mr.metadata.expected_coordinates | {
            "init_time": [np.datetime64(it.replace(tzinfo=None), "ns")],
        }
        store_metadata: domain.StoreMetadata = domain.StoreMetadata(
            coordinate_map=store_coordinates,
            path=store_path,
            size_kb=0,
        )
        result = store_metadata.write_as_dummy_dataset()
        return result
