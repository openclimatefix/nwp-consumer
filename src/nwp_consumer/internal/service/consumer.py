"""Implementation of the NWP consumer service."""

import datetime as dt
import logging
import pathlib

import numpy as np
import xarray as xr
from joblib import Parallel
from returns.result import Failure, Result, ResultE, Success

from nwp_consumer.internal import entities, ports

from .memory import PerformanceMonitor

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
        create_result = self.create_store_for_init_time(it=it)

        match create_result:
            case Failure(e):
                return Result.from_failure(OSError(f"Failed to create store for init time: {e}"))
            case Success(smd):
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
                    write_result = smd.write_to_region(ds)
                    # Fail hard if any of the writes failed
                    # * TODO: Consider just how hard we want to fail in this instance
                    if isinstance(write_result, Failure):
                        return Result.from_failure(write_result.failure())

                # TODO: Validate store
                store_da: xr.DataArray = xr.open_dataarray(smd.path, engine="zarr")
                for parameter in store_da.coords["variable"].values:
                    if parameter not in entities.params.__slots__:
                        log.warning(
                            f"Cannot validate unknown parameter: {parameter}. "
                            "Ensure the parameter has been renamed to match the entities "
                            "parameters defined in `entities.parameters` if desired, or "
                            "add the parameter to the entities parameters if it is new. "
                            f"Known parameters: {entities.params.__slots__}",
                        )
                    else:
                        parameter_obj = getattr(entities.params, parameter)
                        print(parameter_obj)


                del result_generator
                monitor.join()

                notify_result = self._nr.notify(
                    entities.StoreCreatedNotification(
                        filename=smd.path.name,
                        size_mb=smd.size_mb,
                        performance=entities.PerformanceMetadata(
                            duration_seconds=monitor.get_runtime(),
                            memory_mb=max(monitor.memory_buffer) / 1e6,
                        ),
                    ),
                )

                return Result.from_value(smd.path)

            case _:
                return Result.from_failure(
                    TypeError(f"Unexpected result type: {type(create_result)}"),
                )


    def create_store_for_init_time(self, it: dt.datetime) -> ResultE[entities.StoreMetadata]:
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
        store_metadata: entities.StoreMetadata = entities.StoreMetadata(
            coordinate_map=store_coordinates,
            path=store_path,
            size_mb=0,
        )
        result = store_metadata.write_as_dummy_dataarray(name=self._mr.metadata.name)
        return result
