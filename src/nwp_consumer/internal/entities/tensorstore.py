"""Domain classes for store metadata.

Converted data is stored in Zarr stores, which are chunked datastores
enabling subselection across any dimension of data, provided it is
chunked appropriately.

This module provides a class for storing metadata about a Zarr store.
"""

import dataclasses
import datetime as dt
import json
import logging
import os
import pathlib
import shutil
import time
from typing import Any

import dask.array
import xarray as xr
import zarr
from zarr._storage.store import Store
from returns.result import Failure, Result, ResultE, Success
from importlib.metadata import version, PackageNotFoundError

from .coordinates import NWPDimensionCoordinateMap
from .parameters import Parameter, params
from .postprocess import PostProcessOptions

log = logging.getLogger("nwp-consumer")

try:
    __version__ = version("nwp-consumer")
except PackageNotFoundError:
    __version__ = "v?"


@dataclasses.dataclass(slots=True)
class ParameterScanResult:
    """Container for the results of a scan of a parameter's values."""

    mean: float
    """The mean value of the parameter's data."""
    is_valid: bool
    """Whether the parameter's data values are valid.

    This is determined according to the parameter's limits and threshold.
    See `entities.parameters.Parameter`.
    """
    has_nulls: bool
    """Whether the parameter's data contains null values."""


@dataclasses.dataclass(slots=True)
class TensorStore:
    """Store class for multidimensional data.

    This class is used to store data in a Zarr store.
    Each store instance is associated with a single init time,
    and is capable of handling parallel, region-based updates.
    """

    name: str
    """Identifier for the store and the data within."""

    path: pathlib.Path
    """The path to the store."""

    coordinate_map: NWPDimensionCoordinateMap
    """The coordinates of the store."""

    size_mb: int
    """The size of the store in megabytes."""

    encoding: dict[str, Any]
    """The encoding passed to Zarr whilst writing."""

    @classmethod
    def initialize_empty_store(
        cls,
        name: str,
        coords: NWPDimensionCoordinateMap,
    ) -> ResultE["TensorStore"]:
        """Initialize a store for a given init time.

        This method writes a blank dataarray to disk based on the input coordinates,
        which define the dimension labels and tick values of the output dataset object.
        The dataarray is 'blank' because it is written via::

            dataarray.to_zarr("<example>.zarr", compute=False)

        which writes the metadata alone.
        The utility of this is to enable region-based writing of new data to the store,
        which further allows for parallel write processes.

        There is a gotcha: regional writes can never be done in parallel to the same chunk,
        so writes must always be done at the chunk level or higher (as a chunk is an
        individual file in the store). To this effect, chunks are chosen to cover as small
        a unit of data as could reasonably be expected to be provided by an NWP source:

        - Raw data files may not contain the full grid of data, hence a chunk of size equal
          to a quarter the length of the grid dimension (lat/lon/x/y axes) is used.
        - Raw data files may contain as little as one step for a single parameter, so a chunk
          size of 1 is used along the step dimension.
        - As above for the init_time dimension.

        Args:
            name: The name of the tensor.
            coords: The coordinates of the store.

        Returns:
            An indicator of a successful store write containing the number of bytes written.

        See Also:
            - https://docs.xarray.dev/en/stable/user-guide/io.html#appending-to-existing-zarr-stores
            - https://docs.xarray.dev/en/stable/user-guide/io.html#distributed-writes

        Returns:
            A new instance of the TensorStore class.
        """
        if not isinstance(coords.init_time, list) or len(coords.init_time) == 0:
            return Result.from_failure(
                ValueError(
                    "Cannot initialize store with 'init_time' dimension coordinates not "
                    "specified via a populated list. Check instantiation of "
                    "NWPDimensionCoordinateMap. "
                    f"Got: {coords.init_time} (not a list, or empty).",
                ),
            )
        store_range: str = f"{coords.init_time[0]:%Y%m%d%H}"
        if len(coords.init_time) > 1:
            store_range = f"{coords.init_time[0]:%Y%m%d%H}-{coords.init_time[-1]:%Y%m%d%H}"

        store_path = pathlib.Path(
            f"{os.getenv('NWP_WORKDIR', f'~/.local/cache/nwp/{name}')}/{store_range}.zarr",
        )

        # * Define a set of chunks allowing for intermediate parallel writes
        #   NOTE: This is not the same as the final chunking of the dataset!
        #   Merely a chunksize that is small enough to allow for parallel writes
        #   to different regions of the init store.
        intermediate_chunks: dict[str, int] = {
            "init_time": 1,
            "step": 1,
            "variable": 1,
            "latitude": coords.shapemap.get("latitude", 400) // 4,
            "longitude": coords.shapemap.get("longitude", 400) // 8,
            "values": coords.shapemap.get("values", 100),
        }
        # Create a dask array of zeros with the shape of the dataset
        # * The values of this are ignored, only the shape and chunks are used
        dummy_values = dask.array.zeros(
            shape=list(coords.shapemap.values()),
            chunks=tuple([intermediate_chunks[k] for k in coords.shapemap]),
        )
        attrs: dict[str, str] = {
            "produced_by": "".join((
                f"nwp-consumer {__version__} at ",
                f"{dt.datetime.now(tz=dt.UTC).strftime('%Y-%m-%d %H:%M')}",
            )),
            "variables": json.dumps(
                {p.name: {"description": p.description, "units": p.units} for p in coords.variable},
            ),
        }
        # Create a DataArray object with the given coordinates and dummy values
        da: xr.DataArray = xr.DataArray(
            name=name,
            data=dummy_values,
            coords=coords.to_pandas(),
            attrs=attrs,
        )
        encoding: dict[str, Any] ={
            "init_time": {"units": "nanoseconds since 1970-01-01"},
            "step": {"units": "hours"},
        }

        try:
            # Write the dataset to a skeleton zarr file
            # * 'compute=False' enables only saving metadata
            # * 'mode="w"' overwrites any existing store
            da.to_zarr(
                store=store_path,
                compute=False,
                mode="w",
                consolidated=True,
                encoding=encoding,
            )
            # Ensure the store is readable
            store_da: xr.DataArray = xr.open_dataarray(store_path, engine="zarr")
        except Exception as e:
            return Result.from_failure(
                OSError(
                    f"Failed writing blank store to disk: {e}",
                ),
            )
        # Check the resultant array's coordinates can be converted back
        coordinate_map_result = NWPDimensionCoordinateMap.from_xarray(store_da)
        if isinstance(coordinate_map_result, Failure):
            return Result.from_failure(
                OSError(
                    f"Error reading back coordinates of initialized store "
                    f"from disk (possible corruption): {coordinate_map_result}",
                ),
            )

        return Result.from_value(
            cls(
                name=name,
                path=store_path,
                coordinate_map=coordinate_map_result.unwrap(),
                size_mb=0,
                encoding=encoding,
            ),
        )

    # --- Business logic methods --- #
    def write_to_region(
        self,
        da: xr.DataArray,
        region: dict[str, slice] | None = None,
    ) -> ResultE[int]:
        """Write partial data to the store.

        The optional region is a dictionary which maps dimension labels to slices.
        These define the region in the store to write to.

        If the region dict is empty or not provided, the region is determined
        via the 'determine_region' method.

        Args:
            da: The data to write to the store.
            region: The region to write to.

        Returns:
            An indicator of a successful store write containing the number of bytes written.
        """
        # Attempt to determine the region if missing
        if region is None or region == {}:
            region_result = NWPDimensionCoordinateMap.from_xarray(da).bind(
                self.coordinate_map.determine_region,
            )
            if isinstance(region_result, Failure):
                return Result.from_failure(region_result.failure())
            region = region_result.unwrap()

        # Perform the regional write
        try:
            da.to_zarr(store=self.path, region=region, consolidated=True)
        except Exception as e:
            return Result.from_failure(
                OSError(
                    f"Error writing to region of store: {e}",
                ),
            )

        # Calculate the number of bytes written
        nbytes: int = da.nbytes
        del da
        self.size_mb += nbytes // (1024**2)
        return Result.from_value(nbytes)

    def validate_store(self) -> ResultE[bool]:
        """Validate the store.

        This method checks the store for the presence of all expected parameters.

        Returns:
            A bool indicating the result of the validation.
        """
        store_da: xr.DataArray = xr.open_dataarray(self.path, engine="zarr")
        # Consistency check on the coordinates of the store
        coords_result = NWPDimensionCoordinateMap.from_xarray(store_da)
        match coords_result:
            case Failure(e):
                return Result.from_failure(e)
            case Success(coords):
                if coords != self.coordinate_map:
                    return Result.from_failure(
                        ValueError(
                            "Coordinate consistency check failed: "
                            "Store coordinates do not match expected coordinates. "
                            f"Expected: {self.coordinate_map}. Got: {coords}.",
                        ),
                    )

        # Validity check on the parameters of the store
        for param in self.coordinate_map.variable:
            scan_result: ResultE[ParameterScanResult] = self.scan_parameter_values(p=param)
            match scan_result:
                case Failure(e):
                    return Result.from_failure(e)
                case Success(scan):
                    log.debug(f"Scanned parameter {param.name}: {scan.__repr__()}")
                    if not scan.is_valid or scan.has_nulls:
                        return Result.from_value(False)

    def scan_parameter_values(self, p: Parameter) -> ResultE[ParameterScanResult]:
        """Scan the values of a parameter in the store.

        Extracts data from the values of the given parameter in the store.
        This reads the data from the store, so note that this can be an
        expensive operation for large datasets.

        Args:
            p: The name of the parameter to scan.

        Returns:
            A ParameterScanResult object.
        """
        if p not in self.coordinate_map.variable:
            return Result.from_failure(
                KeyError(
                    "Parameter scan failed: "
                    f"Cannot validate unknown parameter: {p.name}. "
                    "Ensure the parameter has been renamed to match the entities "
                    "parameters defined in `entities.parameters` if desired, or "
                    "add the parameter to the entities parameters if it is new. "
                    f"Store parameters: {[p.name for p in self.coordinate_map.variable]}. "
                    f"Known parameters: {params.names()}",
                ),
            )
        store_da: xr.DataArray = xr.open_dataarray(self.path, engine="zarr")

        mean = store_da.mean().values

        return Result.from_value(
            ParameterScanResult(
                mean=mean,
                is_valid=True,
                has_nulls=False,
            ),
        )


    def postprocess(self, options: PostProcessOptions) -> ResultE[pathlib.Path]:
        """Post-process the store.

        This creates a new store, as many of the postprocess options require
        modifications to the underlying file structure of the store.
        """
        if options.requires_postprocessing():
            log.info("Applying postprocessing options to store %s", self.name)

            if options.validate:
                log.warning("Validation not yet implemented in efficient manner. Skipping option.")

            store_da: xr.DataArray = xr.open_dataarray(
                self.path,
                engine="zarr",
            )

            if options.codec:
                log.debug("Applying codec %s to store %s", options.codec.name, self.name)
                self.encoding = self.encoding | {"compressor": options.codec.value}

            if options.rechunk:
                store_da = store_da.chunk(chunks=self.coordinate_map.default_chunking())

            if options.standardize_coordinates:
                # Make the longitude values range from -180 to 180
                store_da = store_da.assign_coords({
                    "longitude": ((store_da.coords["longitude"] + 180) % 360) - 180,
                })
                # Find the index of the maximum value
                idx: int = store_da.coords["longitude"].argmax().values
                # Move the maximum value to the end, and do the same to the underlying data
                store_da = store_da.roll(
                    longitude=len(store_da.coords["longitude"]) - idx - 1,
                    roll_coords=True,
                )
                coordinates_result = NWPDimensionCoordinateMap.from_xarray(store_da)
                match coordinates_result:
                    case Failure(e):
                        return Result.from_failure(e)
                    case Success(coords):
                        self.coordinate_map = coords

            if options.requires_rewrite():
                processed_path = self.path.parent / (self.path.name + ".processed")
                try:
                    log.debug(
                        "Writing postprocessed store to %s",
                        processed_path,
                    )
                    # Clear the encoding for any variables indexed as an 'object' type
                    # * e.g. Dimensions with string labels -> the variable dim
                    # * See https://github.com/sgkit-dev/sgkit/issues/991
                    # * and https://github.com/pydata/xarray/issues/3476
                    store_da.coords["variable"].encoding.clear()
                    zstore: xr.backends.zarr.ZarrStore = store_da.to_zarr(
                        store=processed_path,
                        mode="w",
                        encoding=self.encoding,
                        consolidated=True,
                    )
                    self.path = processed_path
                except Exception as e:
                    return Result.from_failure(
                        OSError(
                            f"Error encountered writing postprocessed store: {e}",
                        ),
                    )

            if options.zip:
                log.debug(
                    "Postprocessor: Zipping store to "
                    f"{self.path.with_suffix(".zarr.zip")}",
                )
                try:
                    shutil.make_archive(self.path.name, "zip", self.path)
                except Exception as e:
                    return Result.from_failure(
                        OSError(
                            f"Error encountered zipping store: {e}",
                        ),
                    )

            log.debug("Postprocessing complete for store %s", self.name)
            return Result.from_value(self.path)

        else:
            return Result.from_value(self.path)

    def update_attrs(self, attrs: dict[str, str]) -> ResultE[pathlib.Path]:
        """Update the attributes of the store.

        This method updates the attributes of the store with the given dictionary.
        """
        group: zarr.Group = zarr.open_group(self.path.as_posix())
        group.attrs.update(attrs)
        zarr.consolidate_metadata(self.path.as_posix())
        return Result.from_value(self.path)
