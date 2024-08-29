"""Domain classes for store metadata.

Converted data is stored in Zarr stores, which are chunked datastores
enabling subselection across any dimension of data, provided it is
chunked appropriately.

This module provides a class for storing metadata about a Zarr store.
"""

import dataclasses
import datetime as dt
import logging
import pathlib

import dask.array
import xarray as xr
from returns.pipeline import flow
from returns.pointfree import bind
from returns.result import Failure, Result, ResultE, Success

from .coordinates import NWPDimensionCoordinateMap, determine_region, to_pandas
from .parameters import Parameter, params

log = logging.getLogger("nwp-consumer")

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
        if not isinstance(coords["init_time"], list):
            return Result.from_failure(
                ValueError(
                    "Cannot initialize store with 'init_time' dimension coordinates not "
                    "specified via a list. Check instantiation of NWPDimensionCoordinateMap. "
                    f"Got: {coords['init_time']} (not a list).",
                ),
            )
        if len(coords["init_time"]) != 1:
            return Result.from_failure(
                ValueError(
                    "Cannot initialize store with 'init_time' dimension specifying "
                    "multiple init time coordinates. "
                    f"Expected a single init time, got: {coords['init_time']}.",
                ),
            )
        store_path = pathlib.Path(
            f"~/.local/cache/nwp/{name}/{coords['init_time'][0]:%Y%m%d%H}.zarr",
        )

        shape_dict: dict[str, int] = {k: len(v) for k, v in coords.items()}
        # * Define a set of chunks allowing for intermediate parallel writes
        #   NOTE: This is not the same as the final chunking of the dataset!
        #   Merely a chunksize that is small enough to allow for parallel writes
        #   to different regions of the init store.
        intermediate_chunks: dict[str, int] = {
            "init_time": 1,
            "step": 1,
            "variable": 1,
            "latitude": shape_dict.get("latitude", 400) // 4,
            "longitude": shape_dict.get("longitude", 400) // 4,
            "values": shape_dict.get("values", 100),
        }
        # Create a dask array of zeros with the shape of the dataset
        # * The values of this are ignored, only the shape and chunks are used
        dummy_values = dask.array.zeros(
            shape=list(shape_dict.values()),
            chunks=tuple([intermediate_chunks[k] for k in shape_dict]),
        )
        attrs: dict[str, str] = {
            "produced_by": "nwp-consumer",
            "produced_at": str(dt.datetime.now(tz=dt.UTC)),
            "variables": "; ".join([f"{v.name}: {v.description}" for v in coords["variable"]]),
        }
        # Create a DataArray object with the given coordinates and dummy values
        da: xr.DataArray = xr.DataArray(
            name=name,
            data=dummy_values,
            coords=to_pandas(coords),
            attrs=attrs,
        )
        try:
            # Write the dataset to a skeleton zarr file
            # * 'compute=False' enables only saving metadata
            # * 'mode="w"' overwrites any existing store
            da.to_zarr(store=store_path, compute=False, mode="w", consolidated=True)
            # Ensure the store is readable
            store_da: xr.DataArray = xr.open_dataarray(store_path, engine="zarr")
        except Exception as e:
            return Result.from_failure(
                OSError(
                    f"Failed writing blank store to disk: {e}",
                ),
            )
        coordinate_map_result = NWPDimensionCoordinateMap.from_pandas(
            pd_indexes=store_da.coords.indexes,
        )
        match coordinate_map_result:
            case Failure(e):
                return Result.from_failure(e)
            case Success(coordinate_map):
                return Result.from_value(cls(
                    name=name,
                    path=store_path,
                    coordinate_map=coordinate_map,
                    size_mb=0,
                ))

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
        if region is None or region == {}:

            region_result = flow(
                NWPDimensionCoordinateMap.from_pandas(da.coords.indexes),
                bind(
                    lambda map: determine_region(inner=map, outer=self.coordinate_map)
                ),
            )
            try:
                Result.do(
                    da.to_zarr(store=self.path, region=region, consolidated=True)
                    for region in region_result
                )
            except Exception as e:
                return Result.from_failure(OSError(
                    f"Error writing to region of store: {e}",
                ))
            nbytes: int = da.nbytes
            del da
            self.size_mb += nbytes // (1024 ** 2)
            return Result.from_value(nbytes)

    def validate_store(self) -> ResultE[bool]:
        """Validate the store.

        This method checks the store for the presence of all expected parameters.

        Returns:
            A bool indicating the result of the validation.
        """
        store_da: xr.DataArray = xr.open_dataarray(self.path, engine="zarr")
        # Consistency check on the coordinates of the store
        coords_result = NWPDimensionCoordinateMap.from_pandas(store_da.coords.indexes)
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
        for param in self.coordinate_map["variable"]:
            scan_result: ResultE[ParameterScanResult] = self.scan_parameter_values(
                param_name=param_name,
            )
            match scan_result:
                case Failure(e):
                    return Result.from_failure(e)
                case Success(scan):
                    log.debug(f"Scanned parameter {param_name}: {scan.__repr__()}")
                    if not scan.is_valid or scan.has_nulls:
                        return Result.from_value(False)


    def scan_parameter_values(self, p: Parameter) -> ResultE[ParameterScanResult]:
        """Scan the values of a parameter in the store.

        Extracts data from the values of the given parameter in the store.
        This reads the data from the store, so note that this can be an
        expensive operation for large datasets.

        Args:
            param_name: The name of the parameter to scan.

        Returns:
            A ParameterScanResult object.
        """
        if p not in self.coordinate_map["variable"]:
            return Result.from_failure(KeyError(
                "Parameter scan failed: "
                f"Cannot validate unknown parameter: {p.name}. "
                "Ensure the parameter has been renamed to match the entities "
                "parameters defined in `entities.parameters` if desired, or "
                "add the parameter to the entities parameters if it is new. "
                f"Store parameters: {[p.name for p in self.coordinate_map['variable']]}. "
                f"Known parameters: {params.__slots__}",
            ))
        store_da: xr.DataArray = xr.open_dataarray(self.path, engine="zarr")
        values: list[float] = (
            store_da.sel(variable=p.name)
            .to_numpy()
            .flatten()
            .tolist()
        )
        total: float = 0.0
        num_outside_limits: int = 0
        num_null: int = 0
        for val in values:
            total += val
            if val > p.limits.upper or val < p.limits.lower:
                num_outside_limits += 1
            if val is None:
                num_null += 1

        return Result.from_value(ParameterScanResult(
            mean=total / len(values),
            is_valid=num_outside_limits / len(values) < p.limits.threshold,
            has_nulls=num_null > 0,
        ))
