"""Domain classes for store metadata.

Converted data is stored in Zarr stores, which are chunked datastores
enabling subselection across any dimension of data, provided it is
chunked appropriately.

This module provides a class for storing metadata about a Zarr store.
"""

import dataclasses
import logging
import pathlib
from typing import TYPE_CHECKING

import dask.array
import xarray as xr
from returns.result import Failure, Result, ResultE, Success
from xarray.core.indexes import Indexes

if TYPE_CHECKING:
    import numpy as np

from ._sharedtypes import LabelCoordinateDict
from .parameters import params

log = logging.getLogger("nwp-consumer")


@dataclasses.dataclass(slots=True)
class StoreMetadata:
    """Metadata for a store."""

    path: pathlib.Path
    """The path to the store."""

    coordinate_map: LabelCoordinateDict | Indexes  # type: ignore
    """The coordinates of the store."""

    size_mb: int
    """The size of the store in megabytes."""

    def write_to_region(
            self,
            ds: xr.Dataset,
            region: dict[str, slice] | None = None,
    ) -> ResultE[int]:
        """Write partial data to the store.

        The optional region is a dictionary which maps dimension labels to slices.
        These define the region in the store to write to.

        If the region dict is empty or not provided, the region is determined
        via the 'determine_region' method.

        Args:
            ds: The data to write to the store.
            region: The region to write to.

        Returns:
            An indicator of a successful store write containing the number of bytes written.
        """
        if region is None or region == {}:
            region_result = self.determine_region(ds.coords.indexes)
            match region_result:
                case Failure(e):
                    return Result.from_failure(e)
                case Success(r):
                    region = r

        try:
            _ = ds.to_zarr(store=self.path, region=region, consolidated=True, write_empty_chunks=False)
            nbytes = ds.nbytes
            del ds
            self.size_mb = nbytes // (1024 ** 2)
            return Result.from_value(nbytes)
        except Exception as e:
            return Result.from_failure(e)

    def determine_region(
            self,
            inner: LabelCoordinateDict | Indexes,  # type: ignore
    ) -> ResultE[dict[str, slice]]:
        """Return the index slices of inner mapping relative to the outer map.

        The calling instance is regarded as the "outer", that is, the larger
        dimension mapping. The "inner" should be a subset of the calling instance.
        A number of requirements must be met for this operation to be successful:

        - The inner must be a subset of the outer mapping along all dimensions
          (i.e. all coordinate values in the inner must be present in the outer
          for each dimension).
        - The inner's coordinate values must be contiguous along each dimension
          of the outer.
        - The inner must be of the same dimension map type as the outer map
          (i.e. must have exactly the same dimension labels).

        The returned dictionary of slices defines the region of the base map covered
        by the instances dimension mapping.

        Examples:
            Getting the inner map slices relative to the outer map:

            >>> outer = StoreMetadata(
            >>>     coordinate_map={
            >>>         "init_time": [np.datetime64("2021-01-01T00:00:00")],
            >>>         "step": [np.timedelta64(i, "h") for i in range(48)],
            >>>         "latitude": [68.0, 69.0, 70.0],
            >>>         "longitude": [-10.0, -9.0, -8.0])
            >>>     },
            >>>     path=pathlib.Path("dummy.zarr"),
            >>> )
            >>>
            >>> inner = outer.coordinate_map.copy()
            >>> inner["step"] = [np.timedelta64(i, "h") for i in range(24)])
            >>>
            >>> outer.determine_region(inner)
            Success({
                "init_time": slice(0, 1),
                "step": slice(0, 24),
                "latitude": slice(0, 3),
                "longitude": slice(0, 3),
            })

        Args:
            inner: dimension coordinate dictionary of the smaller dataset

        Returns:
            Dictionary mapping the slices defining the indexes of the coordinates in
            the outer dataset that correspond to the coordinates of the inner
        """
        # Convert xarray indexes into list views
        # * Enables passing in xarray Dataset's 'coordinates.indexes' object directly
        if isinstance(inner, Indexes):
            inner = {k: v.to_list() for k, v in inner.items()}
        if isinstance(self.coordinate_map, Indexes):
            self.coordinate_map = {k: v.to_list() for k, v in self.coordinate_map.items()}

        # Ensure the inner and outer maps have the same rank and dimension labels
        if inner.keys() != self.coordinate_map.keys():
            return Result.from_failure(KeyError(
                "Cannot find slices in non-matching coordinate mappings: "
                "both objects must have identical dimensions (rank and labels)."
                f"Got: {inner.keys()} and {self.coordinate_map.keys()}.",
            ))

        slices = {}
        inner_dim_label: str
        inner_dim_coords: list[np.datetime64] | list[np.timedelta64] | list[str] | list[float]
        for inner_dim_label, inner_dim_coords in inner.items():
            if not set(inner_dim_coords).issubset(set(self.coordinate_map[inner_dim_label])):
                return Result.from_failure(ValueError(
                    f"Coordinate values for dimension <{inner_dim_label}> not all present "
                    "within outer dimension map. The inner map must be entirely contained "
                    "within the outer map along every dimension. "
                    "Got differing coordinate values: "
                    f"{set(inner_dim_coords) ^ set(self.coordinate_map[inner_dim_label])}.",
                ))

            outer_dim_indices = sorted(
                [self.coordinate_map[inner_dim_label].index(c) for c in inner_dim_coords],
            )
            contiguous_index_run = list(range(outer_dim_indices[0], outer_dim_indices[-1] + 1))
            if outer_dim_indices != contiguous_index_run:
                return Failure(ValueError(
                    f"Coordinate values for dimension <{inner_dim_label}> do not correspond "
                    f"with a contiguous index set in the outer dimension map. "
                    f"Got: <{outer_dim_indices}> indices.",
                ))

            slices[inner_dim_label] = slice(outer_dim_indices[0], outer_dim_indices[-1] + 1)

        return Success(slices)

    def write_as_dummy_dataset(self, name: str) -> ResultE["StoreMetadata"]:
        """Write a blank dataset to disk based on the input coordinates.

        The coordinates define the dimension labels and tick values of the
        output dataset object.

        No actual data is present on the produced dataset. As such, storing it as a zarr via::

            dataset.to_zarr("dummy.zarr", compute=False)

        results in a blank zarr store, save for the metadata alone. The utility of this is
        to enable region-based writing of new data to the store, which can be done using
        parallel processes.

        There is a gotcha: regional writes can never be done in parallel to the same chunk,
        so writes must always be done at the chunk level or higher (as a chunk is an
        individual file in the store). In this manner chunks are chosen to cover as small
        a unit of data as could reasonably be expected to be provided by an NWP source:

        - Raw data files may not contain the full grid of data, hence a chunk of size equal
          to a quarter the length of the grid dimension (lat/lon/x/y axes) is used.
        - Raw data files may contain as little as one step for a single parameter, so a chunk
          size of 1 is used along the step dimension.
        - As above for the init_time dimension.

        Args:
            name: The name of the tensor to write to the store.

        Returns:
            An indicator of a successful store write containing the number of bytes written.

        See Also:
            - https://docs.xarray.dev/en/stable/user-guide/io.html#appending-to-existing-zarr-stores
            - https://docs.xarray.dev/en/stable/user-guide/io.html#distributed-writes
        """
        if list(self.coordinate_map.keys())[:3] != ["init_time", "step", "variable"]:
            return Result.from_failure(KeyError(
                "The first three coordinate labels must be ['init_time', 'step', 'variable']. "
                f"Got: {list(self.coordinate_map.keys())[:3]}.",
            ))
        # Determine the shape of the dataset via the coordinate map
        shape_dict: dict[str, int] = {
            k: len(v) for k, v in self.coordinate_map.items()
        }
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
        data_vars = {name: (tuple(shape_dict.keys()), dummy_values)}
        # Ensure the coordinates are coordinates with dimensions
        # * This is done via the dict value being a tuple of (label, coordinate values)
        xr_coords = {k: (k, v) for k, v in self.coordinate_map.items()}
        ds: xr.Dataset = xr.Dataset(data_vars=data_vars, coords=xr_coords)
        try:
            # Write the dataset to a skeleton zarr file
            # * 'compute=False' enables only saving metadata
            # * 'mode="w"' overwrites any existing store
            ds.to_zarr(store=self.path, compute=False, mode="w")
            # Update the coordinates in the metadata to the actual coordinates
            # * This also ensures the zarr is readable
            store_ds = xr.open_zarr(self.path)
            self.coordinate_map = store_ds.coords.indexes
            self.size_mb = store_ds.nbytes // (1024 ** 2)
            return Result.from_value(self)
        except Exception as e:
            return Result.from_failure(e)




