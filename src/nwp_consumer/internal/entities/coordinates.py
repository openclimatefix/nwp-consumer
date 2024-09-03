"""Domain entities describing dimensional coordinates.

Multidimensional data
---------------------

Tensor datasets are the primary data structure used in the consumer, which are
characterised by their multidimensional nature. To map data points in a tensor
back to selectable, indexable points along the dimensions of the tensor, a
mapping is required between the integer ticks along the dimension axes and the
values those ticks represent.

For instance, consider a 2D tensor containing x, y data of the lap number vs lap
time of a runner running around a racetrack. The point (2, 4) in the tensor
would represent the runner's time at lap 2. In this instance the indexes are
2 and 4, but to get back to the values they represent, a mapping of the
dimension indices to coordinate values must be consulted, for instance:

x index: [0, 1, 2, 3, 4]
x value: [lap 1, lap 2, lap 3, lap 4, lap 5]

y index: [0, 1, 2, 3, 4]
y value: [0 seconds, 30 seconds, 60 seconds, 90 seconds, 120 seconds]

Now by consulting the mapping we can see that the point (2, 4) in the tensor
represents that the runners time at lap three was 60 seconds.


This formalisation is useful also in the reverse case: inserting data into a
tensor according to its dimension coordinate values, and not its indexes.
This is the primary use case for these maps in this service.

It is far more likely that for incoming data the coordinate values along the
dimension axes are known, as opposed to the indexes they represent. This mapping
then enables insertion that data into the correct regions of the tensor, which is
a key part of parallel writing.
"""

import dataclasses
import datetime as dt
from typing import NotRequired, TypedDict, cast

import numpy as np
import pandas as pd
import pytz
from returns.result import Result, ResultE

from .parameters import Parameter, params


class NWPDimensionCoordinateMap(TypedDict):
    """Dictionary of dimensions names and their coordinate index values.

    Each key in the dictionary is a dimension label, and the corresponding
    value is a list of the coordinate values for each index along the dimension.

    All NWP data has an associated init time, step, and variable,
    so these dimensions names are required. Spatial coordinates however
    differ between providers and their grids, so the known spatial dimensions
    are optional (but one of them should be present!).
    """

    init_time: list[dt.datetime]
    """The init times of the forecast values."""
    step: list[int]
    """The forecast step times.

    This corresponds to the horizon of the values, which is the time
    difference between the forecast initialisation time and the target
    time at which the forecast data is valid.
    """
    variable: list[Parameter]
    """The variables in the forecast data."""
    latitude: NotRequired[list[float]]
    """The latitude coordinates of the forecast grid in degrees."""
    longitude: NotRequired[list[float]]
    """The longitude coordinates of the forecast grid in degrees."""

    @classmethod
    def from_pandas(cls, pd_indexes: dict[str, pd.Index]) -> ResultE["NWPDimensionCoordinateMap"]:
        """Create a new NWPDimensionCoordinateMap from a dictionary of pandas Index objects.

        This is useful for interoperability with xarray, which prefers to define
        DataArray coordinates using a dict pandas Index objects.

        To extract the coordinate values from an xarray DataArray,
        there is the "indexes" property on an xarray Coordinates object:

        Example:
        >>> idxs = xr_data.coords.indexes
        >>> NWPDimensionCoordinateMap.from_pandas(idxs)
        {
            "init_time": [dt.datetime(2021, 1, 1, 0, 0, ...],
            "step": [1, 2, ...],
            "variable": [Parameter(name="relative_humidity_gl", ...), ...],
            "latitude": [90, ...],
            "longitude": [45, ...],
        }

        """
        if not all(key in pd_indexes for key in ["init_time", "step", "variable"]):
            return Result.from_failure(
                KeyError(
                    f"Cannot create {cls.__class__.__name__} instance from pandas indexes "
                    " as required keys 'init_time', 'step', and 'variable' are not all present. "
                    f"Got: {pd_indexes.keys()}",
                ),
            )
        if not all(param in params.names() for param in pd_indexes["variable"].to_list()):
            unknown_params: list[str] = list(
                set(pd_indexes["variable"].to_list()).difference(set(params.names())),
            )
            return Result.from_failure(
                ValueError(
                    f"Cannot create {cls.__class__.__name__} instance from pandas indexes "
                    f"as the 'variable' dimension contains unknown parameters: {unknown_params}",
                    "Ensure the parameter names match the entities parameters defined in "
                    "`entities.parameters.params`.",
                ),
            )

        # Convert the pandas Index objects to lists of the appropriate types
        temp_dict = {}
        for key, value in pd_indexes.items():
            match key:
                case "init_time":
                    # NOTE: The timezone information is stripped from the datetime objects
                    # as numpy cannot handle timezone-aware datetime objects. As such, it
                    # must be added back in when converting to a datetime object.
                    temp_dict[key] = [
                        ts.to_pydatetime().replace(tzinfo=dt.UTC)
                        for ts in value.to_list()
                    ]
                case "step":
                    temp_dict[key] = [
                        np.timedelta64(ts, "h").astype(int)
                        for ts in value.to_list()
                    ]
                case "variable":
                    temp_dict[key] = [params.get(param) for param in value.to_list()]
                case "latitude" | "longitude":
                    # NOTE: For latitude and longitude values, we round to 4 decimal places
                    # to avoid floating point precision issues when comparing values.
                    # It is important to note that this places a limit on the precision
                    # of the latitude and longitude values that can be stored in the map.
                    # 4 decimal places corresponds to a precision of ~11m at the equator.
                    temp_dict[key] = [float(f"{lat:.4f}") for lat in value.to_list()]
                case _:
                    return Result.from_failure(KeyError(
                        f"Cannot create {cls.__class__.__name__} instance from pandas indexes "
                        f"as an unknown key was encountered: {key}",
                    ))

        try:
            out_dict: NWPDimensionCoordinateMap = cast(NWPDimensionCoordinateMap, temp_dict)
        except Exception as e:
            return Result.from_failure(ValueError(
                f"Cannot create {cls.__class__.__name__} instance from pandas indexes "
                f"as an error was encountered during casting: {e}",
            ))

        return Result.from_value(out_dict)

    def desired_chunking(self) -> dict[str, int]:
        """The expected chunk sizes for each dimension.

        A dictionary mapping of dimension labels to the size of a chunk along that
        dimension. Note that the number is chunk size, not chunk number, so a chunk
        that wants to cover the entire dimension should have a size equal to the
        dimension length.

        It defaults to a single chunk per init time and step, and a single chunk
        for each entire other dimension.
        """
        out_dict: dict[str, int] = {
            "init_time": 1,
            "step": 1,
        } | {
            key: len(value)
            for key, value in self.items()
            if key not in ["init_time", "step"]
        }
        return out_dict



def to_pandas(coords: NWPDimensionCoordinateMap) -> dict[str, pd.Index]:
    """Convert the coordinate map to a dictionary of pandas Index objects.

    This is useful for interoperability with xarray, which prefers to define
    DataArray coordinates using a dict pandas Index objects.

    For the most part, the conversion consists of a straighforward cast
    to a pandas Index object. However, there are some caveats involving
    the time-centric dimensions:

    - XArray will complain if any of the numpy time types have any precision
      other than nanoseconds, so care is taken to convert all time types to
      np.timedelta64['ns'] or np.datetime64['ns'] as appropriate.
    - Similarly, numpy can't handle timezone-aware datetime objects, so
      any timezone information is stripped before conversion.

    """
    out_dict = {
        "init_time": pd.Index([
            np.datetime64(t.astimezone(pytz.utc).replace(tzinfo=None), "ns")
            for t in coords["init_time"]
        ]),
        "step": pd.Index([
            np.timedelta64(np.timedelta64(h, "h"), "ns") for h in coords["step"]
        ]),
        "variable": pd.Index([p.name for p in coords["variable"]]),
    } | {
        key: pd.Index(value)
        for key, value in coords.items()
        if key not in ["init_time", "step", "variable"]
    }
    return out_dict

def determine_region(
    inner: NWPDimensionCoordinateMap,
    outer: NWPDimensionCoordinateMap,
) -> dict[str, slice]:
    """Return the index slices of inner mapping relative to the outer map.

    The "outer" is the larger dimension mapping, which the "inner" should be
    a subset of. A number of requirements must be met for this operation to be
    successful:

    - The inner must be a subset of the outer mapping along all dimensions
      (i.e. all coordinate values in the inner must be present in the outer
      for each dimension).
    - The inner's coordinate values must be contiguous along each dimension
      of the outer.
    - The inner must be of the same dimension map type as the outer map
      (i.e. must have exactly the same dimension labels).

    The returned dictionary of slices defines the region of the base map covered
    by the instances dimension mapping.

    Arguments:
        inner: The dimension coordinate dictionary of the smaller dataset.
        outer: The dimension coordinate dictionary of the larger dataset.

    Examples:
        Getting the inner map slices relative to the outer map:

        >>> outer = NWPCoordinateMap(
        >>>     init_time=[dt.datetime(2021, 1, 1, 0, 0)],
        >>>     step=list(range(48)),
        >>>     variable=["temperature_sl", "downward_shortwave_radiation_flux_gl"],
        >>>     latitude=[68.0, 69.0, 70.0],
        >>>     longitude=[-10.0, -9.0, -8.0]
        >>> ),
        >>> inner = outer.copy()
        >>> # Modify the step of the inner to only cover half the outer
        >>> inner["step"] = list(range(24))
        >>>
        >>> determine_region(inner, outer)
        Success({
            "init_time": slice(0, 1),
            "step": slice(0, 24), # Notice the slice is inclusive of the last index
            "variable": slice(0, 2),
            "latitude": slice(0, 3),
            "longitude": slice(0, 3),
        })

    Args:
        inner: dimension coordinate dictionary of the smaller dataset

    Returns:
        Dictionary mapping the slices defining the indexes of the coordinates in
        the outer dataset that correspond to the coordinates of the inner
    """
    # Ensure the inner and outer maps have the same rank and dimension labels
    if inner.keys() != outer.keys():
        return Result.from_failure(
            KeyError(
                "Cannot find slices in non-matching coordinate mappings: "
                "both objects must have identical dimensions (rank and labels)."
                f"Got: {inner.keys()} and {outer.keys()}.",
            ),
        )

    # Ensure the inner map is entirely contained within the outer map
    slices = {}
    for inner_dim_label, inner_dim_coords in inner.items():
        if inner_dim_label == "variable":
            inner_dim_coords = [p.name for p in inner["variable"]]
            outer_dim_coords = [p.name for p in outer["variable"]]
        else:
            outer_dim_coords = outer[inner_dim_label]
        if not set(inner_dim_coords).issubset(set(outer_dim_coords)):
            diff_coords = list(set(inner_dim_coords).difference(set(outer_dim_coords)))
            first_diff_index: int = inner_dim_coords.index(diff_coords[0])
            return Result.from_failure(
                ValueError(
                    f"Coordinate values for dimension '{inner_dim_label}' not all present "
                    "within outer dimension map. The inner map must be entirely contained "
                    "within the outer map along every dimension. "
                    f"Got: {len(diff_coords)}/{len(outer_dim_coords)} differing coordinate values. "
                    f"First differing value: '{diff_coords[0]}' (inner[{first_diff_index}]) != "
                    f"'{outer_dim_coords[first_diff_index]}' (outer[{first_diff_index}]).",
                ),
            )

        # Ensure the inner map's coordinate values are contiguous in the outer map
        outer_dim_indices = sorted(
            [outer_dim_coords.index(c) for c in inner_dim_coords],
        )
        contiguous_index_run = list(range(outer_dim_indices[0], outer_dim_indices[-1] + 1))
        if outer_dim_indices != contiguous_index_run:
            return Result.from_failure(
                ValueError(
                    f"Coordinate values for dimension <{inner_dim_label}> do not correspond "
                    f"with a contiguous index set in the outer dimension map. "
                    f"Got: <{outer_dim_indices}> indices.",
                ),
            )

        slices[inner_dim_label] = slice(outer_dim_indices[0], outer_dim_indices[-1] + 1)

    return Result.from_value(slices)
