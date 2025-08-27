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
import json
import logging
import math
from importlib.metadata import PackageNotFoundError, version
from typing import Any

import dask.array
import numpy as np
import pandas as pd
import pytz
import xarray as xr
from returns.result import Failure, ResultE, Success

from .parameters import Parameter

try:
    __version__ = version("nwp-consumer")
except PackageNotFoundError:
    __version__ = "v?"

log = logging.getLogger("nwp-consumer")


@dataclasses.dataclass(slots=True)
class NWPDimensionCoordinateMap:
    """Container for dimensions names and their coordinate index values.

    Each public field in the container is a dimension label, and the corresponding
    value is a list of the coordinate values for each index along the dimension.

    All NWP data has an associated init time, step, and variable,
    so these dimensions names are required. Spatial coordinates however
    differ between providers and their grids, so the known spatial dimensions
    are optional (but one of them should be present!).

    For instance, most models produce data on a latitude/longitude grid,
    but others use alternative projections resulting in x/y/ grids instead.
    """

    init_time: list[dt.datetime]
    """The init times of the forecast values."""
    step: list[int]
    """The forecast step times.

    This corresponds to the horizon of the values, which is the time
    difference between the forecast initialization time and the target
    time at which the forecast data is valid.
    """
    variable: list[Parameter]
    """The variables in the forecast data."""
    ensemble_stat: list[str] | None = None
    """The relevant ensemble statistics of the forecast data."""
    ensemble_member: list[int] | None = None
    """The ensemble member numbers making up the ensemble forecast."""
    latitude: list[float] | None = None
    """The latitude coordinates of the forecast grid in degrees.

    Will be truncated to 4 decimal places, and ordered as 90 -> -90.
    """
    longitude: list[float] | None = None
    """The longitude coordinates of the forecast grid in degrees. """

    y_osgb: list[int] | None = None
    """Y coordinates of an OSGB grid."""
    x_osgb: list[int] | None = None
    """X coordinates of an OSGB grid."""

    y_laea: list[int] | None = None
    """Y coordinates of a Lambert Azimuthal Equal Area grid."""
    x_laea: list[int] | None = None
    """X coordinates of a Lambert Azimuthal Equal Area grid."""

    def __post_init__(self) -> None:
        """Rigidly set input value ordering and precision."""
        self.variable = sorted(self.variable)
        # Make latitude descending, longitude ascending, and both rounded to 4 d.p.
        # NOTE: For latitude and longitude values, we round to 4 decimal places
        # to avoid floating point precision issues when comparing values.
        # It is important to note that this places a limit on the precision
        # of the latitude and longitude values that can be stored in the map.
        # 4 decimal places corresponds to a precision of ~11m at the equator.
        if self.latitude is not None:
            self.latitude = sorted([float(f"{lat:.4f}") for lat in self.latitude], reverse=True)
        if self.longitude is not None:
            self.longitude = sorted([float(f"{lon:.4f}") for lon in self.longitude])

    @property
    def dims(self) -> list[str]:
        """Get instantiated dimensions.

        Ignores any dimensions that do not have a corresponding coordinate
        index value list.
        """
        return [f.name for f in dataclasses.fields(self) if getattr(self, f.name) is not None]

    @property
    def shapemap(self) -> dict[str, int]:
        """Mapping of dimension names to lengths; the 'shape' of the coordinates.

        This is the length of each dimension in the map,
        which can be thought of as the number of ticks along each dimension
        axis.
        """
        return {dim: len(getattr(self, dim)) for dim in self.dims}

    @property
    def coord_system(self) -> dict[str, Any]:
        """The coordinate system of the map.

        Returns:
            A string representing the coordinate system of the map.
        """
        if self.latitude is not None and self.longitude is not None:
            return {"geodesic_ellipsiodal": {"crs": "EPSG:4326"}}
        if self.y_osgb is not None and self.x_osgb is not None:
            return {
                "transverse_mercator": {
                    "latitude_of_projection_origin": 49.0,
                    "longitude_of_central_meridian": -2.0,
                    "false_easting": 400000.0,
                    "false_northing": -100000.0,
                    "scale_factor_at_central_meridian": 0.0,
                    "ellipsoid": {
                        "semi_major_axis": 6377563.4,
                        "semi_minor_axis": 6356256.91,
                    },
                },
            }
        if self.y_laea is not None and self.x_laea is not None:
            return {
                "lambert_azimuthal_equal_area": {
                    "latitude_of_projection_origin": 54.9,
                    "longitude_of_projection_origin": -2.5,
                    "false_easting": 0.0,
                    "false_northing": 0.0,
                    "ellipsoid": {
                        "semi_major_axis": 6378137.0,
                        "semi_minor_axis": 6356752.314140356,
                    },
                },
            }
        return {}

    @classmethod
    def from_pandas(
        cls,
        pd_indexes: dict[str, pd.Index],
    ) -> ResultE["NWPDimensionCoordinateMap"]:
        """Create a new NWPDimensionCoordinateMap from a dictionary of pandas Index objects.

        This is useful for interoperability with xarray, which prefers to define
        DataArray coordinates using a dict pandas Index objects.

        To extract the coordinate values from an xarray DataArray,
        there is the "indexes" property on an xarray Coordinates object:

        Example:
        >>> > idxs = xr_data.coords.indexes
        >>> > NWPDimensionCoordinateMap.from_pandas(idxs)
        >>> {
        >>>     "init_time": [dt.datetime(2021, 1, 1, 0, 0)],
        >>>     "step": [1, 2],
        >>>     "variable": [Parameter.TEMPERATURE_SL],
        >>>     "latitude": [90, 80, 70],
        >>>     "longitude": [45, 50, 55],
        >>> }

        See Also:
            `NWPDimensionCoordinateMap.to_pandas` for the reverse operation.
        """
        if not all(key in pd_indexes for key in ["init_time", "step", "variable"]):
            return Failure(
                KeyError(
                    f"Cannot create {cls.__class__.__name__} instance from pandas indexes "
                    "as required keys 'init_time', 'step', and 'variable' are not all present. "
                    f"Got: '{list(pd_indexes.keys())}'",
                ),
            )
        if not all(len(pd_indexes[key].to_list()) > 0 for key in ["init_time", "step", "variable"]):
            return Failure(
                ValueError(
                    f"Cannot create {cls.__class__.__name__} instance from pandas indexes "
                    "as the 'init_time', 'step', and 'variable' dimensions must have "
                    "at least one coordinate value.",
                ),
            )
        input_parameter_set: set[str] = set(pd_indexes["variable"].to_list())
        known_parameter_set: set[str] = {str(p) for p in Parameter}
        if not input_parameter_set.issubset(known_parameter_set):
            return Failure(
                ValueError(
                    f"Cannot create {cls.__class__.__name__} instance from pandas indexes "
                    "as the 'variable' dimension contains unknown parameters: ",
                    f"'{list(input_parameter_set.difference(known_parameter_set))}'. "
                    "Ensure the parameter names match the names of the standard parameter set "
                    "defined by the `entities.Parameter` Enum.",
                ),
            )
        if not all(key in [f.name for f in dataclasses.fields(cls)] for key in pd_indexes):
            unknown_keys: list[str] = list(
                set(pd_indexes.keys()).difference([f.name for f in dataclasses.fields(cls)]),
            )
            return Failure(
                KeyError(
                    f"Cannot create {cls.__class__.__name__} instance from pandas indexes "
                    f"as unknown index/dimension keys were encountered: {unknown_keys}.",
                ),
            )
        if (
            "latitude" in pd_indexes
            and pd_indexes["latitude"].values[0] < pd_indexes["latitude"].values[-1]
        ):
            return Failure(
                ValueError(
                    "Cannot create NWPDimensionCoordinateMap instance from pandas indexes "
                    "as the latitude values are not in descending order. "
                    "Latitude coordinates should run from 90 -> -90. "
                    "Modify the coordinate in the source data to be in descending order.",
                ),
            )
        if (
            "longitude" in pd_indexes
            and pd_indexes["longitude"].values[0] > pd_indexes["longitude"].values[-1]
        ):
            return Failure(
                ValueError(
                    "Cannot create NWPDimensionCoordinateMap instance from pandas indexes "
                    "as the longitude values are not in ascending order. "
                    "Longitude coordinates should run from -180 -> 180. "
                    "Modify the coordinate in the source data to be in ascending order.",
                ),
            )
        if (
            "y_osgb" in pd_indexes
            and pd_indexes["y_osgb"].values[0] < pd_indexes["y_osgb"].values[-1]
        ):
            return Failure(
                ValueError(
                    "Cannot create NWPDimensionCoordinateMap instance from pandas indexes "
                    "as the y_osgb values are not in descending order. "
                    "Modify the coordinate in the source data to be in descending order.",
                ),
            )
        if (
            "x_osgb" in pd_indexes
            and pd_indexes["x_osgb"].values[0] > pd_indexes["x_osgb"].values[-1]
        ):
            return Failure(
                ValueError(
                    "Cannot create NWPDimensionCoordinateMap instance from pandas indexes "
                    "as the x_osgb values are not in ascending order. "
                    "Modify the coordinate in the source data to be in ascending order.",
                ),
            )
        if (
            "y_laea" in pd_indexes
            and pd_indexes["y_laea"].values[0] < pd_indexes["y_laea"].values[-1]
        ):
            return Failure(
                ValueError(
                    "Cannot create NWPDimensionCoordinateMap instance from pandas indexes "
                    "as the y_laea values are not in descending order. "
                    "Modify the coordinate in the source data to be in descending order.",
                ),
            )
        if (
            "x_laea" in pd_indexes
            and pd_indexes["x_laea"].values[0] > pd_indexes["x_laea"].values[-1]
        ):
            return Failure(
                ValueError(
                    "Cannot create NWPDimensionCoordinateMap instance from pandas indexes "
                    "as the x_laea values are not in ascending order. "
                    "Modify the coordinate in the source data to be in ascending order.",
                ),
            )

        # Convert the pandas Index objects to lists of the appropriate types
        return Success(
            cls(
                # NOTE: The timezone information is stripped from the datetime objects
                # as numpy cannot handle timezone-aware datetime objects. As such, it
                # must be added back in when converting to a datetime object.
                init_time=[
                    ts.to_pydatetime().replace(tzinfo=dt.UTC)
                    for ts in pd_indexes["init_time"].to_list()
                ],
                step=[np.timedelta64(ts, "h").astype(int) for ts in pd_indexes["step"].to_list()],
                # NOTE: This list comprehension can be done safely, as above we have
                # already performed a check on the pandas variable names being a subset
                # of the `Parameter` enum value names.
                variable=[Parameter(pdp) for pdp in pd_indexes["variable"].to_list()],
                ensemble_stat=pd_indexes["ensemble_stat"].to_list()
                if "ensemble_stat" in pd_indexes
                else None,
                ensemble_member=pd_indexes["ensemble_member"].to_list()
                if "ensemble_member" in pd_indexes
                else None,
                latitude=pd_indexes["latitude"].to_list() if "latitude" in pd_indexes else None,
                longitude=pd_indexes["longitude"].to_list() if "longitude" in pd_indexes else None,
                y_osgb=pd_indexes["y_osgb"].to_list() if "y_osgb" in pd_indexes else None,
                x_osgb=pd_indexes["x_osgb"].to_list() if "x_osgb" in pd_indexes else None,
                y_laea=pd_indexes["y_laea"].to_list() if "y_laea" in pd_indexes else None,
                x_laea=pd_indexes["x_laea"].to_list() if "x_laea" in pd_indexes else None,
            ),
        )

    @classmethod
    def from_xarray(
        cls, xarray_obj: xr.DataArray | xr.Dataset,
    ) -> ResultE["NWPDimensionCoordinateMap"]:
        """Create a new NWPDimensionCoordinateMap from an XArray DataArray or Dataset object."""
        return cls.from_pandas(xarray_obj.coords.indexes)  # type: ignore

    def to_pandas(self) -> dict[str, pd.Index]:
        """Convert the coordinate map to a dictionary of pandas Index objects.

        This is useful for interoperability with xarray, which prefers to define
        DataArray coordinates using a dict pandas Index objects.

        For the most part, the conversion consists of a straightforward cast
        to a pandas Index object. However, there are some caveats involving
        the time-centric dimensions:

        - XArray will complain if any of the numpy time types have any precision
          other than nanoseconds, so care is taken to convert all time types to
          np.timedelta64['ns'] or np.datetime64['ns'] as appropriate.
        - Similarly, numpy can't handle timezone-aware datetime objects, so
          any timezone information is stripped before conversion.

        See Also:
            `NWPDimensionCoordinateMap.from_pandas` for the reverse operation.

        """
        out_dict: dict[str, pd.Index] = {  # type: ignore
            "init_time": pd.Index(
                [
                    np.datetime64(t.astimezone(pytz.utc).replace(tzinfo=None), "ns")
                    for t in self.init_time
                ],
            ),
            "step": pd.Index([np.timedelta64(np.timedelta64(h, "h"), "ns") for h in self.step]),
            "variable": pd.Index([p.value for p in self.variable]),
        } | {
            dim: pd.Index(getattr(self, dim))
            for dim in self.dims
            if dim not in ["init_time", "step", "variable"]
        }
        return out_dict

    def determine_region(
        self,
        inner: "NWPDimensionCoordinateMap",
    ) -> ResultE[dict[str, slice]]:
        """Return the index slices of inner mapping relative to the outer map.

        The caller is the "outer" dimension mapping, which the "inner" should be
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

        Note that xarray does have its own implementation of this: the "region='auto'"
        argument to the "to_zarr" method performs a similar function. This is
        reimplemented in this package partly to ensure consistency of behaviour,
        partly to enable more descriptive logging in failure states, and partly to
        enable extending the functionality.

        Args:
            inner: The dimension coordinate dictionary of the smaller dataset.

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
            >>> outer.determine_region(inner)
            Success({
                "init_time": slice(0, 1),
                "step": slice(0, 24), # Notice the slice is inclusive of the last index
                "variable": slice(0, 2),
                "latitude": slice(0, 3),
                "longitude": slice(0, 3),
            })

        Returns:
            Dictionary mapping the slices defining the indexes of the coordinates in
            the outer dataset that correspond to the coordinates of the inner
        """
        # Ensure the inner and outer maps have the same rank and dimension labels
        if inner.dims != self.dims:
            return Failure(
                KeyError(
                    "Cannot find slices in non-matching coordinate mappings: "
                    "both objects must have identical dimensions (rank and labels). "
                    f"Got: {inner.dims} (inner) and {self.dims} (outer).",
                ),
            )

        # Ensure the inner map is entirely contained within the outer map
        slices = {}
        for inner_dim_label in inner.dims:
            inner_dim_coords = getattr(inner, inner_dim_label)
            outer_dim_coords = getattr(self, inner_dim_label)
            if len(inner_dim_coords) > len(outer_dim_coords):
                return Failure(
                    ValueError(
                        f"Coordinate values for dimension '{inner_dim_label}' in the inner map "
                        "exceed the number of coordinate values in the outer map. "
                        f"Got: {len(inner_dim_coords)} (> {len(outer_dim_coords)}) "
                        f"coordinate values.",
                    ),
                )
            if not set(inner_dim_coords).issubset(set(outer_dim_coords)):
                diff_coords = list(set(inner_dim_coords).difference(set(outer_dim_coords)))
                first_diff_index: int = inner_dim_coords.index(diff_coords[0])
                return Failure(
                    ValueError(
                        f"Coordinate values for dimension '{inner_dim_label}' not all present "
                        "within outer dimension map. The inner map must be entirely contained "
                        "within the outer map along every dimension. "
                        f"Got: {len(diff_coords)}/{len(outer_dim_coords)} differing values. "
                        f"First differing value: '{diff_coords[0]}' (inner[{first_diff_index}]) != "
                        f"'{outer_dim_coords[first_diff_index]}' (outer[{first_diff_index}]).",
                    ),
                )

            # Ensure the inner map's coordinate values are contiguous in the outer map.
            # * First, get the index of the corresponding value in the outer map for each
            #   coordinate value in the inner map:
            outer_dim_indices = sorted(
                [outer_dim_coords.index(c) for c in inner_dim_coords],
            )
            contiguous_index_run = list(range(outer_dim_indices[0], outer_dim_indices[-1] + 1))
            if outer_dim_indices != contiguous_index_run:
                idxs = np.argwhere(np.gradient(outer_dim_indices) > 1).flatten()
                return Failure(
                    ValueError(
                        f"Coordinate values for dimension '{inner_dim_label}' do not correspond "
                        f"with a contiguous index set in the outer dimension map. "
                        f"Non-contiguous values '{[outer_dim_coords[i] for i in idxs]} "
                        f"(index {[outer_dim_indices[i] for i in idxs]})' "
                        f"adjacent in dimension coordinates.",
                    ),
                )

            slices[inner_dim_label] = slice(outer_dim_indices[0], outer_dim_indices[-1] + 1)

        return Success(slices)

    def chunking(self, chunk_count_overrides: dict[str, int]) -> dict[str, int]:
        """The expected chunk sizes for each dimension.

        A dictionary mapping of dimension labels to the size of a chunk along that
        dimension. Note that the number is chunk size, not chunk number, so a chunk
        that wants to cover the entire dimension should have a size equal to the
        dimension length.

        It defaults to a single chunk per init time, step, and variable coordinate,
        and 2 chunks for each entire other dimension, unless overridden by the
        `chunk_count_overrides` argument.

        The defaults are purposefully small, to ensure that when performing parallel
        writes, chunk boundaries are not crossed.

        Args:
            chunk_count_overrides: A dictionary mapping dimension labels to the
                *number* of chunks to split the dimension into.
        """
        default_dict: dict[str, int] = {
            dim: 1
            if len(getattr(self, dim)) <= 8 or dim in ["init_time", "step", "variable"]
            else math.ceil(len(getattr(self, dim)))
            for dim in self.dims
        }

        out_dict = {}
        # Put overrides in
        for key in default_dict:
            if key in chunk_count_overrides:
                out_dict[key] = len(getattr(self, key)) // chunk_count_overrides[key]
            else:
                out_dict[key] = default_dict[key]

        return out_dict

    def as_zeroed_dataarray(self, name: str, chunks: dict[str, int]) -> xr.DataArray:
        """Express the coordinates as an xarray DataArray.

        The underlying dask array is a zeroed array with the shape of the dataset,
        that is chunked according to the given chunking scheme.

        Args:
            name: The name of the DataArray.
            chunks: A mapping of dimension names to the size of the chunks
                along the dimensions.

        See Also:
            - https://docs.xarray.dev/en/stable/user-guide/io.html#distributed-writes
        """
        # Create a dask array of zeros with the shape of the dataset
        # * The values of this are ignored, only the shape and chunks are used
        dummy_values = dask.array.zeros(  # type: ignore
            shape=list(self.shapemap.values()),
            chunks=tuple([chunks[k] for k in self.shapemap]),
        )
        attrs: dict[str, str] = {
            "produced_by": "".join(
                (
                    f"nwp-consumer {__version__} at ",
                    f"{dt.datetime.now(tz=dt.UTC).strftime('%Y-%m-%d %H:%M')}",
                ),
            ),
            "variables": json.dumps(
                {
                    p.value: {
                        "description": p.metadata().description,
                        "units": p.metadata().units,
                    }
                    for p in self.variable
                },
            ),
            "coord_system": json.dumps(self.coord_system),
        }
        # Create a DataArray object with the given coordinates and dummy values
        da: xr.DataArray = xr.DataArray(
            name=name,
            data=dummy_values,
            coords=self.to_pandas(),
            attrs=attrs,
        )
        return da

    def crop(
        self,
        north: float,
        west: float,
        south: float,
        east: float,
    ) -> ResultE["NWPDimensionCoordinateMap"]:
        """Return a new map cropped to the given region.

        Args:
            north: The northernmost latitude of the region in degrees.
            west: The westernmost longitude of the region in degrees.
            south: The southernmost latitude of the region in degrees.
            east: The easternmost longitude of the region in degrees.

        Returns:
            A new NWPDimensionCoordinateMap object with the latitude and longitude
            coordinates cropped to the given region.
        """
        # Ensure the region is valid
        if self.latitude is None or self.longitude is None:
            return Failure(
                ValueError(
                    "Cannot crop coordinates to a region as latitude and/or longitude "
                    "dimension coordinates are not present in the map. "
                    f"Dimensions: '{self.dims}'.",
                ),
            )

        if not (-90 <= south < north <= 90 and -180 <= west < east <= 180):
            return Failure(
                ValueError(
                    "Cannot crop coordinates to an invalid region. "
                    f"North ({north}) must be greater than South ({south}) "
                    " and both must sit between 90 and -90 degrees; "
                    f"East ({east}) greater than West ({west}) "
                    " and both must sit between 180 and -180 degrees.",
                ),
            )

        if (
            north > self.latitude[0]
            or south < self.latitude[-1]
            or west < self.longitude[0]
            or east > self.longitude[-1]
        ):
            return Failure(
                ValueError(
                    "Cannot crop coordinates to a region outside the bounds of the map. "
                    f"Crop region '{north, west, south, east}' not in "
                    f"map bounds '{self.nwse()}'.",
                ),
            )

        # Determine the indices of the region in the latitude and longitude lists
        lat_indices = [i for i, lat in enumerate(self.latitude) if south <= lat <= north]
        lon_indices = [i for i, lon in enumerate(self.longitude) if west <= lon <= east]

        # Create a new map with the cropped coordinates
        return Success(
            dataclasses.replace(
                self,
                latitude=[self.latitude[i] for i in lat_indices],
                longitude=[self.longitude[i] for i in lon_indices],
            ),
        )

    def nwse(self) -> tuple[float, float, float, float]:
        """Return the north, west, south, and east bounds of the map.

        Returns:
            A tuple of the north, west, south, and east bounds of the map.
        """
        if self.latitude is not None and self.longitude is not None:
            return self.latitude[0], self.longitude[0], self.latitude[-1], self.longitude[-1]
        return 90.0, -180.0, -90.0, 180.0  # TODO: Cross this bridge when we come to it
