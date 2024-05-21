"""Domain classes for NWP tensor data representation.

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
x value: [0, 1, 2, 3, 4]

y index: [0, 1, 2, 3, 4]
y value: [0 seconds, 30 seconds, 60 seconds, 90 seconds, 120 seconds]

Now by consulting the mapping we can see that the point (2, 4) in the tensor
represents that the runners time at lap two was 60 seconds.


This formalisation is useful also in the reverse case: inserting data into a
tensor according to its dimension coordinate values, and not its indexes.
This is the primary use case for these maps in this service.

It is far more likely that for incoming data the coordinate values along the
dimension axes are known, as opposed to the indexes the represent. This mapping
then enables insertion that data into the correct regions of the tensor, which is
a key part of parallel writing.


This module defines firstly an abstract base class to represent the shape of a
dimension map object, and thereafter concrete implementations of the dimension
map for different types of grid.
"""
import abc

import attrs
import numpy as np
from result import Err, Ok, Result


class TensorDimensionMap(abc.ABC):
    """Mapping of dimension labels to coordinate values for an NWP tensor.

    Can reasonably be thought of as a map of axis labels to the labels of
    each tick along that axis of a graph of the dataset.
    """

    @abc.abstractmethod
    def as_dict(self) -> dict[str, list]:
        """Return the dimension map as a dictionary."""
        pass

    def shape(self) -> dict[str, int]:
        """Return the shape specified by the dimension mapping.

        Shape does not make assumptions about the ordering of the dimensions,
        hence returning a dictionary instead of a tuple.

        Returns:
            Dictionary with keys corresponding to the coordinate names
            and values corresponding to the number of ticks along the
            coordinate axis.
        """
        return {k: len(v) for k, v in self.as_dict().items()}

    def as_slices_of(self, outer: "TensorDimensionMap") -> Result[dict[str, slice], str]:
        """Return the index slices of this mapping relative to the outer map.

        The calling instance is the "inner", that is, the smaller dimension map
        that is a subset of the argument-provided "outer" dimension map.
        A number of requirements must be met for this operation to be successful:

        - The inner must be a subset of the outer mapping along all dimensions.
        - The inner must be contiguous along each dimension.
        - The inner must be of the same dimension map type as the base map
          (i.e. must have exactly the same dimension labels).

        The returned dictionary of slices defines the region of the base map covered
        by the instances dimension mapping.

        Examples:
            Getting the inner map slices relative to the outer map:

            >>> outer = ISLLTensorDimensionMap(
            >>>    init_time=[np.datetime64("2021-01-01T00:00:00")],
            >>>    step=[np.timedelta64(i, "h") for i in range(48)],
            >>>    latitude=[68.0, 69.0, 70.0],
            >>>    longitude=[-10.0, -9.0, -8.0])
            >>>
            >>> inner = attrs.evolve(outer, step=[np.timedelta64(i, "h") for i in range(24)])
            >>>
            >>> inner.as_slices_of(outer)
            Ok({
                "init_time": slice(0, 1),
                "step": slice(0, 24),
                "latitude": slice(0, 3),
                "longitude": slice(0, 3),
            })

        Args:
            outer: The larger outer dimension map to slice against.

        Returns:
            Dictionary with keys corresponding to the coordinate names
            and values corresponding to the slices of the base map that
            are represented by the ticks along the instance's dimensions.
        """
        if outer.__class__.__name__ != self.__class__.__name__:
            return Err(
                f"Cannot find slices of <{self.__class__.__name__}> in non-matching dimension map "
                f"<{outer.__class__.__name__}>. Both objects must have identical dimensions "
                f"(rank and labels).",
            )

        slices = {}
        inner_dim_label: str
        inner_dim_coords: list
        for inner_dim_label, inner_dim_coords in self.as_dict().items():
            if not set(inner_dim_coords).issubset(set(getattr(outer, inner_dim_label))):
                return Err(f"Coordinate values for dimension <{inner_dim_label}> not all present "
                           f"within outer dimension map. The inner map must be entirely contained "
                           f"within the outer map along every dimension.")

            outer_dim_indices = sorted([getattr(outer, inner_dim_label).index(i) for i in inner_dim_coords])
            contiguous_index_run = list(range(outer_dim_indices[0], outer_dim_indices[-1] + 1))
            if outer_dim_indices != contiguous_index_run:
                return Err(f"Coordinate values for dimension <{inner_dim_label}> do not correspond "
                           f"with a contiguous index set in the outer dimension map. "
                           f"Got <{outer_dim_indices}>.")

            slices[inner_dim_label] = slice(outer_dim_indices[0], outer_dim_indices[-1] + 1)

        return Ok(slices)


@attrs.frozen
class ISLLTensorDimensionMap(TensorDimensionMap):
    """Mapping of dimension labels to coordinate values for an ISLL NWP tensor.

    The ISLL tensor has dimension labels and index maps according to
    the respective names and values of the class variables.
    """

    init_time: list[np.datetime64]
    """Initialization times of the forecast."""

    step: list[np.timedelta64]
    """Time steps of the forecast data."""

    latitude: list[float]
    """Latitude coordinates of the grid cells."""

    longitude: list[float]
    """Longitude coordinates of the grid cells."""

    def as_dict(self) -> dict[str, list]:  # noqa: D102
        return attrs.asdict(self)


@attrs.frozen
class ISXYTensorDimensionMap(TensorDimensionMap):
    """Mapping of dimension labels to coordinate values for an ISXY NWP tensor.

    The ISXY tensor has dimension labels and index maps according to
    the respective names and values of the class variables.
    """

    init_time: list[np.datetime64]
    """Initialization times of the forecast."""

    step: list[np.timedelta64]
    """Time steps of the forecast data."""

    x: list[float]
    """X coordinates of the grid cells."""

    y: list[float]
    """Y coordinates of the grid cells."""

    def as_dict(self) -> dict[str, list]:  # noqa: D102
        return attrs.asdict(self)


@attrs.frozen
class ISITensorDimensionMap(TensorDimensionMap):
    """Mapping of dimension labels to coordinate values for an ISI NWP tensor.

    The ISI tensor has dimension labels and index maps according to
    the respective names and values of the class variables.

    The ISI dataset is not gridded.
    """

    init_time: list[np.datetime64]
    """Initialization times of the forecast."""

    step: list[np.timedelta64]
    """Time steps of the forecast data."""

    station_id: list[int]
    """The station IDs of the data points."""

    def as_dict(self) -> dict[str, list]:  # noqa: D102
        return attrs.asdict(self)
