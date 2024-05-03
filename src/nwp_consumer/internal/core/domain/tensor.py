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
"""

import attrs
import numpy as np
from result import Err, Ok, Result


@attrs.define
class TensorDimensionMap:
    """Mapping of dimension labels to coordinate values for an NWP tensor.

    Can reasonably be thought of as a map of axis labels to the labels of
    each tick along that axis of a graph of the dataset.
    """

    def asdict(self) -> dict[str, list]:
        """Return the dimension map as a dictionary."""
        return attrs.asdict(self)

    def shape(self) -> dict[str, int]:
        """Return the shape specified by the dimension mapping.

        Returns:
            Dictionary with keys corresponding to the coordinate names
            and values corresponding to the number of ticks along the
            coordinate axis.
        """
        return {k: len(v) for k, v in self.asdict().items()}

    def as_slices_of(self, base: "TensorDimensionMap") -> Result[dict[str, slice], str]:
        """Return the index slices of this mapping with regards to the base map.

        A number of requirements must be met for this operation to be successful:
        - The instance must be a subset of the base mapping.
        - The subset must be contiguous along each dimension.
        - The subset must be of the same dimension map type as the base map.

        The returned dictionary of slices defines the region of the base map covered
        by the instances dimension mapping.

        :param base: The base dimension map to slice against.

        :return: Dictionary with keys corresponding to the coordinate names
            and values corresponding to the slices of the base map that
            are represented by the ticks along the instance's dimensions.
        """
        if type(base) != type(self):
            return Err(
                f"Cannot find slices of {type(self)} in non-matching dimension map type {type(base)}.",
            )

        slices = {}
        k: str
        v: list
        for k, v in self.asdict().items():
            if not set(v).issubset(set(getattr(base, k))):
                return Err(f"Subset {k} dimension values not in base dimension map.")
            k_indices = sorted([getattr(base, k).index(i) for i in v])
            if k_indices != list(range(k_indices[0], k_indices[-1])):
                return Err(f"Subset {k} values not contiguous in base dimension map.")
            slices[k] = slice(k_indices[0], k_indices[-1] + 1)

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
