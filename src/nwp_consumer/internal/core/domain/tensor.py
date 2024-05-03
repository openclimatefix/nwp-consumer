import attrs
import numpy as np
from result import Err, Ok, Result


@attrs.define
class DatasetDimensionMap:
    """Mapping of dimension labels to coordinate values for an NWP dataset.

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

    def as_slices_of(self, base: "DatasetDimensionMap") -> Result[dict[str, slice], str]:
        """Return the index slices of this mapping with regards to the base map.

        A number of requirements must be met for this operation to be successful:
        - The instance must be a subset of the base mapping.
        - The subset must be contiguous along each dimension.
        - The subset must be of the same dimension map type as the base map.

        The returned dictionary of slices defines the region of the base map covered
        by the instances dimension mapping.

        Args:
            base: The base dimension map to slice against.

        Returns:
            Dictionary with keys corresponding to the coordinate names
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
class ISLLDatasetDimensionMap(DatasetDimensionMap):
    """Mapping of dimension labels to coordinate values for an ISLL NWP dataset.

    The ISLL dataset is a dataset with the following dimensions:
    - init_time: The initial time of the forecast.
    - step: The time step of the forecast.
    - latitude: The latitude of the grid cell.
    - longitude: The longitude of the grid cell.
    """

    init_time: list[np.datetime64]
    step: list[np.timedelta64]
    latitude: list[float]
    longitude: list[float]


@attrs.frozen
class ISXYDatasetDimensionMap(DatasetDimensionMap):
    """Mapping of dimension labels to coordinate values for an ISXY NWP dataset.

    The ISXY dataset is a dataset with the following dimensions:
    - init_time: The initial time of the forecast.
    - step: The time step of the forecast.
    - x: The x coordinate of the grid cell.
    - y: The y coordinate of the grid cell.
    """

    init_time: list[np.datetime64]
    step: list[np.timedelta64]
    x: list[float]
    y: list[float]


@attrs.frozen
class ISIDatasetDimensionMap(DatasetDimensionMap):
    """Mapping of dimension labels to coordinate values for an ISI NWP dataset.

    The ISI dataset is a dataset with the following dimensions:
    - init_time: The initial time of the forecast.
    - step: The time step of the forecast.
    - station_id: The id of the station.

    The ISI dataset is not gridded.
    """

    init_time: list[np.datetime64]
    step: list[np.timedelta64]
    station_id: list[int]
