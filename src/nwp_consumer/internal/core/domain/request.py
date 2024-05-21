"""Domain model for a request for data from a source repository.

NWP data is multidimensional, and as such can be very large. Sources of NWP
data that allow customization of the request will limit the data returned to
a subset of the available dimension coordinates to better track and limit the
amount of data returned.

This module defines a `DataRequest` object that encapsulates the desired, or
expected, data returned to the requester from the source.
"""


import datetime as dt

import attrs
import dask.array
import numpy as np
import xarray as xr

from .area import Area
from .parameter import Parameter
from .tensor import (
    ISLLTensorDimensionMap,
    ISVTensorDimensionMap,
)


@attrs.frozen
class DataRequest:
    """A request for data for an init time from a source repository."""

    area: Area
    """The desired Area of the data."""

    steps: list[int] = attrs.field(validator=attrs.validators.min_len(1))
    """The desired steps of the data."""

    parameters: list[Parameter] = attrs.field(validator=attrs.validators.min_len(1))
    """The desired parameters of the data."""

    init_time: dt.datetime
    """The init time of the data."""

    def as_dataset(self, resolution_degrees: float) -> xr.Dataset:
        """Return a dummy dataset according to the request.

        The request is used to define the dimension labels and tick values of the output
        dataset object, as well as the data variables tracked within the dataset and
        their dependence on the dimensions.

        No actual data is defined on the produced dataset. As such, storing it as a zarr via::

            dataset.to_zarr('dummy.zarr', compute=False)

        will result in a zarr store containing the metadata alone. The utility of this is
        to enable region-based writing of new data to the store, which can be done using
        parallel processes.

        There is a gotcha: regional writes can never be done in parallel to the same chunk,
        so writes must always be done at the chunk level or higher (as a chunk is an
        individual file in the store). In this manner chunks are chosen to cover as small
        a unit of data as could reasonably be expected to be provided by an NWP source:

        - Raw data files will always contain the full grid of data, hence 1 chunk per
          grid dimension (lat/lon/x/y axes) is sufficient.
        - Raw data files may contain as little as one step for a single parameter, so equate
          the number of chunks to the number of steps along the step dimension.

        Args:
            resolution_degrees: The resolution of the grid in degrees.

        See Also:
            - https://docs.xarray.dev/en/v2023.10.1/user-guide/io.html#appending-to-existing-zarr-stores
        """
        coords: ISLLTensorDimensionMap = self.as_isll_dataset_dimension_map(resolution_degrees)
        data_vars = {
            p: (
                ("init_time", "step", "latitude", "longitude"),
                dask.array.zeros(coords.shape(), chunks=(1, len(self.steps), 1, 1)),
            ) for p in self.parameters
        }

        return xr.Dataset(data_vars=data_vars, coords=coords.as_dict())

    def as_isll_dataset_dimension_map(self, resolution_degrees: float) -> ISLLTensorDimensionMap:
        """Return the request as an ISLL mapping of dataset dimension labels to values.

        Args:
            resolution_degrees: The resolution of the lat/long grid in degrees.

        Returns:
            The ISLL dimension map for the request.
        """
        return ISLLTensorDimensionMap(
            init_time=[self.init_time],
            step=self.steps,
            latitude=self.area.lats(resolution_degrees),
            longitude=self.area.lons(resolution_degrees),
        )

    def as_isv_dataset_dimension_map(self) -> ISVTensorDimensionMap:
        """Return the request as an ISV mapping of dataset dimension labels to values.

        Returns:
            The ISV dimension map for the request.
        """
        return ISVTensorDimensionMap(
            init_time=[self.init_time],
            step=self.steps,
            values=self.parameters,
        )

    def total_values(self, resolution_degrees: float) -> int:
        """Return the total number of data points specified by the request definition.

        Args:
            resolution_degrees: The resolution of the lat/long grid in degrees.
        """
        num: int = len(self.steps) \
                   * self.area.nlats(resolution_degrees) \
                   * self.area.nlons(resolution_degrees) \
                   * len(self.parameters)
        return num
