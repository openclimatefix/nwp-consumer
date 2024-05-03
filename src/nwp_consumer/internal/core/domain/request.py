import datetime as dt

import attrs
import dask.array
import numpy as np
import xarray as xr

from .area import Area
from .tensor import (
    DatasetDimensionMap,
    ISLLDatasetDimensionMap,
)


@attrs.frozen
class DataRequest:
    """A request for data for an init time from a source repository."""

    area: Area
    steps: list[int] = attrs.field(validator=attrs.validators.min_len(1))
    parameters: list[str] = attrs.field(validator=attrs.validators.min_len(1))
    init_time: dt.datetime

    def as_dataset(self, resolution_degrees: float) -> xr.Dataset:
        """Return a dummy dataset according to the request.

        The request is used to define the dimension labels and tick values of the output
        dataset object, as well as the data variables tracked within the dataset and
        their dependence on the dimensions.

        No actual data is defined on the produced dataset. As such, storing it as a zarr via
        ```
        dataset.to_zarr('dummy.zarr', compute=False)
        ```
        will result in a zarr store containing the metadata alone. The utility of this is
        to enable region-based writing of new data to the store, which can be done using
        parallel processes.

        There is a gotcha: regional writes can never be done in parallel to the same chunk,
        so writes must always be done at the chunk level or higher (as a chunk is an
        individual file in the store). In this manner chunks are chosen to cover as small
        a unit of data as could reasonbaly be expected to be provided by an NWP source:
        - Raw data files will always contain the full grid of data, hence 1 chunk per
          grid dimension (lat/lon/x/y axes) is sufficient.
        - Raw data files may contain as little as one step for a single parameter, so equate
          the number of chunks to the number of steps along the step dimension.
        """
        coords: DatasetDimensionMap = self.as_isll_dataset_dimension_map(resolution_degrees)
        data_vars = {
            p: (
                ("init_time", "step", "latitude", "longitude"),
                dask.array.zeros(coords.shape(), chunks=(1, len(self.steps), 1, 1)),
            ) for p in self.parameters
        }

        return xr.Dataset(data_vars=data_vars, coords=attrs.asdict(coords))

    def as_isll_dataset_dimension_map(self, resolution_degrees: float) -> ISLLDatasetDimensionMap:
        """Return the request as a mapping of dataset dimension labels to values."""
        return ISLLDatasetDimensionMap(
            # Convert to UTC and remove timezone info to prevent numpy complaints
            init_time=[
                np.datetime64(self.init_time.astimezone(tz=dt.UTC).replace(tzinfo=None), "ns"),
            ],
            # Manually specify as timedelta64[ns] to prevent xarray complaints
            step=[np.timedelta64(np.timedelta64(i, "h"), "ns") for i in self.steps],
            latitude=self.area.lats(resolution_degrees),
            longitude=self.area.lons(resolution_degrees),
        )

    def total_values(self, resolution_degrees: float) -> int:
        """Return the total number of data points specified by the request definition."""
        num: int = len(self.steps) \
                   * self.area.nlats(resolution_degrees) \
                   * self.area.nlons(resolution_degrees) \
                   * len(self.parameters)
        return num
