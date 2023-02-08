"""Describes the business use case of fetching a dataset for a specific initTime."""

import numpy as np
import xarray as xr
import datetime as dt
import numpy.typing as npt
import structlog
import pathlib
from ocf_blosc2 import Blosc2

from src.nwp_consumer import internal

log = structlog.stdlib.get_logger()


def updateMonthlyZarrForInitTime(client: internal.ClientInterface, initTime: dt.datetime) -> None:
    """Fetches a dataset for the given initTime from the given client."""

    dataset = client.getDatasetForInitTime(initTime=initTime)

    chunkedDataset = _createChunkedDaskDataset(dataset)

    # Append to the monthly Zarr store the new dataset

    zarrFileName: str = f"UKV-{initTime.strftime('%Y%m')}"

    # TODO: Inject Zarr Path
    save_path: pathlib.Path = pathlib.Path(__file__).parents[4].resolve() / "downloads"

    zarrPath: pathlib.Path = save_path / zarrFileName

    _append_to_zarr(dataset=chunkedDataset, zarr_path=zarrPath)


def _createChunkedDaskDataset(ds: xr.Dataset) -> xr.Dataset:
    """Create a chunked Dask Dataset from the input multi-variate Dataset.

    Converts the input multivariate DataSet (with different DataArrays for
    each NWP variable) to a single DataArray with a `variable` dimension.
    This allows each Zarr chunk to hold multiple variables (useful for loading
    many/all variables at once from disk)."""

    # Create single-variate dataarray from dataset, with new "variable" dimension
    da = ds.to_array(dim="variable", name="UKV")
    del ds

    return (
        da.to_dataset().chunk(
            {
                "init_time": 1,
                "step_time": 1,
                "variable": -1,
            })
    )


def _append_to_zarr(dataset: xr.Dataset, zarr_path: pathlib.Path) -> None:
    """If zarr_path already exists then append to the init_time dim.  Else create a new Zarr.
    If creating a new Zarr, then this function sets the units for representing time to
    "nanoseconds since 1970-01-01" (which is the encoding used by `numpy.datetime64[ns]`) otherwise,
    by default, xarray defaults representing time as an integer numbers of *days* and hence cannot
    represent sub-day temporal resolution and corrupts the `init_time` values when we
    append to Zarr.  See:
        https://github.com/pydata/xarray/issues/5969   and
        http://xarray.pydata.org/en/stable/user-guide/io.html#time-units
    Also sets the compressor to `numcodecs.Blosc(cname="zstd", clevel=5)` which has been shown
    to provide a good balance of speed and small file sizes in empirical testing.
    """
    zarr_path = pathlib.Path(zarr_path)
    if zarr_path.exists():
        # Append to existing Zarr store.
        to_zarr_kwargs = dict(
            append_dim="init_time",
        )

        # Check that dataset has same dimensions as the dataset on disk:
        assert len(dataset["step_time"]) == 37
    else:
        # Create new Zarr store.
        to_zarr_kwargs = dict(
            encoding={
                "init_time": {"units": "nanoseconds since 1970-01-01"},
                "UKV": {
                    "compressor": Blosc2(cname="zstd", clevel=5),
                },
            },
        )
    dataset["UKV"] = dataset.astype(np.float16)["UKV"]
    dataset.to_zarr(zarr_path, **to_zarr_kwargs)


def removeDataWhereStepDiffIsNot1Hour(ds: xr.Dataset) -> xr.Dataset:
    """Return the slice of the dataset where the step coordinate changes uniformly by hourly increments.

    Drops data beyond the point at which the step coordinate's adjacent step difference is
    not 1 hour."""

    # Access the np.ndarray of the coordinate values
    coord_values: npt.NDArray[np.timedelta64] = ds.coords['step'].values
    # Create an array of booleans specifying whether the
    # differences between the coord values is equal to diff
    coord_diff_is_1hr: npt.NDArray[np.bool_] = np.diff(coord_values) == np.timedelta64(1, 'h')

    # If False is in the array, there exists non-hourly steps
    if False in coord_diff_is_1hr:
        # Find the first instance of False in the above array, must use "==" here
        index_where_diff_is_not_1hour = np.where(coord_diff_is_1hr == False)[0][0]

        # Drop the data that does not change uniformly
        ds = ds.sel({
            'step': slice(coord_values[0], coord_values[index_where_diff_is_not_1hour])
        }, drop=True)

    return ds
