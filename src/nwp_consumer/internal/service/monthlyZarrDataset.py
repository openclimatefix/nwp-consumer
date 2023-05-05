"""Describes the business use case of fetching a dataset for a specific initTime."""

import datetime as dt
import pathlib

import numpy as np
import numpy.typing as npt
import structlog
import xarray as xr

from src.nwp_consumer import internal

log = structlog.stdlib.get_logger()


def CreateMonthlyZarrDataset(
        fetcher: internal.FetcherInterface,
        storer: internal.StorageInterface,
        startDate: dt.date,
        endDate: dt.date) -> None:
    """Fetches a dataset for each initTime in the given time range and saves it to the given store."""
    # Get model init times for each day in the given time range
    for d in (startDate + dt.timedelta(days=n) for n in range((endDate - startDate).days + 1)):

        zarrFilename: pathlib.Path = pathlib.Path(f"UKV-{d.strftime('%Y%m')}.zarr")

        for initTime in [dt.datetime(year=d.year, month=d.month, day=d.day, hour=x) for x in range(0, 24, 6)]:

            # TODO - multiprocessing

            if storer.existsInZarrDir(relativePath=zarrFilename):

                # TODO - check if data already exists in zarr store for this initTime
                # If it does, skip to next initTime
                dataset = fetcher.getDatasetForInitTime(initTime=initTime)
                storer.appendDataset(dataset=dataset, relativePath=zarrFilename)

            else:
                dataset = fetcher.getDatasetForInitTime(initTime=initTime)
                storer.saveDataset(dataset=dataset, relativePath=zarrFilename)


def removeDataWhereStepDiffIsNot1Hour(ds: xr.Dataset) -> xr.Dataset:
    """Return the slice of the dataset where the step coordinate changes uniformly by hourly increments.

    Drops data beyond the point at which the step coordinate's adjacent step difference is
    not 1 hour.
    """
    # Access the np.ndarray of the coordinate values
    coord_values: npt.NDArray[np.timedelta64] = ds.coords['step'].values
    # Create an array of booleans specifying whether the
    # differences between the coord values is equal to diff
    coord_diff_is_1hr: npt.NDArray[np.bool_] = np.diff(coord_values) == np.timedelta64(1, 'h')

    # If False is in the array, there exists non-hourly steps
    if False in coord_diff_is_1hr:
        # Find the first instance of False in the above array, must use "==" here
        index_where_diff_is_not_1hour = np.where(coord_diff_is_1hr is False)[0][0]

        # Drop the data that does not change uniformly
        ds = ds.sel({
            'step': slice(coord_values[0], coord_values[index_where_diff_is_not_1hour])
        }, drop=True)

    return ds
