"""Implements a client to fetch data from Meteomatics."""
import datetime as dt
import pathlib

import meteomatics.api
import pandas as pd
import xarray as xr
from internal import FileInfoModel, OCFParameter

from nwp_consumer import internal

from ._models import (
    MeteomaticsFileInfo,
    solar_coords,
    solar_parameters,
    wind_coords,
    wind_parameters,
)


class Client(internal.FetcherInterface):
    """Client to fetch data from Meteomatics."""

    area: str
    resource_type: str

    # Authentication
    _username: str
    _password: str

    # Subscription-specific limits
    _subscription_max_requestable_parameters: int = 10
    _subscription_min_date = dt.datetime(2019, 3, 19, tzinfo=dt.UTC)

    def __init__(self, area: str, resource_type: str) -> None:
        """Initialize the client."""
        self.area = area
        self.resource_type = resource_type

    def datasetName(self) -> str:
        """Return the dataset name."""
        return f"meteomatics_{self.area}_{self.resource_type}"

    def getInitHours(self) -> list[int]:
        """Return the number of hours to initialize the data."""
        # This should follow ECMWF's initialization hours as we fetch the ECMWF-IFS data
        return [0, 6, 12, 18]

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[FileInfoModel]:
        """Overrides the corresponding method in the parent class."""
        return [
            MeteomaticsFileInfo(
                inittime=it,
                area=self.area,
                params=solar_parameters if self.resource_type == "solar" else wind_parameters,
                steplist=list(range(48)),
                sitelist=solar_coords if self.resource_type == "solar" else wind_coords,
            ),
        ]

    def downloadToCache(self, *, fi: FileInfoModel) -> pathlib.Path:
        """Query the Meteomatics API for NWP data and add to cache."""
        # Ensure subscription limits are respected
        # * Split the parameters into groups of max size
        groups = [
            fi.variables()[i: i + self._subscription_max_requestable_parameters]
            for i in range(0, len(fi.variables()), self._subscription_max_requestable_parameters)
        ]

        # Ensure the fileinfo is a MeteomaticsFileInfo
        if not isinstance(fi, MeteomaticsFileInfo):
            raise ValueError("FileInfoModel must be a MeteomaticsFileInfo instance.")

        dfs: list[pd.DataFrame] = []
        try:
            for param_group in groups:
                df: pd.DataFrame = meteomatics.api.query_time_series(
                    coordinate_list=fi.sites(),
                    startdate=max(fi.it(), self._subscription_min_date),
                    enddate=max(fi.it(), self._subscription_min_date) + dt.timedelta(hours=fi.steps()[-1]),
                    interval=dt.timedelta(minutes=15),
                    parameters=param_group,
                    username=self._username,
                    password=self._password,
                    model="ecmwf-ifs",
                )
                dfs.append(df)
        except Exception as e:
            raise RuntimeError(f"Failed to query the Meteomatics API: {e}") from e

        # Merge dataframes
        cdf: pd.DataFrame = dfs[0]
        if len(dfs) > 1:
            cdf = cdf.join(dfs[1:])

        # Save the dataframe to a file
        p: pathlib.Path = internal.rawCachePath(it=fi.it(), filename=fi.filename())
        cdf.to_csv(p)

        return p

    def mapCachedRaw(self, *, p: pathlib.Path) -> xr.Dataset:
        """Map DataFrame as CSV to xarray Dataset."""
        # Read the CSV file into a DataFrame
        try:
            df: pd.DataFrame = pd.read_csv(p, index_col=0, parse_dates=True)
        except Exception as e:
            raise RuntimeError(f"Failed to read CSV file: {e}") from e

        # Reset index to create columns for lat, lon, and validdate
        df = df.reset_index(level=["lat", "lon", "validdate"])
        # Create a station_id column based on the coordinates
        df["station_id"] = df.groupby(["lat", "lon"], sort=False).ngroup() + 1
        # Create a time_utc column based on the validdate
        df["time_utc"] = pd.to_datetime(df["validdate"])
        # Make a new index based on station_id and time_utc
        df = df.set_index(["station_id", "time_utc"]).drop(columns=["validdate"])
        # Create xarray dataset from dataframe
        ds = xr.Dataset.from_dataframe(df).set_coords(("lat", "lon"))
        # Ensure time_utc is a timestamp object
        ds["time_utc"] = pd.to_datetime(ds["time_utc"])

        return ds

    def parameterConformMap(self) -> dict[str, OCFParameter]:
        """Overrides the corresponding method in the parent class."""
        raise NotImplementedError()
