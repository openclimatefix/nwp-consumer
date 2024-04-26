"""Struct definitions for core domain objects.

Not every struct is a domain model! Only those involved in the business logic.
"""

import datetime as dt
import pathlib
from typing import Any

import attrs
import dask.array
import numpy as np
import pint
import xarray as xr


@attrs.frozen
class Parameter:
    """A parameter that can be requested from a source repository.

    Attributes:
        longname: The full name of the parameter.
        shortname: A short name for the parameter.
        units: The units of the parameter.
        level: The level of the parameter.
        levelunits: The units of the level.
    """

    longname: str = attrs.field(validator=attrs.validators.min_len(3))
    shortname: str = attrs.field(validator=attrs.validators.max_len(10))
    units: pint.Unit
    level: int = attrs.field(validator=attrs.validators.ge(0))
    levelunits: pint.Unit

    def __repr__(self) -> str:
        """Return a representation of the parameter."""
        levelrepr = f"_{self.level}{self.levelunits}" if self.level > 0 else "_agl"
        return f"{self.longname}{levelrepr}:{self.units}"

LCC = Parameter("low_cloud_cover", "lcc", pint.Unit("fraction"), 0, pint.Unit("meter"))
TEMPERATURE_2M = Parameter("temperature", "t", pint.Unit("kelvin"), 2, pint.Unit("meters"))


@attrs.frozen
class Area:
    """A geographical area, with bounding box in NWSE format.

    Lat/Long coordinates are in decimal degrees and must be given as signed floats as follows:
    - Latitudes north of the equator are *positive*.
    - Latitudes south of the equator are *negative*.
    - Longitudes east of the prime meridian are *positive*.
    - Longitudes west of the prime meridian are *negative*.
    """

    name: str
    north: float = attrs.field(validator=[attrs.validators.ge(-90), attrs.validators.le(90)])
    west: float = attrs.field(validator=[attrs.validators.ge(-180), attrs.validators.le(180)])
    south: float = attrs.field(validator=[attrs.validators.ge(-90), attrs.validators.le(90)])
    east: float = attrs.field(validator=[attrs.validators.ge(-180), attrs.validators.le(180)])

    def __str__(self) -> str:
        """String representation of the area."""
        return self.name + " (" + self.nwse() + ")"

    def nwse(self) -> str:
        """Return the bounding box as north/west/south/east string."""
        return "/".join([str(x) for x in (self.north, self.west, self.south, self.east)])

    def lats(self, resolution_degrees: float) -> list[float]:
        """Return the latitudes of the area."""
        return [self.north - i * resolution_degrees for i in range(self.nlats(resolution_degrees))]

    def lons(self, resolution_degrees: float) -> list[float]:
        """Return the longitudes of the area."""
        return [self.west + i * resolution_degrees for i in range(self.nlons(resolution_degrees))]

    def nlats(self, resolution_degrees: float) -> int:
        """Return the number of latitudes in the area at a given resolution."""
        # Add one to include the last latitude
        return round((self.north - self.south) / resolution_degrees) + 1

    def nlons(self, resolution_degrees: float) -> int:
        """Return the number of longitudes in the area at a given resolution."""
        # Add one to include the last longitude
        return round((self.east - self.west) / resolution_degrees) + 1

# Predefined areas
GLOBAL = Area("global", 90, -180, -90, 180)
EUROPE = Area("eu", 73.5, -27, 33, 45)
NW_INDIA = Area("nw_india", 31, 68, 20, 79)
UK = Area("uk", 62, -12, 48, 3)
MALTA = Area("malta", 37, 68, 20, 79)


@attrs.frozen
class SourceRepositoryMetadata:
    """Metadata for a source repository.

    Attributes:
        name: The name of the repository.
        is_archive: Whether the repository is a complete archival set.
            Archival datasets are able to be used to backfill old data.
            Non archival datasets only provide a limited window of data.
        is_order_based: Whether the repository is order-based.
            This means parameters cannot be chosen freely,
            but rather are defined by pre-selected agreements
            with the provider.
        running_hours: The running hours or the source.
        available_steps: The available steps of the repository.
        available_areas: The available areas of the repository.
        required_env: Environmant variables required for usage.
    """

    name: str = attrs.field(validator=attrs.validators.min_len(3), type=str)
    is_archive: bool
    is_order_based: bool
    running_hours: list[int]
    available_steps: list[int]
    available_areas: list[Area]
    required_env: list[str]

@attrs.frozen
class DataRequest:
    """A request for data for an init time from a source repository."""

    area: Area
    steps: list[int] = attrs.field(validator=attrs.validators.min_len(1))
    parameters: list[str] = attrs.field(validator=attrs.validators.min_len(1))
    init_time: dt.datetime

    def to_dummy_dataset(self, resolution_degrees: float) -> xr.Dataset:
        """Return a dummy dataset according to the request's shape.

        The dataset definition will contain only the coordinates and the parameters
        dependence on them. Storing it as a zarr using the `compute=False` arg
        will result in a zarr store containing only the metadata.
        Converted raw files can then be written in parallel to specific regions of the store.

        Dataset writes can never occur at a sub-chunk level, so in order to be able to
        perform parallel writes to the dataset, we need to ensure that the chunks are
        such to cover any possible raw data write we could reasonably expect from a store.
        the following assumptions are made:
        - Raw data files will always contain the full grid of data, hence 1 chunk per
          grid coordinate axis is sufficient.
        - Raw data files may contain as little as one step, so equate the number of chunks
          to the number of steps.
        """
        coords = self.ds_coords(resolution_degrees)
        shape = self.shape(resolution_degrees)

        data_vars = {
            p: (
                    ("init_time", "step", "latitude", "longitude"),
                    dask.array.zeros(shape, chunks=(1, len(self.steps), 1, 1),
                ),
            ) for p in self.parameters
        }

        return xr.Dataset(data_vars=data, coords=coords)

    def ds_coords(self, resolution_degrees: float) -> dict[str, list[Any]]:
        """Return the request as a dictionary of dataset coordinates."""
        return {
            # Convert to UTC and remove timezone info to prevent numpy complaints
            "init_time": [
                np.datetime64(self.init_time.astimezone(tz=dt.UTC).replace(tzinfo=None), "ns"),
            ],
            # Manually specify as timedelta64[ns] to prevent xarray complaints
            "step": [np.timedelta64(np.timedelta64(i, "h"), "ns") for i in self.steps],
            "latitude": self.area.lats(resolution_degrees),
            "longitude": self.area.lons(resolution_degrees),
        }

    def nvals(self, resolution_degrees: float) -> int:
        """Return the number of values in the request."""
        return (
            len(self.steps)
            * self.area.nlats(resolution_degrees)
            * self.area.nlons(resolution_degrees)
            * len(self.parameters)
        )

    def shape(self, resolution_degrees: float) -> dict[str, int]:
        """Return the shape of the request."""
        return {k: len(v) for k, v in self.ds_coords(resolution_degrees).items()}


@attrs.frozen
class SourceFileMetadata:
    """Metadata for a raw file."""

    name: str
    path: pathlib.Path
    extension: str
    size: int
    steps: list[int]
    parameters: list[str]
    init_time: dt.datetime
