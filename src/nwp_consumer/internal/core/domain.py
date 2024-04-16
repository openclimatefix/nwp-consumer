"""Struct definitions for core domain objects.

Not every struct is a domain model! Only those involved in the business logic.
"""

import datetime as dt
import pathlib
from enum import Enum
from typing import Any

import attrs
import numpy as np


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
    """

    name: str
    is_archive: bool
    is_order_based: bool
    running_hours: list[int]
    available_steps: list[int]
    available_areas: list[Area]

@attrs.frozen
class DataRequest:
    """A request for data from a source repository."""

    area: Area
    steps: list[int] = attrs.field(validator=attrs.validators.min_len(1))
    parameters: list[str] = attrs.field(validator=attrs.validators.min_len(1))
    init_time: dt.datetime

    def ds_coords(self) -> dict[str, list[Any]]:
        """Return the request as a dictionary of dataset coordinates."""
        return {
            # Convert to UTC and remove timezone info to prevent numpy complaints
            "init_time": [np.datetime64(self.init_time.astimezone(tz=dt.UTC).replace(tzinfo=None), "ns")],
            # Manually specify as timedelta64[ns] to prevent xarray complaints
            "step": [np.timedelta64(np.timedelta64(i, "h"), "ns") for i in self.steps],
            "latitude": self.area.lats(resolution_degrees=0.1),
            "longitude": self.area.lons(resolution_degrees=0.1),
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
        return {k: len(v) for k, v in self.ds_coords().items()}


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
