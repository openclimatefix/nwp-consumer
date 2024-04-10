"""Struct definitions for core domain objects.

Not every struct is a domain model! Only those involved in the business logic.
"""

import datetime as dt
import pathlib
from enum import Enum

import attrs


class Area(str, Enum):
    """Requestable areas of data."""

    GLOBAL = "global"
    EUROPE = "eu"
    NW_INDIA = "nw_india"
    UK = "uk"

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
    steps: list[int]
    parameters: list[str]
    init_time: dt.datetime

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

