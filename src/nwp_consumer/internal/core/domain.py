"""Struct definitions for core domain objects."""

from dataclasses import dataclass
import datetime as dt
import pathlib

@dataclass
class RawRepositoryMetadata:
    """Metadata for a raw repository."""

    #
    is_archive: bool
    is_order_based: bool
    running_hours: list[int]
    available_steps: list[int]
    available_areas: list[str]


class RawFileMetadata:
    """Metadata for a raw file."""

    name: str
    size: int
    steps: list[int]
    parameters: dict[str, str]
    init_time: dt.datetime

