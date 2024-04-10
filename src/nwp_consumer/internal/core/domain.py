"""Struct definitions for core domain objects."""

import datetime as dt
import pathlib

import attrs


@attrs.frozen
class RawRepositoryMetadata:
    """Metadata for a raw repository."""

    #
    is_archive: bool
    is_order_based: bool
    running_hours: list[int]
    available_steps: list[int]
    available_areas: list[str]


@attrs.frozen
class RawFileMetadata:
    """Metadata for a raw file."""

    name: str
    size: int
    steps: list[int]
    parameters: dict[str, str]
    init_time: dt.datetime

@attrs.frozen
class CachedFileMetadata(RawFileMetadata):
    """Metadata for a cached file."""

    path: pathlib.Path


