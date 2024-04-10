"""Struct definitions for core domain objects."""

import datetime as dt
import pathlib

import attrs


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
    available_areas: list[str]


@attrs.frozen
class SourceFileMetadata:
    """Metadata for a raw file."""

    name: str
    size: int
    steps: list[int]
    parameters: dict[str, str]
    init_time: dt.datetime

@attrs.frozen
class CachedSourceFileMetadata(SourceFileMetadata):
    """Metadata for a cached file."""

    path: pathlib.Path


