"""Domain classes for repository metadata.

Sources of NWP data have attributes both pertaining to and apart from
the data they deliver. This module defines classes for metadata that
tracks relevant information about the source repository and the data
it provides. This might be helpful in determining the quality of the
data, defining pipelines for processing, or establishing the availability
for a live service.
"""

import datetime as dt
import pathlib

import attrs

from .area import Area
from .parameter import Parameter


@attrs.frozen
class SourceRepositoryMetadata:
    """Metadata for a source repository."""

    name: str = attrs.field(validator=attrs.validators.min_len(3))
    """The name of the repository."""

    is_archive: bool
    """Whether the repository is a complete archival set.

    Archival datasets are able to backfill old data.
    Non-archival datasets only provide a limited window of data.
    """

    is_order_based: bool
    """Whether the repository is order-based.

    This means parameters cannot be chosen freely,
    but rather are defined by pre-selected agreements with the provider.
    """

    running_hours: list[int]
    """The running hours or the source."""

    delay_minutes: int
    """The delay in minutes between init time and the time to which the data is actually available.
    """

    available_steps: list[int]
    """The available steps of the repository."""

    available_areas: list[Area]
    """The available areas of the repository."""

    required_env: list[str]
    """Environment variables required for usage."""

    optional_env: dict[str, str]
    """Optional environment variables."""


@attrs.frozen
class SourceFileMetadata:
    """Metadata for a raw file."""

    name: str
    """The name of the file."""

    path: pathlib.Path
    """The relevant (remote or local) path to the file."""

    extension: str
    """The file extension, including the dot (e.g. '.grib')."""

    size_bytes: int
    """The size of the file in bytes."""

    steps: list[int]
    """The steps within the file."""

    parameters: list[Parameter]
    """The parameters within the file."""

    init_time: dt.datetime
    """The init time of the file."""


@attrs.frozen
class PostProcessOptions:
    """Options for post-processing NWP data."""

    create_variable_dimension: bool = False
    """Whether to create a variable dimension.

    Squashes all data variables into their own "variable" dimension.
    """

    rename_variables: bool = False
    """Whether to rename variables."""
