"""Domain classes for repository metadata.

Sources of NWP data have attributes both pertaining to and apart from
the data they deliver. This module defines classes for metadata that
tracks relevant information about the model repository and the data
it provides. This might be helpful in determining the quality of the
data, defining pipelines for processing, or establishing the availability
for a live service.
"""

import dataclasses
import datetime as dt
import pathlib

from .coordinates import NWPDimensionCoordinateMap


@dataclasses.dataclass(slots=True)
class ModelRepositoryMetadata:
    """Metadata for an NWP Model repository."""

    name: str
    """The name of the model.

    Also used to name the tensor in the zarr store.
    """

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
    """The running hours of the model.

    Most NWP models are run at fixed intervals throughout the day."""

    delay_minutes: int
    """The approximate model delay in minutes.

    This delay is the time between the running of the model and the time
    at which the data is actually available."""

    required_env: list[str]
    """Environment variables required for usage."""

    optional_env: dict[str, str]
    """Optional environment variables."""

    expected_coordinates: NWPDimensionCoordinateMap
    """The expected dimension coordinate mapping.

    This is a dictionary mapping dimension labels to their coordinate values,
    for a single init time dataset, e.g.

    >>> {
    >>>     "init_time": [dt.datetime(2021, 1, 1, 0, 0), ...],
    >>>     "step": [1, 2, ...],
    >>>     "latitude": [90, 89.75, 89.5, ...],
    >>>     "longitude": [180, 179, ...],
    >>> }
    """


@dataclasses.dataclass(slots=True)
class ModelFileMetadata:
    """Metadata for a raw file."""

    name: str
    """The name of the file."""

    path: pathlib.Path
    """The relevant (remote or local) path to the file."""

    scheme: str
    """The scheme of the path (e.g. 'https', 'ftp', 'file')."""

    extension: str
    """The file extension, including the dot (e.g. '.grib')."""

    size_bytes: int
    """The size of the file in bytes."""

    parameters: list[str]
    """The parameters within the file."""

    steps: list[int]
    """The steps contained in the file."""

    init_time: dt.datetime
    """The init time the file data corresponds to."""

