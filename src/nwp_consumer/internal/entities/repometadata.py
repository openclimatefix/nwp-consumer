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
from .postprocess import PostProcessOptions


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

    max_connections: int
    """The maximum number of simultaneous connections allowed to the model repository.

    This determines the maximum level of concurrency that can be achieved when
    downloading data from the repository.
    """

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

    To work this out, it can be useful to use the 'grib_ls' tool from eccodes:

    >>> grib_ls -n geography -wcount=13 raw_file.grib

    Which prints grid data from the grib file.
    """

    postprocess_options: PostProcessOptions
    """Options for post-processing the data."""

    def determine_latest_it_from(self, t: dt.datetime) -> dt.datetime:
        """Determine the latest available initialization time from a given time.

        Args:
            t: The time from which to determine the latest initialization time.

        Returns:
            The latest available initialization time prior to the given time.
        """
        it = t.replace(minute=0, second=0, microsecond=0) \
             - dt.timedelta(minutes=self.delay_minutes)
        while it.hour not in self.running_hours:
            it -= dt.timedelta(hours=1)

        return it

    def __str__(self) -> str:
        """Return a pretty-printed string representation of the metadata."""
        pretty: str = "\n".join((
            f"Model: {self.name} ({'archive' if self.is_archive else 'live/rolling'} dataset.)",
            f"\truns at: {self.running_hours} hours (available after {self.delay_minutes} minute delay)",
            "\tCoordinates:",
            "\n".join(f"\t\t{dim}: {vals}"
                      if len(vals) < 5
                      else f"\t\t{dim}: {vals[:3]} ... {vals[-3:]}"
                      for dim, vals in self.expected_coordinates.items()
            ),
            "Environment variables:",
            "\tRequired:",
            "\n".join(f"\t\t{var}" for var in self.required_env),
            "\tOptional:",
            "\n".join(f"\t\t{var}={val}" for var, val in self.optional_env.items()),
        ))
        return pretty

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

