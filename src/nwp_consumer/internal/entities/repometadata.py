"""Domain classes for repository metadata.

Sources of NWP data have attributes both pertaining to and apart from
the data they deliver. This module defines classes for metadata that
tracks relevant information about the model repository and the data
it provides. This might be helpful in determining the quality of the
data, defining pipelines for processing, or establishing the availability
for a live service.

In this instance, the `ModelMetadata` refers to information pertaining
to the model used to generate the data itself, whilst the
`ModelRepositoryMetadata` refers to information about the repository
where NWP data produced by the model resides.
"""

import dataclasses
import datetime as dt
import os

import pandas as pd

from .coordinates import NWPDimensionCoordinateMap
from .postprocess import PostProcessOptions


@dataclasses.dataclass(slots=True)
class ModelMetadata:
    """Metadata for an NWP model."""

    name: str
    """The name of the model.

    Used to name the tensor in the zarr store.
    """

    resolution: str
    """The resolution of the model with units."""

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

    def __str__(self) -> str:
        """Return a pretty-printed string representation of the metadata."""
        pretty: str = "".join((
            "Model:",
            "\n\t{self.name} ({self.resolution} resolution)",
            "\tCoordinates:",
            "\n".join(
                f"\t\t{dim}: {vals}"
                if len(vals) < 5
                else f"\t\t{dim}: {vals[:3]} ... {vals[-3:]}"
                for dim, vals in self.expected_coordinates.__dict__.items()
            ),
        ))
        return pretty


@dataclasses.dataclass(slots=True)
class ModelRepositoryMetadata:
    """Metadata for an NWP Model repository."""

    name: str
    """The name of the model repository."""

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

    def month_its(self, year: int, month: int) -> list[dt.datetime]:
        """Generate all init times for a given month."""
        days = pd.Period(f"{year}-{month}").days_in_month
        its: list[dt.datetime] = []
        for day in range(1, days + 1):
            for hour in self.running_hours:
                its.append(dt.datetime(year, month, day, hour, tzinfo=dt.UTC))
        return its

    def missing_required_envs(self) -> list[str]:
        """Get a list of unset required environment variables.

        Returns:
            A list of missing environment variables.
        """
        return [var for var in self.required_env if var not in os.environ]

    def __str__(self) -> str:
        """Return a pretty-printed string representation of the metadata."""
        pretty: str = "".join((
            "Model Repository: ",
            f"\n\t{self.name} ({'archive' if self.is_archive else 'live/rolling'} dataset.)",
            f"\n\truns at: {self.running_hours} hours ",
            "(available after {self.delay_minutes} minute delay)",
            "\nEnvironment variables:",
            "\n\tRequired:",
            "\n".join(f"\t\t{var}" for var in self.required_env),
            "\n\tOptional:",
            "\n".join(f"\t\t{var}={val}" for var, val in self.optional_env.items()),
        ))
        return pretty
