"""Domain classes for repository metadata.

Sources of NWP data have attributes both pertaining to and apart from
the data they deliver. This module defines classes for metadata that
tracks relevant information about the model repository and the data
it provides. This might be helpful in determining the quality of the
data, defining pipelines for processing, or establishing the availability
for a live service.

In this instance, the `RawRepositoryMetadata` refers to information
about the repository where NWP data produced by the model resides.
"""

import dataclasses
import datetime as dt
import os

from .modelmetadata import ModelMetadata
from .postprocess import PostProcessOptions


@dataclasses.dataclass(slots=True)
class RawRepositoryMetadata:
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

    available_models: dict[str, ModelMetadata]
    """A dictionary of available models and their metadata."""

    def determine_latest_it_from(self, t: dt.datetime, running_hours: list[int]) -> dt.datetime:
        """Determine the latest available initialization time from a given time.

        Args:
            t: The time from which to determine the latest initialization time.
            running_hours: A list of hours at which the model runs each day.

        Returns:
            The latest available initialization time prior to the given time.
        """
        it = (
            t.replace(minute=0, second=0, microsecond=0) - dt.timedelta(minutes=self.delay_minutes)
        ).replace(minute=0)
        while it.hour not in running_hours:
            it -= dt.timedelta(hours=1)

        return it

    def missing_required_envs(self) -> list[str]:
        """Get a list of unset required environment variables.

        Returns:
            A list of missing environment variables.
        """
        return [var for var in self.required_env if var not in os.environ]

    def __str__(self) -> str:
        """Return a pretty-printed string representation of the metadata."""
        pretty: str = "".join(
            (
                "Model Repository: ",
                f"\n\t{self.name} ({'archive' if self.is_archive else 'live/rolling'} dataset.)",
                f"\n\t\t(available after {self.delay_minutes} minute delay)",
                "\nEnvironment variables:",
                "\n\tRequired:",
                "\n".join(f"\t\t{var}" for var in self.required_env),
                "\n\tOptional:",
                "\n".join(f"\t\t{var}={val}" for var, val in self.optional_env.items()),
            ),
        )
        return pretty
