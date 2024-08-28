"""Domain entities for post-processing."""

import dataclasses
from enum import Enum


class AppendToArchiveOption(str, Enum):
    """Options for appending to an archive."""

    Unset: str = "unset"
    Monthly: str = "monthly"
    Yearly: str = "yearly"


@dataclasses.dataclass(slots=True)
class PostProcessOptions:
    """Options for post-processing NWP data.

    The defaults for any option should be the null value,
    i.e. nothing occurs by default.
    """

    append_to_archive: AppendToArchiveOption = AppendToArchiveOption.Unset
    """Whether to append the init time dataset to a larger archive."""
