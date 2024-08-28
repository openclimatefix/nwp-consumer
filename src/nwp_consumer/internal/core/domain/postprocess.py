from enum import Enum
from dataclasses import dataclass, field


@dataclass
class AppendToArchiveOption(str, Enum):
    """Options for appending to an archive."""

    Unset = "unset"
    Monthly = "monthly"
    Yearly = "yearly"


@dataclass
class PostProcessOptions:
    """Options for post-processing NWP data.

    The defaults for any option should be the null value,
    i.e. nothing occurs by default.
    """

    append_to_archive: AppendToArchiveOption = field(
        default_factory=lambda: AppendToArchiveOption.Unset,
    )
    """Whether to append the init time dataset to a larger archive."""
