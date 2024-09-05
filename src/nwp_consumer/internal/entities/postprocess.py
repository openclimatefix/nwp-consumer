"""Domain entities for post-processing."""

import dataclasses
from codecs import Codec


@dataclasses.dataclass(slots=True)
class PostProcessOptions:
    """Options for post-processing NWP data.

    The defaults for any option should be the null value,
    i.e. nothing occurs by default.
    """

    standardize_coordinates: bool = False
    """Whether to standardize the coordinates of the data.

    Note that this doesn't refer to interpolation: rather, it makes
    the coordinates adhere to the usual directionality and regionality
    within the circular space, i.e.:

    - Latitude values should be in the range and direction [+90, -90]
    - Longitude values should be in the range and direction [-180, 180]
    - Y values should be in descending order
    - X values should be in ascending order
    """

    rechunk: bool = False
    """Whether to rechunk the data."""

    validate: bool = False
    """Whether to validate the data.

    Note that for the moment, this is a very memory-intensive operation.
    Turn on only if there exists RAM to spare!
    """

    compressor: Codec | None = None
    """The compressor to use for the data."""

    zip: bool = False
    """Whether to zip the data."""

    plot: bool = False
    """Whether to save a plot of the data."""

