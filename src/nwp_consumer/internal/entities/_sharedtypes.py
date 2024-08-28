"""Type aliases used throughout the entities layer."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np


type LabelCoordinateDict = dict[  # type: ignore
    str, list[np.datetime64] | list[np.timedelta64] | list[str] | list[float],
]
"""Type alias for dictionary mapping dimension labels to index coordinate values."""
