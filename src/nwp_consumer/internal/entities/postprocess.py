"""Domain entities for post-processing."""

import dataclasses
from enum import Enum

from zarr.codecs import BloscCodec


class CodecOptions(Enum):
    """Options for Zarr v3 compression codecs."""

    UNSET = None
    BLOSC = BloscCodec(cname="blosclz", clevel=5, shuffle="shuffle")
    """Use the Blosc codec.

    See Also:
        - https://zarr.readthedocs.io/en/stable/api/zarr/codecs/index.html
    """

    def __bool__(self) -> bool:
        """Boolean indicating whether a codec is set."""
        return self != CodecOptions.UNSET


@dataclasses.dataclass(slots=True)
class PostProcessOptions:
    """Options for post-processing NWP data.

    The defaults for any option should be the null value,
    i.e. nothing occurs by default.
    """

    validate: bool = False
    """Whether to validate the data.

    Note that for the moment, this is a very memory-intensive operation.
    Turn on only if there exists RAM to spare!
    """

    codec: CodecOptions = CodecOptions.UNSET
    """Whether to compress the data with a non-standard codec.

    By default, Zarr v3 writes chunks compressed using the Zstd codec.
    """

    plot: bool = False
    """Whether to save a plot of the data."""

    def requires_rewrite(self) -> bool:
        """Boolean indicating whether the specified options necessitate a rewrite."""
        return any(
            [
                self.codec,
            ],
        )

    def requires_postprocessing(self) -> bool:
        """Boolean indicating whether the specified options necessitate post-processing."""
        return any(
            [
                self.validate,
                self.codec,
                self.plot,
            ],
        )
