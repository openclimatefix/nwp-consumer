"""Domain entities for post-processing."""

import dataclasses
from codecs import Codec
from enum import Enum

import ocf_blosc2


class CodecOptions(Codec, Enum):
    """Options for compression codecs."""

    UNSET = None
    OCF_BLOSC2 = ocf_blosc2.Blosc2(clevel=5)
    """Use the OCF Blosc2 codec.

    See Also:
        - https://pypi.org/project/ocf-blosc2/
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

    By default, Zarr writes chunks compressed using the `Blosc compressor
    <https://zarr.readthedocs.io/en/stable/tutorial.html#compressors>`_.
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
