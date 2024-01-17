"""Output modules the consumer can write to."""

from . import (
    huggingface,
    localfs,
    s3,
)

__all__ = [
    "localfs",
    "s3",
    "huggingface",
]
