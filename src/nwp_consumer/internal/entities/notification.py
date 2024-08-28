"""Domain entities for service notifications.

Upon completion of the processing of a request, the service can
deliver a notification of the result to a notification repository.
This module defines the structure of the notification messages.
"""

import dataclasses


@dataclasses.dataclass
class PerformanceMetadata:
    """Metadata for a service operation."""

    duration_seconds: float
    """The duration of the operation in seconds."""

    memory_mb: float
    """The memory usage of the operation in megabytes."""


@dataclasses.dataclass(slots=True)
class StoreCreatedNotification:
    """A notification of successful store creation."""

    filename: str
    """The name of the store created, including extension."""

    size_mb: int
    """The size of the store in megabytes."""

    performance: PerformanceMetadata
    """Metadata for the operation."""


@dataclasses.dataclass(slots=True)
class StoreAppendedNotification:
    """A notification of successful append to a store."""

    filename: str
    """The name of the store appended to, including extension."""

    size_mb: int
    """The size of the store in megabytes."""

    performance: PerformanceMetadata
    """Metadata for the operation."""
