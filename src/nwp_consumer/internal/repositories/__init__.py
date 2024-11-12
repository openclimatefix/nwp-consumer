"""Implementation of adaptors for driven actors.

Driven actors
--------------

A driven actor is an external component that is acted upon by the core logic.
Also referred to as *secondary* actors, a driven actor represents an external
system that the core logic interacts with. They extend the core driven ports
(see `nwp_consumer.internal.ports`) in their implementation.

Examples of driven or secondary actors include:

- a database
- a message queue
- a filesystem

This module
-----------

This module contains implementations for the following driven actors:

- Notification Repository - Somewhere to send notifications to
- Model Repository - Source of NWP data

Both inherit from the repository ports specified in the core via `nwp_consumer.internal.ports`.
"""

from .model_repositories import (
    CEDAFTPModelRepository,
    ECMWFRealTimeS3ModelRepository,
    NOAAS3ModelRepository,
)
from .notification_repositories import (
    StdoutNotificationRepository,
    DagsterPipesNotificationRepository,
)

__all__ = [
    "CEDAFTPModelRepository",
    "ECMWFRealTimeS3ModelRepository",
    "NOAAS3ModelRepository",
    "StdoutNotificationRepository",
    "DagsterPipesNotificationRepository",
]
