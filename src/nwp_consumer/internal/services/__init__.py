"""Interfaces for core services implementations.

The services module defines abstract interfaces that specify the signatures
any services implementations must obey in order to interact with the core.
"""

from .consumer_service import ConsumerService

__all__ = [
    "ConsumerService",
]
