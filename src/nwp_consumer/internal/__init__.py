"""Internal workings of the services.

Why have an internal package?
-----------------------------

This package is meant to be run as a services, either via a binary or a container image.
However, the code can still be used as a library, and a user could import the modules
from this package and use them in their own code.

The "internal" package signifies that the modules within are not meant to be used,
or are not guaranteed to be stable, for external users. This helps to discourage casual
dependence in other services. Any functionality looking to be re-used should either
become a shared library or simply be copied from the source code.
"""

from . import entities, handlers, ports, repositories, services

__all__ = ["entities", "ports", "handlers", "repositories", "services"]
