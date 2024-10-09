"""Implementation of adaptors for driving actors.

Driving actors
--------------

A driving actor is an external component that initiates interaction
with the core logic. Also referred to as *primary* actors, a driving
actor represents an entrypoint that uses the core driving ports
(see `nwp_consumer.internal.ports.services`) in its implementation.
In this manner, it *handles* whatever input it receives and *drives*
the core logic to perform the necessary operations, hence the module
name.

Examples of driving or primary actors include:

- a REST services receiving requests
- a CLI tool processing user input

This module
-----------

This module contains implementations for the following driving actors:

- Command-line interface (CLI) - `nwp_consumer.internal.handlers.cli`
"""

from .cli import CLIHandler

__all__ = [
    "CLIHandler"
]
