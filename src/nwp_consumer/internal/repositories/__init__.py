"""Implementation of adaptors for driven actors.

Driven actors
--------------

A driven actor is an external component that is acted upon by the core logic.
Also referred to as *secondary* actors, a driven actor represents an external
system that the core logic interacts with. They extend the core driven ports
(see `nwp_consumer.internal.ports.repositories`) in their implementation.

Examples of driven or secondary actors include:

- a database
- a message queue
- a filesystem

This module
-----------

This module contains implementations for the following driven actors:

- Notification - `notification_repositories`
- Model - `model_repositories`
"""

from .model_repositories import *
from .notification_repositories import *