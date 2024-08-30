"""Struct definitions for domain entities.

These define data objects and behaviours that are used in the services core.

Domain Entities
---------------

Entities are the core building blocks of the domain layer. They are the
representations of the business objects that are manipulated by the application.

By using domain entities in the core, it is ensured that the business logic is
separated from the technical details of the application.

A domain entity may have associated methods that define its behaviour, but it
should not contain any logic that is specific to a particular implementation.
"""

from .repometadata import *
from .tensorstore import *
from .postprocess import *
from .notification import *
from .parameters import *
from .coordinates import *
