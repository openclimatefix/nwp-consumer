"""Interfaces for actor-core communication.

The ports module defines abstract interfaces that specify the signatures
any actors (driving and driven) must obey in order to interact with the core.

*Driving* actors are found in the `services` module, and *driven* actors are found
in the `repositories` module.
"""

from .services import *
from .repositories import *
