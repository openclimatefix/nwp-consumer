"""NWP Consumer.

Overview
--------

A microservice for multi-source consumption of NWP data, storing it in a common format. Built with inspiration
from the `Hexagonal Architecture`_ pattern, the nwp-consumer is
currently packaged with adapters for pulling and converting grib data from:

- Nothing (yet!)

Its modular nature enables straightforward extension to alternate future sources.

Ubiquitous language
-------------------

The following terms are used throughout the codebase and documentation. They are defined here to avoid ambiguity.

- ***InitTime*** - The time at which a forecast is initialised.
    For example, a forecast initialised at 12:00 on 1st January.

- ***TargetTime*** - The time at which a predicted value is valid.
    For example, a forecast with InitTime 12:00 on 1st January predicts that the temperature
    at TargetTime 12:00 on 2nd January at position x will be 10 degrees.

The assumption is made that every piece of NWP forecast data has both an associated init time and target time.

Structure
---------

`nwp-consumer` is structured following principles from the hexagonal architecture pattern. In brief, this means a clear
separation between the application's business logic - it's **Core** - and the **Actors** that are external to it.
The core of the service is split into three main components:

- **Domain** - The entities classes that define the structure of the data that the service works with.
- **Ports** - The interfaces that define how the service interacts with the outside world.
- **Service** - The service logic that defines how the service processes' data.

In this package, the actors are in `nwp_consumer.internal.repositories`, the entities in
`nwp_consumer.internal.entities`, and the service logic in `nwp_consumer.internal.service`.

The business logic has no knowledge of the external actors, instead defining interfaces that
the actors must implement. These are found in `nwp_consumer.internal.ports`.
The actors are then responsible for implementing these interfaces, and are *dependency-injected* in at runtime.
This allows the service to be easily tested and extended. See 'further reading' for more information.

Head into `nwp_consumer.internal` to see the details of each of these components.

Further reading
---------------

On packaging a python project using setuptools and pyproject.toml:

- The official PyPA packaging guide:
    https://packaging.python.org/
- A step-by-step practical guide on the *godatadriven* blog:
    https://godatadriven.com/blog/a-practical-guide-to-setuptools-and-pyproject-toml/
- The pyproject.toml metadata specification:
    https://packaging.python.org/en/latest/specifications/declaring-project-metadata

On hexagonal architecture:

- A concrete example using Python:
    https://medium.com/towards-data-engineering/a-concrete-example-of-the-hexagonal-architecture-in-python-d821213c6fb9
- An overview of the fundamentals incorporating Typescript:
    https://medium.com/ssense-tech/hexagonal-architecture-there-are-always-two-sides-to-every-story-bc0780ed7d9c

- Another example using Go:
    https://medium.com/@matiasvarela/hexagonal-architecture-in-go-cfd4e436faa3

On the directory structure:

- The official PyPA discussion on src and flat layouts"
    https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/

.. _Hexagonal Architecture:
    https://alistair.cockburn.us/hexagonal-architecture/
"""

import logging
import sys

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="{"
    + '"message": "%(message)s", '
    + '"severity": "%(levelname)s", "timestamp": "%(asctime)s.%(msecs)03dZ", '
    + '"logging.googleapis.com/labels": {"python_logger": "%(name)s"}, '
    + '"logging.googleapis.com/sourceLocation": '
    + ' {"file": "%(filename)s", "line": %(lineno)d, "function": "%(funcName)s"}'
    + "}",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

for logger in ["numcodecs", "numexpr"]:
    logging.getLogger(logger).setLevel(logging.WARNING)
