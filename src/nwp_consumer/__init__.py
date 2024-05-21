"""NWP Consumer.

Overview
========

A microservice for multi-source consumption of NWP data, storing it in a common format. Built with inspiration
from the `Hexagonal Architecture`_ pattern, the nwp-consumer is
currently packaged with adapters for pulling and converting `.grib` data from:

- Nothing (yet!)

Its modular nature enables straightforward extension to alternate future sources.

Ubiquitous Language
-------------------

The following terms are used throughout the codebase and documentation. They are defined here to avoid ambiguity.

- ***InitTime*** - The time at which a forecast is initialised.
    For example, a forecast initialised at 12:00 on 1st January.

- ***TargetTime*** - The time at which a predicted value is valid.
    For example, a forecast with InitTime 12:00 on 1st January predicts that the temperature
    at TargetTime 12:00 on 2nd January at position x will be 10 degrees.

Structure
---------

`nwp-consumer` is structured following principles from the hexagonal architecture pattern. In brief, this means a clear
separation between the application's business logic - it's **Core** - and the **Actors** that are external to it.
The core of the service is split into three main components:

- **Domain** - The domain classes that define the structure of the data that the service works with.
- **Ports** - The interfaces that define how the service interacts with the outside world.
- **Service** - The service logic that defines how the service processes' data.

In this package, the core of the service is in `nwp_consumer.internal.core`,
and the actors are in `nwp_consumer.internal.handlers` and`nwp_consumer.internal.repositories`.

The service logic has no knowledge of the external actors, instead defining interfaces that
the actors must implement. These are found in `nwp_consumer.internal.core.ports`.
The actors are then responsible for implementing these interfaces, and are *dependency-injected* in at runtime.
This allows the service to be easily tested and extended. See `further reading` for more information.

Head into `nwp_consumer.internal.core` to see the details of each of these components.

Further reading
---------------

On packaging a python project using setuptools and pyproject.toml:

- The official PyPA packaging guide:
    https://packaging.python.org/.
- A step-by-step practical guide on the *godatadriven* blog:
    https://godatadriven.com/blog/a-practical-guide-to-setuptools-and-pyproject-toml/
- The pyproject.toml metadata specification:
    https://packaging.python.org/en/latest/specifications/declaring-project-metadata.

On hexagonal architecture:

- A concrete example using Python:
    https://medium.com/towards-data-engineering/a-concrete-example-of-the-hexagonal-architecture-in-python-d821213c6fb9
- An overview of the fundamentals incorporating Typescript:
    https://medium.com/ssense-tech/hexagonal-architecture-there-are-always-two-sides-to-every-story-bc0780ed7d9c

- Another example using Go:
    https://medium.com/@matiasvarela/hexagonal-architecture-in-go-cfd4e436faa3.

On the directory structure:

- The official PyPA discussion on src and flat layouts"
    https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/.

.. _Hexagonal Architecture:
    https://alistair.cockburn.us/hexagonal-architecture/
"""

import structlog

structlog.configure(processors=[
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.CallsiteParameterAdder(
        [
            structlog.processors.CallsiteParameter.FILENAME,
            structlog.processors.CallsiteParameter.LINENO,
        ],
    ),
    structlog.stdlib.add_log_level,
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.EventRenamer("message", replace_by="_event"),
    structlog.processors.dict_tracebacks,
    structlog.processors.JSONRenderer(sort_keys=True),
])
