"""NWP Consumer.

Usage Documentation
===================

Configuration
-------------

The following environment variables can be used to configure the application:

.. code-block:: none

    | Key                       | Description                         | Default                                     |
    |---------------------------|-------------------------------------|---------------------------------------------|
    | LOGLEVEL                  | The logging level for the app.      | INFO                                        |
    |---------------------------|-------------------------------------|---------------------------------------------|
    | RAWDIR                    | The working directory for the app.  | ~/.local/cache/nwp/<REPO>/<MODEL>/raw       |
    |                           | Can be a local path or an S3 URI.   |                                             |
    |---------------------------|-------------------------------------|---------------------------------------------|
    | ZARRDIR                   | The output directory for the app.   | ~/.local/cache/nwp/<REPO>/<MODEL>/data      |
    |                           | Can be a local path or an S3 URI.   |                                             |
    |---------------------------|-------------------------------------|---------------------------------------------|
    | NOTIFICATION_REPOSITORY   | The notification repository to use. | stdout                                      |
    |---------------------------|-------------------------------------|---------------------------------------------|
    | MODEL_REPOSITORY          | The model repository to use.        | ceda-metoffice-global                       |
    |---------------------------|-------------------------------------|---------------------------------------------|
    | CONCURRENCY               | Whether to use concurrency.         | True                                        |
    |---------------------------|-------------------------------------|---------------------------------------------|

There is also specific configuration variables for some model repositories.
Refer to their documentation for more information: `nwp_consumer.internal.repositories`.


Development Documentation
=========================

Getting started for development
-------------------------------

In order to work on the project, first clone the repository.
Then, create a virtual environment and install the dependencies
using an editable pip installation::

    $ git clone git@github.com:openclimatefix/nwp-consumer.git
    $ cd nwp-consumer
    $ python -m venv ./venv
    $ source ./venv/bin/activate
    $ pip install -e .[dev]

.. note:: ZSH users may have to escape the square brackets in the last command.

This enables the use of the 'nwp-consumer-cli' command in the virtualenv, which
runs the `nwp_consumer.cmd.main.run_cli` entrypoint. The editable installation
ensures that changes to the code are immediately reflected while using the command.


Project structure
-----------------

The code is structured following principles from the `Hexagonal Architecture`_ pattern.
In brief, this means a clear separation between
the application's business logic - it's *core* - and the *actors* that are external to it.

The core of the services is split into three main components:

- `nwp_consumer.internal.entities` - The domain classes that define the structure of the data
  that the services works with, and the business logic they contain.
- `nwp_consumer.internal.ports` - The interfaces that define how the services interact with external actors.
- `nwp_consumer.internal.services` - The business logic that defines how the service functions.

Alongside these core components are the actors, which adhere to the interfaces defined in the
ports module. Actors come in two flavours, *driving* and *driven*.
Driven actors are sources and sinks of data, such as databases and message queues,
while driving actors are methods of interacting with the core, such as a command-line interface
or REST server.

This application currently has the following defined actors:

- `nwp_consumer.internal.repositories.raw_repositories` (driven) - The sources of NWP data.
- `nwp_consumer.internal.repositories.notification_repositories` (driven) - The sinks of notification data.
- `nwp_consumer.internal.handlers.cli` (driving) - The command-line interface for the services.

The actors are then responsible for implementing the abstract ports,
and are *dependency-injected* in at runtime. This allows the services to be easily tested
and extended. See 'further reading' for more information.

Head into `nwp_consumer.internal` to see the details of each of these components.

Where do I go to...?
--------------------

- **...modify the business logic?** Check out the `internal.services` module.
- **...add a new source of NWP data?** Implement a new repository in `internal.repositories.raw_repositories`.
- **...modify the command line interface?** Check out `internal.handlers.cli`.

Further reading
===============

On packaging a python project using setuptools and pyproject.toml:

- The official PyPA packaging guide: https://packaging.python.org/
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

.. _Hexagonal Architecture: https://alistair.cockburn.us/hexagonal-architecture/
"""

import logging
import sys
import os

if sys.stdout.isatty():
    # Simple logging for terminals
    _formatstr="%(levelname)s [%(name)s] | %(message)s"
else:
    # JSON logging for containers
    _formatstr="".join((
        "{",
        '"message": "%(message)s", ',
        '"severity": "%(levelname)s", "timestamp": "%(asctime)s.%(msecs)03dZ", ',
        '"logging.googleapis.com/labels": {"python_logger": "%(name)s"}, ',
        '"logging.googleapis.com/sourceLocation": ',
        '{"file": "%(filename)s", "line": %(lineno)d, "function": "%(funcName)s"}',
        "}",
    ))

_loglevel: int | str = logging.getLevelName(os.getenv("LOGLEVEL", "INFO").upper())
logging.basicConfig(
    level=logging.INFO if isinstance(_loglevel, str) else _loglevel,
    stream=sys.stdout,
    format=_formatstr,
    datefmt="%Y-%m-%dT%H:%M:%S",
)

for logger in [
    "numcodecs",
    "numexpr",
    "gribapi",
    "aiobotocore",
    "s3fs",
    "fsspec",
    "asyncio",
    "botocore",
    "cfgrib",
]:
    logging.getLogger(logger).setLevel(logging.WARNING)
