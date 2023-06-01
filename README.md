<h2 align="center">
🌐 nwp-consumer 🌐
<br>
<br>
Microservice for consuming NWP data.
</h2>

<div align="center">

<a href="https://github.com/openclimatefix/nwp-consumer/graphs/contributors" alt="Contributors">
    <img src="https://img.shields.io/github/contributors/openclimatefix/nwp-consumer?style=for-the-badge" /></a>
<a href="https://github.com/openclimatefix/nwp-consumer/actions/workflows/ci.yml">
    <img alt="GitHub Workflow Status (with branch)" src="https://img.shields.io/github/actions/workflow/status/openclimatefix/nwp-consumer/ci.yml?branch=main&style=for-the-badge"></a>
<a href="https://github.com/openclimatefix/nwp-consumer/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc">
    <img src="https://img.shields.io/github/issues/openclimatefix/nwp-consumer?style=for-the-badge"></a>
<a href="https://github.com/openclimatefix/nwp-consumer/tags">
    <img alt="GitHub tag (latest SemVer pre-release)" src="https://img.shields.io/github/v/tag/openclimatefix/nwp-consumer?include_prereleases&sort=semver&style=for-the-badge"></a>

</div>

<br>

A microservice for consuming NWP data from various sources and storing it in a common format. Built with inspiration 
from the [Hexagonal Architecture](https://alistair.cockburn.us/hexagonal-architecture) pattern, the nwp-consumer is 
currently packaged with adapters for pulling and converting `.grib` data from the 
[MetOffice](https://gridded-data-ui.cda.api.metoffice.gov.uk) and from [CEDA](https://catalogue.ceda.ac.uk). Its modular
nature enables straightforward extension to alternate future sources.

### :warning: This is still a work in progress!

Still TODO:
- Complete the module for storage in S3

## Running the service

### Using Docker (recommended)

This service is designed to be run as a Docker container. The `Containerfile` is the Dockerfile for the service.
It is recommended to run it this way due to the present dependency on various external binaries, which at the moment
cannot be easily distributed in a PyPi package. To run, pull the latest version from `ghcr.io` via:

```shell
$ docker run ghcr.io/openclimatefix/nwp-consumer:latest \
  -v /path/to/datadir:/data \
  -e ZARR_DIR=/data/zarr \
  -e RAW_DIR=/data/raw \
  -e <other required env vars...>  
```

### Using the Python Package (not recommended)

Ensure the [external dependencies](#external-dependencies) are installed. Then, either:

1. Download the latest wheel from the artifacts of the desired
    [CI run](https://github.com/openclimatefix/nwp-consumer/actions/workflows/ci.yml) and install it via
    ```shell
    $ pip install nwp-consumer-<version>.whl
    ```

2. Clone the repository and install the package via
    ```shell
    $ pip install .
    ```

Then run the service via

```shell
$ ZARR_DIR="~/zarr" RAW_DIR="~/raw" <other required env vars...> nwp-consumer 
```

## Repository structure

Produced using [exa](https://github.com/ogham/exa):
```shell
$ exa --tree --git-ignore -F -I "*init*|test*.*"
```

```yml
./
├── Containerfile # The Dockerfile for the service
├── pyproject.toml # The build configuration for the service
├── README.md
└── src/
   ├── nwp_consumer/ # The main library package
   │  ├── cmd/
   │  │  └── main.py # The entrypoint to the service
   │  └── internal/ # Packages internal to the service. Like the 'lib' folder
   │     ├── config/ 
   │     │  └── config.py # Contains the configuration specification for running the service
   │     ├── inputs/ # Holds subpackages for each incoming data source
   │     │  ├── ceda/
   │     │  │  ├── _models.py
   │     │  │  ├── client.py # Contains the client and functions to map CEDA data to the service model
   │     │  │  └── README.md # Info about the CEDA data source
   │     │  └── metoffice/
   │     │     ├── _models.py
   │     │     ├── client.py # # Contains the client and functions to map MetOffice data to the service model
   │     │     └── README.md # Info about the MetOffice data source
   │     ├── models.py # Describes the internal data models for the service
   │     ├── outputs/ # Holds subpackages for each data sink
   │     │  ├── localfs/
   │     │  │  └── client.py # Contains the client for storing data on the local filesystem
   │     │  └── s3/
   │     │     └── client.py # Contains the client for storing data on S3
   │     └── service/ # Contains the business logic and use-cases of the application
   │        └── service.py # Defines the service class for the application, whose methods are the use-cases
   └── test_integration/
```

`nwp-consumer` is structured following principles from the hexagonal architecture pattern. In brief, this means a clear 
separation between the application's business logic - it's **Core** - and the **Actors** that are external to it. In 
this package, the core of the service is in `internal/service/` and the actors are in `internal/inputs/` and 
`internal/outputs/`. The service logic has no knowledge of the external actors, instead defining interfaces that the 
actors must implement. These are found in `internal/models.py`. The actors are then responsible for implementing these 
interfaces, and are *dependency-injected* in at runtime. This allows the service to be easily tested and extended. See
[further reading](#further-reading) for more information.

## Local development

Clone the repository and create and activate a new python virtualenv for it. `cd` to the repository root.

Install the [External](#external-dependencies) and [Python](#python-requirements) dependencies.

### External dependencies

The `cfgrib` python library depends on the ECMWF *cfgrib* binary, which is a wrapper around the ECMWF *ecCodes* library.
One of these must be installed on the system and accessible as a shared library.

On a MacOS with HomeBrew use

```shell
$ brew install eccodes
```

Or if you manage binary packages with *Conda* use

```shell
$ conda install -c conda-forge cfgrib
```

As an alternative you may install the official source distribution
by following the instructions at
https://confluence.ecmwf.int/display/ECC/ecCodes+installation

You may run a simple selfcheck command to ensure that your system is set up correctly:

```shell
$ python -m <eccodes OR cfgrib> selfcheck
Found: ecCodes v2.27.0.
Your system is ready.
```

### Python requirements

Install the required python dependencies with

```shell
$ pip install -e . 
```

This looks for requirements specified in the `pyproject.toml` file.

<details>
    <summary>Where is the requirements.txt file?</summary>

There is no `requirements.txt` file. Instead, the project uses setuptool's pyproject.toml integration to specify 
dependencies. This is a new feature of setuptools and pip, and is the 
[recommended way](https://packaging.python.org/en/latest/tutorials/packaging-projects/) to specify dependencies.
See [the setuptools guide](https://setuptools.pypa.io/en/latest/userguide/pyproject_config.html) and
[the PEP621 specification](https://packaging.python.org/en/latest/specifications/declaring-project-metadata)
for more information, as well as [Further Reading](#further-reading).
</details>

### Running tests

Due to the `src` directory structure, the module must be installed into your site-packages before running tests.

```shell
$ pip install .
```

Run the unit tests with

```shell
$ python -m unittest discover -s src/nwp_consumer -p "test_*.py"
```

and the integration tests with

```shell
$ python -m unittest discover -s test_integration -p "test_*.py"
```

See [further reading](#further-reading) for more information on the `src` directory structure.

---

## Further reading

On packaging a python project using setuptools and pyproject.toml:
- The official [PyPA packaging guide](https://packaging.python.org/en/latest/tutorials/packaging-projects/).
- A [step-by-step practical guide](https://godatadriven.com/blog/a-practical-guide-to-setuptools-and-pyproject-toml/)
on the *godatadriven* blog.
- The pyproject.toml
[metadata specification](https://packaging.python.org/en/latest/specifications/declaring-project-metadata).

On hexagonal architecture:
- A [concrete example](https://medium.com/towards-data-engineering/a-concrete-example-of-the-hexagonal-architecture-in-python-d821213c6fb9)
using Python.
- An [overview of the fundamentals](https://medium.com/ssense-tech/hexagonal-architecture-there-are-always-two-sides-to-every-story-bc0780ed7d9c) 
incorporating Typescript 
- Another [example](https://medium.com/@matiasvarela/hexagonal-architecture-in-go-cfd4e436faa3) using Go.

On the directory structure:
- The official [PyPA discussion](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/) on 
src and flat layouts.


---

## Contributing and community

- See the [OCF Organisation Repo](https://github.com/openclimatefix) for details on contributing.
- Find out more about OCF in the [Meta Repo](https://github.com/openclimatefix/ocf-meta-repo).
- Follow OCF on [Twitter](https://twitter.com/OpenClimateFix).
- Check out the OCF blog at https://openclimatefix.org/blog for updates.
