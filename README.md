<h1 align="center">nwp-consumer</h1>
<p align="center">
    <a href="https://github.com/openclimatefix/nwp-consumer/graphs/contributors" alt="Contributors">
        <img src="https://img.shields.io/github/contributors/openclimatefix/nwp-consumer" /></a>
</p>

Consumer for NWP data. Currently works with MetOffice and CEDA datasets.

# :warning: This is still a work in progress! :warning:

Still TODO:
- Dependency-inject storage client for S3 and local
- Complete the test suite
- Create the use-cases and business logic for the live service and for backfilling
- Create the entrypoint for the service
- Ensure the dockerfile and the workflow are working correctly

## Installation


### System dependencies

The `eccodes` python library depends on the ECMWF *ecCodes* library
that must be installed on the system and accessible as a shared library.

On a MacOS with HomeBrew use

```shell
$ brew install eccodes
```

Or if you manage binary packages with *Conda* use

```shell
$ conda install -c conda-forge eccodes
```

As an alternative you may install the official source distribution
by following the instructions at
https://confluence.ecmwf.int/display/ECC/ecCodes+installation

You may run a simple selfcheck command to ensure that your system is set up correctly:

```shell
$ python -m eccodes selfcheck
Found: ecCodes v2.27.0.
Your system is ready.
```

### Local development

Clone the repository and create and activate a new python virtualenv for it. `cd` to the repository root.

Install the required python dependencies with

```shell
$ pip install -e . 
```

### PyPi

TODO

## Repository structure

```yaml
nwp-consumer:
  src: # Top-level folder for the source code
    nwp_consumer: # The main library package
      internal: # Packages internal to the service. Like the 'lib' folder
        inputs: # Holds subpackages for each data source
          ceda:
            - _models.py # Contains the data models specific to the CEDA API
            - client.py # Functions for fetching CEDA data and mapping test_integration to the service model
          metoffice:
            - _models.py # Contains the data models for the MetOffice API
            - client.py # Functions for fetching MetOffice data and mapping test_integration to the service model
          - common.py # Common functions for the input sources
        service: # Holds the business logic and use-cases of the application
          - specificInitTime.py
      - main.py # The entrypoint for the application
    test_integration: # Contains the integration tests that test calls to external services
  - pyproject.toml # Describes the project and its dependencies
  - Containerfile # Contains the Dockerfile for the project
```

## Further reading

On packaging a python project using setuptools and pyproject.toml:
- https://godatadriven.com/blog/a-practical-guide-to-setuptools-and-pyproject-toml/

On hexagonal architecture: