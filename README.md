# nwp-consumer

A microservice for multi-source consumption of NWP data, storing it in a common format.

[![contributors badge](https://img.shields.io/github/contributors/openclimatefix/nwp-consumer?color=FFFFFF)](https://github.com/openclimatefix/nwp-consumer/graphs/contributors)
[![workflows badge](https://img.shields.io/github/actions/workflow/status/openclimatefix/nwp-consumer/ci.yml?branch=maine&color=FFD053)](https://github.com/openclimatefix/nwp-consumer/actions/workflows/ci.yml)
[![issues badge](https://img.shields.io/github/issues/openclimatefix/nwp-consumer?color=FFAC5F)](https://github.com/openclimatefix/nwp-consumer/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc)
[![tags badge](https://img.shields.io/github/v/tag/openclimatefix/nwp-consumer?include_prereleases&sort=semver&color=7BCDF3)](https://github.com/openclimatefix/nwp-consumer/tags)
[![pypi badge](https://img.shields.io/pypi/v/nwp-consumer?&color=086788)](https://pypi.org/project/nwp-consumer)
[![documentation badge](https://img.shields.io/badge/docs-latest-333333)](https://openclimatefix.github.io/nwp-consumer/)

## Overview

Some renewables, such as solar and wind, generate power according to the weather conditions.
As such, in order to forecast this generation, predictions of the upcoming weather conditions are required.
Many meteorological organisations provide Numerical Weather Prediction (NWP) data,
which can then used for model training and inference. 

This data is often very large and can come in various formats.
These formats are not necessarily suitable for training, so may require preprocessing and conversion. 

This package aims to streamline the collection and processing of this NWP data.

> [!Note]
> This is *not* built to replace tools such as [Herbie](https://github.com/blaylockbk/Herbie). 
> It is built to produce data specific to the needs of Open Climate Fix's models,
> so things like the output format and the variable selection are hard-coded.
> If you need a more configurable cli-driven tool, consider using herbie instead.

## Installation

Install from PyPi using pip:

```bash
$ pip install nwp-consumer
```

Or use the container image:

```bash
$ docker pull ghcr.io/openclimatefix/nwp-consumer
```

## Example usage

TODO

## Documentation

TODO: link to built documentation

Documentation is generated via [pydoctor](https://pydoctor.readthedocs.io/en/latest/).
To build the documentation, run the following command in the repository root:

```bash
$ python -m pydoctor
```

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

![OCF Logo](https://cdn.prod.website-files.com/62d92550f6774db58d441cca/6324a2038936ecda71599a8b_OCF_Logo_black_trans.png)
