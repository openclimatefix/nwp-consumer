# NWP Consumer

**Download and convert weather data for use in ML pipelines**

[![tags badge](https://img.shields.io/github/v/tag/openclimatefix/nwp-consumer?include_prereleases&sort=semver&color=7BCDF3)](https://github.com/openclimatefix/nwp-consumer/tags)
[![pypi badge](https://img.shields.io/pypi/v/nwp-consumer?&color=086788)](https://pypi.org/project/nwp-consumer)
[![documentation badge](https://img.shields.io/badge/docs-latest-333333)](https://openclimatefix.github.io/nwp-consumer/)
[![contributors badge](https://img.shields.io/github/contributors/openclimatefix/nwp-consumer?color=FFFFFF)](https://github.com/openclimatefix/nwp-consumer/graphs/contributors)
[![workflows badge](https://img.shields.io/github/actions/workflow/status/openclimatefix/nwp-consumer/branch_ci.yml?branch=main&color=FFD053)](https://github.com/openclimatefix/nwp-consumer/actions/workflows/ci.yml)
[![ease of contribution: easy](https://img.shields.io/badge/ease%20of%20contribution:%20easy-32bd50)](https://github.com/openclimatefix/ocf-meta-repo?tab=readme-ov-file#overview-of-ocfs-nowcasting-repositories)

Some renewables, such as solar and wind, generate power according to the weather conditions.
Any forecasting therefore requires predictions of how these conditions will change.
Many meteorological organisations provide Numerical Weather Prediction (NWP) data,
which can then used for model training and inference. 

This data is often very large and can come in various formats.
Furthermore, these formats are not necessarily suitable for training,
so may require preprocessing and conversion. 

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

**To create an archive of GFS data:**

TODO

## Documentation

TODO: link to built documentation

Documentation is generated via [pydoctor](https://pydoctor.readthedocs.io/en/latest/).
To build the documentation, run the following command in the repository root:

```bash
$ python -m pydoctor
```

## FAQ

### How do I authenticate with model repositories that require accounts?



## Development
 
This project uses [MyPy](https://mypy.readthedocs.io/en/stable/) for static type checking
and [Ruff](https://docs.astral.sh/ruff/) for linting.
Installing the development dependencies makes them available in your virtual environment.

Use them via:

```bash
$ python -m mypy .
$ python -m ruff check .
```

Be sure to do this periodically while developing to catch any errors early
and prevent headaches with the CI pipeline.

### Running the test suite

Run the unittests with:

```bash
$ python -m unittest discover -s src/nwp_consumer -p "test_*.py"
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

[![issues badge](https://img.shields.io/github/issues/openclimatefix/ocf-template?color=FFAC5F)](https://github.com/openclimatefix/ocf-template/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc)

- PR's are welcome! See the [Organisation Profile](https://github.com/openclimatefix) for details on contributing
- Find out about our other projects in the [OCF Meta Repo](https://github.com/openclimatefix/ocf-meta-repo)
- Check out the [OCF blog](https://openclimatefix.org/blog) for updates
- Follow OCF on [LinkedIn](https://uk.linkedin.com/company/open-climate-fix)

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

---

*Part of the [Open Climate Fix](https://github.com/orgs/openclimatefix/people) community.*

[![OCF Logo](https://cdn.prod.website-files.com/62d92550f6774db58d441cca/6324a2038936ecda71599a8b_OCF_Logo_black_trans.png)](https://openclimatefix.org)
