# NWP Consumer
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-5-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->

**Download and convert weather data for use in ML pipelines**

[![tags badge](https://img.shields.io/github/v/tag/openclimatefix/nwp-consumer?include_prereleases&sort=semver&color=7BCDF3)](https://github.com/openclimatefix/nwp-consumer/tags)
[![pypi badge](https://img.shields.io/pypi/v/nwp-consumer?&color=086788)](https://pypi.org/project/nwp-consumer)
[![documentation badge](https://img.shields.io/badge/docs-latest-333333)](https://openclimatefix.github.io/nwp-consumer/)
[![contributors badge](https://img.shields.io/github/contributors/openclimatefix/nwp-consumer?color=FFFFFF)](https://github.com/openclimatefix/nwp-consumer/graphs/contributors)
[![workflows badge](https://img.shields.io/github/actions/workflow/status/openclimatefix/nwp-consumer/branch_ci.yml?branch=main&color=FFD053)](https://github.com/openclimatefix/nwp-consumer/actions/workflows/branch_ci.yml)
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

**To download the latest available day of GFS data:***

```bash
$ nwp-consumer consume
```

**To create an archive of a month of GFS data:**

> [!Note]
> This will download several gigabytes of data to your home partition.
> Make sure you have plenty of free space (and time!)

```bash
$ nwp-consumer archive --year 2024 --month 1
```

## Documentation

Documentation is generated via [pdoc](https://pdoc.dev/docs/pdoc.html).
To build the documentation, run the following command in the repository root:

```bash
$ PDOC_ALLOW_EXEC=1 python -m pdoc -o docs --docformat=google src/nwp_consumer
```

> [!Note]
> The `PDOC_ALLOW_EXEC=1` environment variable is required due to a facet
> of the `ocf_blosc2` library, which imports itself automatically and hence
> necessitates execution to be enabled.

## FAQ

### How do I authenticate with model repositories that require accounts?

Authentication, and model repository selection, is handled via environment variables. 
Choose a repository via the `MODEL_REPOSITORY` environment variable. Required environment
variables can be found in the repository's metadata function. Missing variables will be
warned about at runtime.

### How do I use an S3 bucket for created stores?

The `ZARRDIR` environment variable can be set to an S3 url
(ex: `s3://some-bucket-name/some-prefix`). Valid credentials for accessing the bucket
must be discoverable in the environment as per
[Botocore's documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)

### How do I change what variables are pulled?

With difficulty! This package pulls data specifically tailored to Open Climate Fix's needs,
and as such, the data it pulls (and the schema that data is surfaced with)
is a fixed part of the package. A large part of the value proposition of this consumer is
that the data it produces is consistent and comparable between different sources, so pull
requests to the effect of adding or changing this for a specific model are unlikely to be
approved.

However, desired changes can be made via cloning the repo and making the relevant
parameter modifications to the model's expected coordinates in it's metadata for the desired model
repository. 

## Development

### Linting and static type checking
 
This project uses [MyPy](https://mypy.readthedocs.io/en/stable/) for static type checking
and [Ruff](https://docs.astral.sh/ruff/) for linting.
Installing the development dependencies makes them available in your virtual environment.

Use them via:

```bash
$ python -m mypy .
$ python -m ruff check .
```

Be sure to do this periodically while developing to catch any errors early
and prevent headaches with the CI pipeline. It may seem like a hassle at first,
but it prevents accidental creation of a whole suite of bugs.

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

[![issues badge](https://img.shields.io/github/issues/openclimatefix/nwp-consumer?color=FFAC5F)](https://github.com/openclimatefix/nwp-consumer/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc)

- PR's are welcome! See the [Organisation Profile](https://github.com/openclimatefix) for details on contributing
- Find out about our other projects in the [OCF Meta Repo](https://github.com/openclimatefix/ocf-meta-repo)
- Check out the [OCF blog](https://openclimatefix.org/blog) for updates
- Follow OCF on [LinkedIn](https://uk.linkedin.com/company/open-climate-fix)

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/devsjc"><img src="https://avatars.githubusercontent.com/u/47188100?v=4?s=100" width="100px;" alt="devsjc"/><br /><sub><b>devsjc</b></sub></a><br /><a href="#projectManagement-devsjc" title="Project Management">📆</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/peterdudfield"><img src="https://avatars.githubusercontent.com/u/34686298?v=4?s=100" width="100px;" alt="Peter Dudfield"/><br /><sub><b>Peter Dudfield</b></sub></a><br /><a href="https://github.com/openclimatefix/nwp-consumer/commits?author=peterdudfield" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.jacobbieker.com"><img src="https://avatars.githubusercontent.com/u/7170359?v=4?s=100" width="100px;" alt="Jacob Prince-Bieker"/><br /><sub><b>Jacob Prince-Bieker</b></sub></a><br /><a href="https://github.com/openclimatefix/nwp-consumer/commits?author=jacobbieker" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ADIMANV"><img src="https://avatars.githubusercontent.com/u/68527614?v=4?s=100" width="100px;" alt="Aditya Sawant"/><br /><sub><b>Aditya Sawant</b></sub></a><br /><a href="https://github.com/openclimatefix/nwp-consumer/commits?author=ADIMANV" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://mvanderbroek.com"><img src="https://avatars.githubusercontent.com/u/6012624?v=4?s=100" width="100px;" alt="Mark van der Broek"/><br /><sub><b>Mark van der Broek</b></sub></a><br /><a href="https://github.com/openclimatefix/nwp-consumer/commits?author=markkvdb" title="Documentation">📖</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

---

*Part of the [Open Climate Fix](https://github.com/orgs/openclimatefix/people) community.*

[![OCF Logo](https://cdn.prod.website-files.com/62d92550f6774db58d441cca/6324a2038936ecda71599a8b_OCF_Logo_black_trans.png)](https://openclimatefix.org)

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->
<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!