# Build a virtualenv using miniconda
# * Install required compilation tools for wheels via apt
# * Install required non-python binaries via conda
FROM quay.io/condaforge/miniforge3:latest AS build-venv
RUN apt -qq update && apt -qq install -y build-essential
RUN conda create -p /venv python=3.12
RUN /venv/bin/pip install --upgrade -q pip wheel setuptools

# Install packages into the virtualenv as a separate step
# * Only re-execute this step when the requirements files change
FROM build-venv AS build-reqs
WORKDIR /app
COPY pyproject.toml pyproject.toml
RUN conda install -p /venv -q -y eccodes zarr
RUN /venv/bin/pip install -q . --no-cache-dir --no-binary=nwp-consumer

# Build binary for the package
# * The package is versioned via setuptools_git_versioning
#   hence the .git directory is required
# * The README.md is required for the long description
FROM build-reqs AS build-app
COPY src src
RUN rm -rf src/**/test_*
COPY .git .git
COPY README.md README.md
RUN /venv/bin/pip install .

# Copy the virtualenv into a distroless image
# * These are small images that only contain the runtime dependencies
FROM gcr.io/distroless/python3-debian11
WORKDIR /app
HEALTHCHECK CMD ["/venv/bin/nwp-consumer", "check"]
ENTRYPOINT ["/venv/bin/nwp-consumer"]
VOLUME /tmp/nwpc
