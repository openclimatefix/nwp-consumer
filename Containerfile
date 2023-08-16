# Build a virtualenv using miniconda
# * Install required compilation tools for wheels via apt
# * Install required non-python binaries via conda
FROM quay.io/condaforge/miniforge3:latest AS build-venv
RUN apt update && apt install -y build-essential
RUN conda create -p /venv python=3.10
RUN /venv/bin/pip install --upgrade pip wheel setuptools
RUN conda install -p /venv -y eccodes zarr

# Install packages into the virtualenv as a separate step
# * Only re-execute this step when the requirements files change
FROM build-venv AS build-reqs
WORKDIR /app
COPY pyproject.toml pyproject.toml
RUN /venv/bin/pip install . --no-cache-dir --no-binary=nwp-consumer

# Build binary for the package
# * The package is versioned via setuptools_git_versioning
#   hence the .git directory is required
# * The README.md is required for the long description
FROM build-reqs AS build-app
COPY src src
COPY .git .git
COPY README.md README.md
RUN /venv/bin/pip install .

# Copy the virtualenv into a distroless image
# * These are small images that only contain the runtime dependencies
FROM gcr.io/distroless/python3-debian11
WORKDIR /app
COPY --from=build-app /venv /venv
HEALTHCHECK CMD ["/venv/bin/nwp-consumer", "check"]
ENTRYPOINT ["/venv/bin/nwp-consumer"]
VOLUME /tmp/nwpc
