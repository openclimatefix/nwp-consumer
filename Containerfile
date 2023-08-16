# Build a virtualenv using miniconda
# * Install required compilation tools for wheels via apt
# * Install required non-python binaries via conda
FROM quay.io/condaforge/miniforge3:latest AS build-venv
RUN apt update && apt install -y build-essential
RUN conda create -p /venv python=3.10
RUN /venv/bin/pip install --upgrade pip wheel setuptools
RUN conda install -p /venv -y eccodes zarr

# Install packages into the virtualenv as a separate step
# * Only re-execute this step when the copied files change
# * This also builds the package into the virtualenv
# * The package is versioned via setuptools_git_versioning
#   hence the .git directory is required
# * The README.md is required for the long description
FROM build-venv AS build-wheels
WORKDIR /app
COPY src src
COPY pyproject.toml pyproject.toml
COPY .git .git
COPY README.md README.md
RUN /venv/bin/pip install .

# Copy the virtualenv into a distroless image
# * These are small images that only contain the runtime dependencies
FROM gcr.io/distroless/python3-debian11
WORKDIR /app
COPY --from=build-wheels /venv /venv
HEALTHCHECK CMD ["/venv/bin/nwp-consumer", "check"]
ENTRYPOINT ["/venv/bin/nwp-consumer"]
VOLUME /tmp/nwpc
