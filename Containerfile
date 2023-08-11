# Build a virtualenv using miniconda
# * Install required non-python binaries
# * Update pip setuputils and wheel to support building new packages
FROM quay.io/condaforge/miniforge3:latest AS build
RUN conda create -p /venv python=3.10
RUN conda install -p /venv -y cfgrib cartopy cf-units cftime numcodecs psutil
RUN /venv/bin/pip install --upgrade pip

# Install packages into the virtualenv as a separate step
# * Only re-execute this step when the copied files change
# * This also builds the package into the virtualenv
# * The package is versioned via setuptools_git_versioning
#   hence the .git directory is required
FROM build AS build-venv
WORKDIR /app
COPY src src
COPY pyproject.toml pyproject.toml
COPY .git .git
RUN /venv/bin/pip install .

# Copy the virtualenv into a distroless image
# * These are small images that only contain the runtime dependencies
FROM gcr.io/distroless/python3-debian11
WORKDIR /app
COPY --from=build-venv /venv /venv
HEALTHCHECK CMD ["/venv/bin/nwp-consumer", "check"]
ENTRYPOINT ["/venv/bin/nwp-consumer"]
VOLUME /tmp/nwpc
