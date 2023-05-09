# Build a virtualenv using miniconda
# * Install required non-python binaries
# * Update pip setuputils and wheel to support building new packages
FROM continuumio/miniconda3:latest AS build
RUN conda create -p /venv python=3.9
RUN conda install -p /venv -c conda-forge -y eccodes cartopy cf-units cftime numcodecs
RUN /venv/bin/pip install --upgrade pip

# Install packages into the virtualenv as a separate step
# * Only re-execute this step when the source changes
# * This also builds the package into the virtualenv
# * The package is versioned via setuptools_git_versioning
FROM build AS build-venv
COPY . .
RUN /venv/bin/pip install .

# Copy the virtualenv into a distroless image
# * These are small images that only contain the runtime dependencies
FROM gcr.io/distroless/python3-debian11
COPY --from=build-venv /venv /venv
COPY src/nwp_consumer /app/nwp_consumer
WORKDIR /app
ENTRYPOINT ["/venv/bin/nwp-consumer"]
