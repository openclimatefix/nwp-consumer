# Build a virtualenv using miniconda
# * Install required non-python binaries
# * Update pip setuputils and wheel to support building new packages
FROM continuumio/miniconda3:latest AS build
RUN conda create -p /venv python=3.9
RUN conda install -p /venv -c conda-forge -y eccodes
RUN conda install -p /venv -c conda-forge -y cartopy cf-units cftime numcodecs
RUN /venv/bin/pip install --upgrade pip setuptools wheel

# Install packages into the virtualenv as a separate step
# * Only re-execute this step when the source changes
FROM build AS build-venv
COPY pyproject.toml /pyproject.toml
COPY src /src
RUN /venv/bin/pip install .

# Copy the virtualenv into a distroless image
# * These are small images that only contain the runtime dependencies
FROM gcr.io/distroless/python3-debian11
COPY --from=build-venv /venv /venv
COPY src/nwp_consumer /app
WORKDIR /app
ENTRYPOINT ["/venv/bin/python3", "main.py"]
