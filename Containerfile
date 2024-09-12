# Build a virtualenv using miniconda
# * Conda creates a completely isolated environment,
#   including all required shared libraries, enabling
#   just putting the virtualenv into a distroless image
#   without having to faff around with linking all
#   the filelist (including for each dependency) of
#   https://packages.debian.org/trixie/libpython3.12-dev, e.g.
#
#       echo "Copying symlinked python binary into venv" && \
#       cp --remove-destination /usr/local/bin/python3.12 /venv/bin/python && \
#       echo "Copying libpython package into venv" && \
#       cp -r /usr/local/lib/* /venv/lib/ && \
#       cp -r /usr/local/include/python3.12/* /venv/include/ && \
#       mkdir -p /venv/lib/aarch64-linux-gnu/ && \
#       cp -r /usr/lib/aarch64-linux-gnu/* /venv/lib/aarch64-linux-gnu/ && \
#       mkdir -p /venv/include/aarch64-linux-gnu/ && \
#       cp -r /usr/include/aarch64-linux-gnu/* /venv/include/aarch64-linux-gnu/ && \

FROM quay.io/condaforge/miniforge3:latest AS build-venv

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=python3.12 \
    UV_PROJECT_ENVIRONMENT=/venv
COPY pyproject.toml /_lock/

# Synchronize DEPENDENCIES without the application itself.
# This layer is cached until uv.lock or pyproject.toml change.
# Delete any unwanted parts of the installed packages to reduce size
RUN --mount=type=cache,target=/root/.cache \
    apt-get update && apt-get install build-essential -y && \
    echo "Creating virtualenv at /venv" && \
    conda create -qy -p /venv python=3.12 numcodecs
RUN which gcc
RUN echo "Installing dependencies into /venv" && \
    cd /_lock && \
    mkdir src && \
    uv sync --no-dev --no-install-project && \
    echo "Optimizing /venv site-packages" && \
    rm -r /venv/lib/python3.12/site-packages/**/tests && \
    rm -r /venv/lib/python3.12/site-packages/**/_*cache*


# Then install the application itself
# * Delete the test and cache folders from installed packages to reduce size
COPY . /src
RUN --mount=type=cache,target=/root/.cache \
    uv pip install --no-deps --python=$UV_PROJECT_ENVIRONMENT /src

# Copy the virtualenv into a distroless image
# * These are small images that only contain the runtime dependencies
FROM gcr.io/distroless/python3-debian11
WORKDIR /app
COPY --from=build-venv /venv /venv

ENV RAWDIR=/work/raw \
    ZARRDIR=/work/data \
    ECCODES_DEFINITION_PATH=/venv/share/eccodes/definitions

ENTRYPOINT ["/venv/bin/nwp-consumer-cli"]
VOLUME /work
STOPSIGNAL SIGINT
