# Build a virtualenv using uv
FROM python:3.12-slim-bookworm AS build-venv

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=python3.12 \
    UV_PROJECT_ENVIRONMENT=/venv
COPY pyproject.toml /_lock/

# Synchronize DEPENDENCIES without the application itself.
# This layer is cached until uv.lock or pyproject.toml change.
# Also copy the symlinked python and shared libraries into the virtualenv
RUN --mount=type=cache,target=/root/.cache \
    cd /_lock && \
    mkdir src && \
    uv sync --no-dev --no-install-project

# Then install the application itself
# * Copy over the required shared libraries into the venv
# * Delete the test and cache folders from installed packages
COPY . /src
RUN --mount=type=cache,target=/root/.cache \
    uv pip install --no-deps --python=$UV_PROJECT_ENVIRONMENT /src && \
    cp -r /usr/local/lib/python3.12 /venv/lib/ && \
    cp --remove-destination /usr/local/bin/python3.12 /venv/bin/python && \
    cp /usr/local/lib/libpython3.12.so.1.0 /venv/lib/ && \
    rm -r /venv/lib/python3.12/site-packages/**/tests && \
    rm -r /venv/lib/python3.12/site-packages/**/_*cache* && \
    rm /venv/lib/python3.12/site-packages/**/libscipy_*.so || true

#     cp --remove-destination /usr/lib/aarch64-linux-gnu/ld-linux-aarch64.so.1 /lib/ && \
# Copy the virtualenv into a distroless image
# * These are small images that only contain the runtime dependencies
FROM gcr.io/distroless/python3-debian11
WORKDIR /app
COPY --from=build-venv /venv /venv
COPY --from=build-venv /usr/lib /usr/lib
COPY --from=build-venv /lib /lib

ENV PATH=/venv/bin:$PATH \
    RAWDIR=/work/raw \
    ZARRDIR=/work/data \
    ECCODES_DEFINITION_PATH=/venv/share/eccodes/definitions \
    PYTHONHOME=/venv

ENTRYPOINT ["/venv/bin/nwp-consumer-cli"]
VOLUME /work
STOPSIGNAL SIGINT
