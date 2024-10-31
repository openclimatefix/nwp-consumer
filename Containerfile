# POTENTIAL FOR SMALLER CONTAINERFILE IF THIS CAN BE GOT WORKING


# # --- Base Python image -----------------------------------------------------------------
# FROM python:3.12-bookworm AS python-base
# 
# # --- Distroless Container creation -----------------------------------------------------
# FROM gcr.io/distroless/cc-debian12 AS python-distroless
# 
# ARG CHIPSET_ARCH=aarch64-linux-gnu
# 
# # Copy the python installation from the base image
# COPY --from=python-base /usr/local/lib/ /usr/local/lib/
# COPY --from=python-base /usr/local/bin/python /usr/local/bin/python
# COPY --from=python-base /etc/ld.so.cache /etc/ld.so.cache
# 
# # Add common compiled libraries
# COPY --from=python-base /usr/lib/${CHIPSET_ARCH}/libz.so.1 /lib/${CHIPSET_ARCH}/
# COPY --from=python-base /usr/lib/${CHIPSET_ARCH}/libffi.so.8 /lib/${CHIPSET_ARCH}/
# COPY --from=python-base /usr/lib/${CHIPSET_ARCH}/libbz2.so.1.0 /lib/${CHIPSET_ARCH}/
# COPY --from=python-base /usr/lib/${CHIPSET_ARCH}/libm.so.6 /lib/${CHIPSET_ARCH}/
# COPY --from=python-base /usr/lib/${CHIPSET_ARCH}/libc.so.6 /lib/${CHIPSET_ARCH}/
# 
# # Don't generate .pyc, enable tracebacks
# ENV LANG=C.UTF-8 \
#     LC_ALL=C.UTF-8 \
#     PYTHONDONTWRITEBYTECODE=1 \
#     PYTHONFAULTHANDLER=1
# 
# # Check python installation works
# COPY --from=python-base /bin/rm /bin/rm
# COPY --from=python-base /bin/sh /bin/sh
# RUN python --version
# RUN rm /bin/sh /bin/rm
# 
# # --- Virtualenv builder image ----------------------------------------------------------
# FROM python-base AS build-venv
# COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
# 
# ENV UV_LINK_MODE=copy \
#     UV_COMPILE_BYTECODE=1 \
#     UV_PYTHON_DOWNLOADS=never \
#     UV_PYTHON=python3.12 \
#     UV_NO_CACHE=1 \
#     CFLAGS="-g0 -Wl,--strip-all" \
#     VENV=/.venv
# 
# COPY pyproject.toml ./
# 
# # Synchronize DEPENDENCIES without the application itself.
# # This layer is cached until uv.lock or pyproject.toml change.
# # Delete any unwanted parts of the installed packages to reduce size
# RUN uv venv ${VENV} && \
#     echo "Installing dependencies into ${VENV}" && \
#     mkdir src && \
#     du -h ${VENV}/lib/python3.12/site-packages && \
#     uv sync --no-dev --no-install-project && \
#     echo "Copying libpython package into ${VENV}" && \
#     cp --remove-destination /usr/local/bin/python3.12 ${VENV}/bin/python && \
#     cp /usr/local/lib/libpython3.12.so.1.0 ${VENV}/lib/ && \
#     echo "Optimizing site-packages" && \
#     rm -r ${VENV}/lib/python3.12/site-packages/**/tests && \
#     du -h ${VENV}/lib/python3.12/site-packages | sort -h | tail -n 4
# 
# COPY . /src
# RUN uv pip install --no-deps /src && ls /.venv/bin
# 
# # --- Distroless App image --------------------------------------------------------------
# FROM python-distroless
# 
# COPY --from=build-venv /.venv /venv
# 
# ENV RAWDIR=/work/raw \
#     ZARRDIR=/work/data \
#     ECCODES_DEFINITION_PATH=.venv/share/eccodes/definitions
# 
# ENTRYPOINT ["/venv/bin/nwp-consumer-cli"]
# VOLUME /work
# STOPSIGNAL SIGINT


# WORKING CONTAINERFILE

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
RUN apt-get -qq update && apt-get -qq -y install gcc && \
    echo "Creating virtualenv at /venv" && \
    conda create --quiet --yes -p /venv python=3.12 numcodecs eccodes
RUN echo "Installing dependencies into /venv" && \
    cd /_lock && \
    mkdir src && \
    uv sync --no-dev --no-install-project && \
    echo "Optimizing /venv site-packages" && \
    rm -r /venv/lib/python3.12/site-packages/**/tests && \
    rm -r /venv/lib/python3.12/site-packages/**/_*cache* && \
    rm -r /venv/share/eccodes/definitions/bufr

# Then install the application itself
# * Delete the test and cache folders from installed packages to reduce size
COPY . /src
RUN uv pip install --no-deps --python=$UV_PROJECT_ENVIRONMENT /src

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
