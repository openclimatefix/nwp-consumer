"""Defines the cache for the application.

Many sources of data do not give any option for accessing their files
via e.g. a BytesIO object. Were this the case, we could use a generic
local filesystem adaptor to handle all incoming data. Since it isn't,
and instead often a pre-existing file object is required to push data
into, a cache is required to store the data temporarily.

The cache is a simple directory structure that stores files in a
hierarchical format; with the top level directory being the source of
the data, followed by a subdirectory for the type of data (raw or
zarr), then further subdirectories according to the init time
associated with the file.

Driven actors are then responsible for mapping the cached data to the
desired storage location.

Example:
|--- /tmp/nwpc
|    |--- source1
|    |    |--- raw
|    |    |    |--- 2021
|    |    |         |--- 01
|    |    |              |--- 01
|    |    |                   |--- 0000
|    |    |                        |--- parameter1.grib
|    |    |                        |--- parameter2.grib
|    |    |                   |--- 1200
|    |    |                        |--- parameter1.grib
|    |    |                        |--- parameter2.grib
|    |    |--- zarr
|    |         |--- 2021
|    |              |--- 01
|    |                   |--- 01
|    |                        |--- 20210101T0000.zarr.zip
|    |                        |--- 20210101T1200.zarr.zip
"""

import datetime as dt
import pathlib

# --- Constants --- #

# Define the location of the consumer's cache directory
CACHE_DIR = pathlib.Path("/tmp/nwpc")  # noqa: S108
CACHE_DIR_RAW = CACHE_DIR / "raw"
CACHE_DIR_ZARR = CACHE_DIR / "zarr"

# Define the datetime format strings for creating a folder
# structure from a datetime object for raw and zarr files
IT_FOLDER_STRUCTURE_RAW = "%Y/%m/%d/%H%M"
IT_FOLDER_GLOBSTR_RAW = "*/*/*/*"
IT_FOLDER_STRUCTURE_ZARR = "%Y/%m/%d"
IT_FOLDER_GLOBSTR_ZARR = "*/*/*"

# Define the datetime format string for a zarr filename
IT_FILENAME_ZARR = "%Y%m%dT%H%M.zarr"
IT_FULLPATH_ZARR = f"{IT_FOLDER_STRUCTURE_ZARR}/{IT_FILENAME_ZARR}"

# --- Functions --- #


def rawCachePath(it: dt.datetime, filename: str) -> pathlib.Path:
    """Create a filepath to cache a raw file.

    Args:
        it: The initialisation time of the file to cache.
        filename: The name of the file (including extension).

    Returns:
        The path to the cached file.
    """
    # Build the directory structure according to the file's datetime
    parent: pathlib.Path = CACHE_DIR_RAW / it.strftime(IT_FOLDER_STRUCTURE_RAW)
    parent.mkdir(parents=True, exist_ok=True)
    return parent / filename


def zarrCachePath(it: dt.datetime) -> pathlib.Path:
    """Create a filepath to cache a zarr file.

    Args:
        it: The initialisation time of the file to cache.

    Returns:
        The path to the cache file.
    """
    # Build the directory structure according to the file's datetime
    parent: pathlib.Path = CACHE_DIR_ZARR / it.strftime(IT_FOLDER_STRUCTURE_ZARR)
    parent.mkdir(parents=True, exist_ok=True)
    return parent / it.strftime(IT_FILENAME_ZARR)
