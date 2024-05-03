import datetime as dt
import pathlib

import attrs

from .area import Area


@attrs.frozen
class SourceRepositoryMetadata:
    """Metadata for a source repository.

    :param name: The name of the repository.
    :param is_archive: Whether the repository is a complete archival set.
            Archival datasets are able to backfill old data.
            Non-archival datasets only provide a limited window of data.
    :param is_order_based: Whether the repository is order-based.
            This means parameters cannot be chosen freely,
            but rather are defined by pre-selected agreements
            with the provider.
    :param running_hours: The running hours or the source.
    :param delay_minutes: The delay in minutes between init time and the
            time to which the data is actually available.
    :param available_steps: The available steps of the repository.
    :param available_areas: The available areas of the repository.
    :param required_env: Environment variables required for usage.
    :param optional_env: Optional environment variables.
    """

    name: str = attrs.field(validator=attrs.validators.min_len(3))
    is_archive: bool
    is_order_based: bool
    running_hours: list[int]
    delay_minutes: int
    available_steps: list[int]
    available_areas: list[Area]
    required_env: list[str]
    optional_env: dict[str, str]


@attrs.frozen
class SourceFileMetadata:
    """Metadata for a raw file.

    :param name: The name of the file.
    :param path: The relevant (remote or local) path to the file.
    :param extension: The file extension, including the dot (e.g. '.grib').
    :param size_bytes: The size of the file in bytes.
    :param steps: The steps within the file.
    :param parameters: The parameters within the file.
    :param init_time: The init time of the file.
    """

    name: str
    path: pathlib.Path
    extension: str
    size_bytes: int
    steps: list[int]
    parameters: list[str]
    init_time: dt.datetime


@attrs.frozen
class PostProcessOptions:
    """Options for post-processing NWP data.

    :param create_variable_dimension: Whether to create a variable dimension.
            Squashes all data variables into their own "variable" dimension.
    :param rename_variables: Whether to rename variables.
    """

    create_variable_dimension: bool = False
    rename_variables: bool = False
