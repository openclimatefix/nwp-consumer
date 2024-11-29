"""Model repository implementation for data from ECMWF's MARS catalogue.

Repository Information
======================

TODO

Documented structure
--------------------
"""

import inspect
import datetime as dt
import dataclasses
import logging
import os
import pathlib
import re
from collections.abc import Callable, Iterator
from typing import override
from ecmwfapi import ECMWFService

import cfgrib
import s3fs
import xarray as xr
from joblib import delayed
from returns.result import Failure, ResultE, Success

from nwp_consumer.internal import entities, ports

log = logging.getLogger("nwp-consumer")


def marsLogger(msg: str) -> None:
    """Redirect log from ECMWF API to structlog.

    Keyword Arguments:
    -----------------
    msg: The message to redirect.
    """
    debugSubstrings: list[str] = ["Requesting", "Transfering", "efficiency", "Done"]
    errorSubstrings: list[str] = ["ERROR", "FATAL"]
    if any(map(msg.__contains__, debugSubstrings)):
        log.debug("[MARS] %s", msg)
    if any(map(msg.__contains__, errorSubstrings)):
        log.warning("[MARS] %s", msg)


class ECMWFMARSModelRepository(ports.ModelRepository):
    """Model repository implementation for archive data from ECMWF's MARS."""

    server: ECMWFService

    def __init__(self, server: ECMWFService) -> None:
        """Create a new instance of the class."""
        self.server = server

    @staticmethod
    @override
    def repository() -> entities.ModelRepositoryMetadata:
        return entities.ModelRepositoryMetadata(
            name="ECMWF-MARS",
            is_archive=True,
            is_order_based=False,
            running_hours=[0, 12],
            delay_minutes=(60 * 26),  # 1 day, plus leeway
            max_connections=20,
            required_env=[
                "ECMWF_API_KEY",
                "ECMWF_API_EMAIL",
                "ECMWF_API_URL",
            ],
            optional_env={},
            postprocess_options=entities.PostProcessOptions(),
        )

    @staticmethod
    @override
    def model() -> entities.ModelMetadata:
        return entities.ModelMetadata(
            name="ENS",
            resolution="0.1 degrees",
            expected_coordinates=entities.NWPDimensionCoordinateMap(
                init_time=[],
                step=list(range(0, 85, 1)),
                variable=[
                    entities.Parameter.WIND_U_COMPONENT_10m,
                    entities.Parameter.WIND_V_COMPONENT_10m,
                    entities.Parameter.DOWNWARD_SHORTWAVE_RADIATION_FLUX_GL,
                ],
                ensemble_stat=["mean", "std", "P10", "P25", "P75", "P90"],
                latitude=list(range(90, -90, -1)),
                longitude=list(range(-180, 180, 1)),
            ),
        )


    @classmethod
    @override
    def authenticate(cls) -> ResultE["ECMWFMARSModelRepository"]:
        missing_envs = cls.repository().missing_required_envs()
        if len(missing_envs) > 0:
            return Failure(OSError(
                "Cannot authenticate with ECMWF's MARS service due to "
                f"missing required environment variables: {', '.join(missing_envs)}",
            ))
        # Auth is picked up from required environment variables
        server = ECMWFService(
            service="mars",
            log=marsLogger,
        )
        return Success(cls(server=server))


@dataclasses.dataclass
class _MARSRequest:
    """A request for data in the MARS format."""

    params: list[str]
    """The parameters to request.

    Presented in the format of a list of ECMWF parameter codes, e.g.:
    ['165.128', '41.128']
    """
    init_time: dt.datetime
    """The init time of the data."""
    steps: list[int]
    """The forecast steps."""
    area: list[int]
    """The area of the grid to request.

    The four integers are N, W, S, E.
    """

    classfication: str = "od"
    """The ECMWF classification given to the data."""
    expver: int = 1
    """The version of the data. 1 is 'Operational'."""
    levtype: str = "sfc"
    """The type of level."""
    stream: str = "oper"
    """The forecasting system used to generate the data."""
    field_type: str = "fc"
    """Determines the type of the field to be retrieved."""
    grid: str = "0.1/0.1"
    """The grid resolution."""

    def to_request(self, method: str, target: str) -> str:
        """Make a MARS-formatted request string.

        Args:
            method: The method to use. Either 'list' or 'retrieve'.
            target: Path to the file where the data will be stored.
        """
        marsReq: str = f"""
            {"list" if method == "list" else "retrieve"},
                class = {self.classfication},
                date = {self.init_time:%Y%m%d},
                expver = {self.expver},
                levtype = {self.levtype},
                param = {'/'.join(self.params)},
                step = {'/'.join(map(str, self.steps))},
                stream = {self.stream},
                time = {self.init_time:%H},
                type = {self.field_type},
                area = {'/'.join(map(str, self.area))},
                grid = {self.grid},
                target = "{target}"
        """
        return inspect.cleandoc(marsReq)


