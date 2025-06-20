"""Model repository implementation for data from ECMWF's MARS catalogue.

Repository Information
======================

Todo:
Documented structure
--------------------
"""

import dataclasses
import datetime as dt
import inspect
import logging
import os
import pathlib
from collections.abc import Callable, Iterator
from typing import override

import cfgrib
import xarray as xr
from ecmwfapi import ECMWFService
from joblib import delayed
from returns.result import Failure, ResultE, Success

from nwp_consumer.internal import entities, ports

log = logging.getLogger("nwp-consumer")


def _mars_logger(msg: str) -> None:
    """Redirect log from ECMWF API to structlog.

    Keyword Arguments:
    ------------------
    msg: The message to redirect.
    """
    debugSubstrings: list[str] = ["Requesting", "Transfering", "efficiency", "Done"]
    errorSubstrings: list[str] = ["ERROR", "FATAL"]
    if any(map(msg.__contains__, debugSubstrings)):
        log.debug("[MARS] %s", msg)
    if any(map(msg.__contains__, errorSubstrings)):
        log.warning("[MARS] %s", msg)


@dataclasses.dataclass
class _MARSRequest:
    """A request for data in the MARS format.

    See Also:
        - https://confluence.ecmwf.int/display/UDOC/MARS+request+syntax
    """

    params: list[entities.Parameter]
    """The parameters to request."""
    init_time: dt.datetime
    """The init time of the data."""
    steps: list[int]
    """The forecast steps."""
    nwse: str
    """The area of the grid to request.

    The format is 'N/W/S/E'.
    """

    classfication: str = "od"
    """The ECMWF classification given to the data."""
    expver: int = 1
    """The version of the data. 1 is 'Operational'."""
    levtype: str = "sfc"
    """The type of level."""
    stream: str = "oper"
    """The forecasting system used to generate the data.

    Options include:
        - "oper" for operational
        - "enfo" for ensemble forecast
        - "enda" for ensemble data assimilation
    """
    field_type: str = "fc"
    """Determines the type of the field to be retrieved.

    Options include:
        - "fc" for forecast
        - "em" for ensemble mean
        - "es" for ensemble standard deviation
        - "pf" for perturbed forecast (full ensemble)
    """
    grid: str = "0.1/0.1"
    """The grid resolution."""
    number: list[int] | None = None
    """The ensemble member numbers to request.

    Only relevant in full ensemble data requests.
    """

    def as_ensemble_mean_request(self) -> "_MARSRequest":
        """Create a new request for the ensemble mean."""
        return dataclasses.replace(
            self,
            field_type="em",
            stream="enfo",
        )

    def as_ensemble_std_request(self) -> "_MARSRequest":
        """Create a new request for the ensemble standard deviation."""
        return dataclasses.replace(
            self,
            field_type="es",
            stream="enfo",
        )

    def as_full_ensemble_request(self) -> "_MARSRequest":
        """Create a new request for the full ensemble."""
        return dataclasses.replace(
            self,
            field_type="pf",
            stream="enfo",
        )

    def as_operational_request(self) -> "_MARSRequest":
        """Create a new request for the operational data."""
        return dataclasses.replace(
            self,
            field_type="fc",
            stream="oper",
        )

    def gen_filename(self) -> str:
        """Generate a filename for the request.

        The stream and type are encoded into the filename, as it is required information
        at conversion time. This is because the data does not have the ensemble_stat
        dimension, so the type must be encoded in the filename to be added in.
        """
        return f"ecmwf_{self.stream}-{self.field_type}_{self.init_time:%Y%m%dT%H}.grib"

    def _to_string(self, method: str, target: str) -> str:
        """Make a MARS-formatted request string.

        Args:
            method: The method to use. Either 'list' or 'retrieve'.
            target: Path to the file where the data will be stored.
        """
        param: str = "/".join([p.metadata().grib2_code for p in self.params])
        step: str = "/".join(map(str, self.steps))

        marsReq: str = f"""
            {"list" if method == "list" else "retrieve"},
                class = {self.classfication},
                date = {self.init_time:%Y%m%d},
                expver = {self.expver},
                levtype = {self.levtype},
                stream = {self.stream},
                param = {param},
                step = {step},
                time = {self.init_time:%H},
        """
        marsReq += f"number = {'/'.join(map(str, self.number))}," if self.number is not None else ""
        marsReq += f"""
                type = {self.field_type},
                area = {self.nwse},
                grid = {self.grid},
                target = "{target}"
        """
        return inspect.cleandoc(marsReq)

    def execute(self, server: ECMWFService, folder: pathlib.Path) -> ResultE[pathlib.Path]:
        """Execute the request on the server.

        Args:
            server: The server to execute the request on.
            folder: The folder to store the data in.

        Returns:
            ResultE with path to the downloaded file.
        """
        target: pathlib.Path = folder / self.gen_filename()
        # TODO: Check against a MARS LIST call first?
        try:
            log.debug("Downloading to '%s'", target)
            server.execute(
                self._to_string(method="retrieve", target=target.as_posix()),
                target=target.as_posix(),
            )
            log.debug("Downloaded to '%s'", target)
        except Exception as e:
            return Failure(
                OSError(
                    "Failed to download data from ECMWF MARS. "
                    "Ensure request targets available parameters and steps. "
                    f"Error context: {e}",
                ),
            )
        return Success(target)


class ECMWFMARSRawRepository(ports.RawRepository):
    """Model repository implementation for archive data from ECMWF's MARS."""

    server: ECMWFService

    def __init__(self, server: ECMWFService) -> None:
        """Create a new instance of the class."""
        self.server = server

    @staticmethod
    @override
    def repository() -> entities.RawRepositoryMetadata:
        return entities.RawRepositoryMetadata(
            name="ECMWF-MARS",
            is_archive=True,
            is_order_based=False,
            delay_minutes=(60 * 26),  # 1 day, plus leeway
            max_connections=20,
            required_env=[
                "ECMWF_API_KEY",
                "ECMWF_API_EMAIL",
                "ECMWF_API_URL",
            ],
            optional_env={},
            postprocess_options=entities.PostProcessOptions(),
            available_models={
                "default": entities.Models.ECMWF_HRES_IFS_0P1DEGREE.with_region("uk"),
                "hres-ifs-uk": entities.Models.ECMWF_HRES_IFS_0P1DEGREE.with_region("uk"),
                "hres-ifs-india": entities.Models.ECMWF_HRES_IFS_0P1DEGREE.with_region("india"),
                "hres-ifs-west-europe": entities.Models.ECMWF_HRES_IFS_0P1DEGREE.with_region(
                    "west-europe",
                ),
                "ens-stat-india": entities.Models.ECMWF_ENS_STAT_0P1DEGREE.with_region("india"),
                "ens-stat-uk": entities.Models.ECMWF_ENS_STAT_0P1DEGREE.with_region("uk"),
                "ens-uk": entities.Models.ECMWF_ENS_0P1DEGREE.with_region(
                    "uk",
                ).with_chunk_count_overrides({"latitude": 1, "longitude": 1}),
            },
        )

    @staticmethod
    @override
    def model() -> entities.ModelMetadata:
        requested_model: str = os.getenv("MODEL", default="default")
        if requested_model not in ECMWFMARSRawRepository.repository().available_models:
            log.warn(
                f"Unknown model '{requested_model}' requested, falling back to default ",
                "ECMWF-MARS repository only supports "
                f"'{list(ECMWFMARSRawRepository.repository().available_models.keys())}'. "
                "Ensure MODEL environment variable is set to a valid model name.",
            )
            requested_model = "default"
        return ECMWFMARSRawRepository.repository().available_models[requested_model]

    @classmethod
    @override
    def authenticate(cls) -> ResultE["ECMWFMARSRawRepository"]:
        missing_envs = cls.repository().missing_required_envs()
        if len(missing_envs) > 0:
            return Failure(
                OSError(
                    "Cannot authenticate with ECMWF's MARS service due to "
                    f"missing required environment variables: {', '.join(missing_envs)}",
                ),
            )
        # Auth is picked up from required environment variables
        server = ECMWFService(
            service="mars",
        )
        return Success(cls(server=server))

    @override
    def fetch_init_data(
        self,
        it: dt.datetime,
    ) -> Iterator[Callable[..., ResultE[list[xr.DataArray]]]]:
        req: _MARSRequest = _MARSRequest(
            params=self.model().expected_coordinates.variable,
            init_time=it,
            steps=self.model().expected_coordinates.step,
            nwse="/".join([str(ord) for ord in self.model().expected_coordinates.nwse()]),
            number=self.model().expected_coordinates.ensemble_member,
        )

        # Yield the download and convert function with the appropriate request type
        if self.model().expected_coordinates.ensemble_stat is not None:
            for stat_req in [req.as_ensemble_mean_request(), req.as_ensemble_std_request()]:
                yield delayed(self._download_and_convert)(stat_req)
        elif self.model().expected_coordinates.ensemble_member is not None:
            yield delayed(self._download_and_convert)(req.as_full_ensemble_request())
        else:
            yield delayed(self._download_and_convert)(req.as_operational_request())

    def _download_and_convert(self, mr: _MARSRequest) -> ResultE[list[xr.DataArray]]:
        """Download and convert data from the ECMWF MARS server.

        Args:
            mr: The request to download data from.
        """
        return self._download(mr).bind(self._convert)

    def _download(self, mr: _MARSRequest) -> ResultE[pathlib.Path]:
        """Download data from the ECMWF MARS server.

        Args:
            mr: The request to download data from.
        """
        local_folder: pathlib.Path = (
            pathlib.Path(
                os.getenv(
                    "RAWDIR",
                    f"~/.local/cache/nwp/{self.repository().name}/{self.model().name}/raw",
                ),
            )
        ).expanduser()
        local_folder.mkdir(parents=True, exist_ok=True)

        local_path: pathlib.Path = local_folder / mr.gen_filename()
        if local_path.exists():
            return Success(local_path)

        return mr.execute(server=self.server, folder=local_folder)

    @staticmethod
    def _convert(path: pathlib.Path) -> ResultE[list[xr.DataArray]]:
        """Convert a local grib file to xarray DataArrays.

        Args:
            path: The path to the file to convert.
        """
        try:
            dss: list[xr.Dataset] = cfgrib.open_datasets(
                path=path.as_posix(),
                chunks={"time": 1, "step": -1, "longitude": -1, "latitude": -1},
                backend_kwargs={"indexpath": ""},
            )
        except Exception as e:
            return Failure(
                OSError(
                    f"Failed to convert raw MARS data at '{path!s}' to xarray. "
                    "Ensure file is in GRIB format and contains expected data. "
                    f"Error context: {e}",
                ),
            )

        processed_das: list[xr.DataArray] = []
        try:
            # Merge the datasets back into one
            ds: xr.Dataset = xr.merge(
                objects=dss,
                compat="override",
                combine_attrs="drop_conflicts",
            )
            del dss

            # Add in missing coordinates for mean/std data
            # * I don't really like basing this off the file name
            # * TODO: Find a better way
            if "enfo-es" in path.name:
                ds = ds.expand_dims(dim={"ensemble_stat": ["std"]})
            elif "enfo-em" in path.name:
                ds = ds.expand_dims(dim={"ensemble_stat": ["mean"]})
            da: xr.DataArray = (
                ds.pipe(
                    entities.Parameter.rename_else_drop_ds_vars,
                    allowed_parameters=ECMWFMARSRawRepository.model().expected_coordinates.variable,
                )
                .rename({"time": "init_time"})
                .expand_dims("init_time")
                .to_dataarray(name=ECMWFMARSRawRepository.model().name)
            )
            if "enfo-pf" in path.as_posix():
                da = da.rename({"number": "ensemble_member"})
            da = (
                da.drop_vars(
                    names=[
                        c
                        for c in ds.coords
                        if c not in ECMWFMARSRawRepository.model().expected_coordinates.dims
                    ],
                    errors="ignore",
                )
                .transpose(*ECMWFMARSRawRepository.model().expected_coordinates.dims)
                .sortby(variables=["step", "variable", "longitude"])
                .sortby(variables="latitude", ascending=False)
            )
            # Put each variable into its own DataArray:
            # * Each raw file does not contain a full set of parameters
            # * and so may not produce a contiguous subset of the expected coordinates.
            processed_das.extend(
                [
                    da.where(cond=da.coords["variable"] == v, drop=True)
                    for v in da.coords["variable"].values
                ],
            )

        except Exception as e:
            return Failure(
                ValueError(
                    f"Error processing DataArray for path '{path!s}'. Error context: {e}",
                ),
            )

        return Success(processed_das)
