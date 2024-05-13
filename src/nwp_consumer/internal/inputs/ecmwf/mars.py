"""Implements a client to fetch data from ECMWF."""

import datetime as dt
import inspect
import os
import pathlib
import re
import tempfile
import typing

import cfgrib
import ecmwfapi.api
import structlog
import xarray as xr
from ecmwfapi import ECMWFService

from nwp_consumer import internal

from ._models import ECMWFMarsFileInfo

log = structlog.getLogger()

# Mapping from ECMWF eccode to ECMWF short name
# * https://codes.ecmwf.int/grib/param-db/?filter=All
PARAMETER_ECMWFCODE_MAP: dict[str, str] = {
    "167.128": "tas",  # 2 metre temperature
    "165.128": "uas",  # 10 metre U-component of wind
    "166.128": "vas",  # 10 metre V-component of wind
    "47.128": "dsrp",  # Direct solar radiation
    "57.128": "uvb",  # Downward uv radiation at surface
    "188.128": "hcc",  # High cloud cover
    "187.128": "mcc",  # Medium cloud cover
    "186.128": "lcc",  # Low cloud cover
    "164.128": "clt",  # Total cloud cover
    "169.128": "ssrd",  # Surface shortwave radiation downward
    "175.128": "strd",  # Surface longwave radiation downward
    "260048": "tprate",  # Total precipitation rate
    "141.128": "sd",  # Snow depth, m
    "246.228": "u100",  # 100 metre U component of wind
    "247.228": "v100",  # 100 metre V component of wind
    "239.228": "u200",  # 200 metre U component of wind
    "240.228": "v200",  # 200 metre V component of wind
    "20.3": "vis",  # Visibility
}

AREA_MAP: dict[str, str] = {
    "uk": "62/-12/48/3",
    "nw-india": "31/68/20/79",
    "india": "35/67/6/97",
    "malta": "37/13/35/15",
    "eu": "E",
    "global": "G",
}

COORDINATE_ALLOW_LIST: typing.Sequence[str] = ("time", "step", "latitude", "longitude")


def marsLogger(msg: str) -> None:
    """Redirect log from ECMWF API to structlog.

    Keyword Arguments:
    -----------------
    msg: The message to redirect.
    """
    debugSubstrings: list[str] = ["Requesting", "Transfering", "efficiency", "Done"]
    errorSubstrings: list[str] = ["ERROR", "FATAL"]
    if any(map(msg.__contains__, debugSubstrings)):
        log.debug(event=msg, caller="mars")
    if any(map(msg.__contains__, errorSubstrings)):
        log.warning(event=msg, caller="mars")


class MARSClient(internal.FetcherInterface):
    """Implements a client to fetch data from ECMWF's MARS API."""

    server: ecmwfapi.api.ECMWFService
    area: str
    desired_params: list[str]

    def __init__(
        self,
        area: str = "uk",
        hours: int = 48,
        param_group: str = "default",
    ) -> None:
        """Create a new ECMWF Mars Client.

        Exposes a client for ECMWF's MARS API that conforms to the FetcherInterface.

        Args:
            area: The area to fetch data for. Can be one of:
                ["uk", "nw-india", "malta", "eu", "global"]
            hours: The number of hours to fetch data for. Must be less than 90.
            param_group: The parameter group to fetch data for. Can be one of:
                ["default", "basic"]
        """
        self.server = ECMWFService(service="mars", log=marsLogger)

        if area not in AREA_MAP:
            raise KeyError(f"area must be one of {list(AREA_MAP.keys())}")
        self.area = area

        self.hours = hours

        match param_group:
            case "basic":
                log.debug(event="Initialising ECMWF Client with basic parameter group")
                self.desired_params = ["167.128", "169.128"]  # 2 Metre Temperature, Dswrf
            case _:
                self.desired_params = list(PARAMETER_ECMWFCODE_MAP.keys())

    def datasetName(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return f"ECMWF_{self.area.upper()}"

    def getInitHours(self) -> list[int]:  # noqa: D102
        # MARS data of the operational archive is available at 00 and 12 UTC
        return [0, 12]

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:  # noqa: D102
        # Ignore inittimes that don't correspond to valid hours
        if it.hour not in self.getInitHours():
            return []

        # MARS requests can only ask for data that is more than 24 hours old: see
        # https://confluence.ecmwf.int/display/UDOC/MARS+access+restrictions
        if it > dt.datetime.now(tz=dt.UTC) - dt.timedelta(hours=24):
            raise ValueError(
                "ECMWF MARS requests can only ask for data that is more than 24 hours old",
            )
            return []

        tf = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)

        with open(tf.name, "w") as f:
            req: str = self._buildMarsRequest(
                list_only=True,
                it=it,
                target=tf.name,
                params=self.desired_params,
                steps=list(range(0, self.hours)),
            )

            log.debug(event="listing ECMWF MARS inittime data", request=req, inittime=it)

            try:
                self.server.execute(req=req, target=tf.name)
            except ecmwfapi.api.APIException as e:
                log.warn("error listing ECMWF MARS inittime data", error=e)
                return []

        # Explicitly check that the MARS listing file is readable and non-empty
        if (os.access(tf.name, os.R_OK) is False) or (os.stat(tf.name).st_size < 100):
            log.warn(
                event="ECMWF filelisting is empty, check error logs",
                filepath=tf.name,
            )
            return []

        # Ensure only available parameters are requested by populating the
        # `available_params` list according to the result of the list request
        with open(tf.name) as f:
            file_contents: str = f.read()
            available_data = _parseListing(fileData=file_contents)
            for parameter in self.desired_params:
                if parameter not in available_data["params"]:
                    log.warn(
                        event=f"ECMWF MARS inittime data does not contain parameter {parameter}",
                        parameter=parameter,
                        inittime=it,
                    )

        log.debug(
            event="Listed raw files for ECMWF MARS inittime",
            inittime=it,
            available_params=available_data["params"],
        )

        # Clean up the temporary file
        tf.close()
        os.unlink(tf.name)

        return [
            ECMWFMarsFileInfo(
                inittime=it,
                area=self.area,
                params=available_data["params"],
                steplist=available_data["steps"],
            ),
        ]

    def downloadToCache(  # noqa: D102
        self,
        *,
        fi: internal.FileInfoModel,
    ) -> pathlib.Path:
        cfp: pathlib.Path = internal.rawCachePath(it=fi.it(), filename=fi.filename())

        req: str = self._buildMarsRequest(
            list_only=False,
            it=fi.it(),
            target=cfp.as_posix(),
            params=fi.variables(),
            steps=fi.steps(),
        )

        log.debug(
            event="fetching ECMWF MARS data",
            request=req,
            inittime=fi.it(),
            filename=fi.filename(),
        )

        try:
            self.server.execute(req=req, target=cfp.as_posix())
        except ecmwfapi.api.APIException as e:
            log.warn("error fetching ECMWF MARS data", error=e)
            return pathlib.Path()

        if cfp.exists() is False:
            log.warn("ECMWF data file does not exist", filepath=cfp.as_posix())
            return pathlib.Path()

        log.debug(
            event="fetched all data from MARS",
            filename=fi.filename(),
            filepath=cfp.as_posix(),
            nbytes=cfp.stat().st_size,
        )

        return cfp

    def mapCachedRaw(self, *, p: pathlib.Path) -> xr.Dataset:
        """Overrides the corresponding method in the parent class."""
        if p.suffix != ".grib":
            log.warn(event="cannot map non-grib file to dataset", filepath=p.as_posix())
            return xr.Dataset()

        log.debug(event="mapping raw file to xarray dataset", filepath=p.as_posix())

        # Load the wholesale file as a list of datasets
        # * cfgrib loads multiple hypercubes for a single multi-parameter grib file
        # * Can also set backend_kwargs={"indexpath": ""}, to avoid the index file
        try:
            datasets: list[xr.Dataset] = cfgrib.open_datasets(
                path=p.as_posix(),
                chunks={
                    "time": 1,
                    "step": -1,
                    "longitude": "auto",
                    "latitude": "auto",
                },
                backend_kwargs={"indexpath": ""},
            )
        except Exception as e:
            log.warn(event="error converting raw file to dataset", filepath=p.as_posix(), error=e)
            return xr.Dataset()

        # Merge the datasets back into one
        wholesaleDataset = xr.merge(
            objects=datasets,
            compat="override",
            combine_attrs="drop_conflicts",
        )
        del datasets

        # Map the data to the internal dataset representation
        # * Transpose the Dataset so that the dimensions are correctly ordered
        # * Rechunk the data to a more optimal size
        wholesaleDataset = (
            wholesaleDataset.rename({"time": "init_time"})
            .expand_dims("init_time")
            .transpose("init_time", "step", "latitude", "longitude")
            .sortby("step")
            .chunk(
                {
                    "init_time": 1,
                    "step": -1,
                    "latitude": len(wholesaleDataset.latitude) // 2,
                    "longitude": len(wholesaleDataset.longitude) // 2,
                },
            )
        )

        return wholesaleDataset

    def parameterConformMap(self) -> dict[str, internal.OCFParameter]:
        """Overrides the corresponding method in the parent class."""
        return {
            "tas": internal.OCFParameter.TemperatureAGL,
            "t2m": internal.OCFParameter.TemperatureAGL,
            "uas": internal.OCFParameter.WindUComponentAGL,
            "vas": internal.OCFParameter.WindVComponentAGL,
            "dsrp": internal.OCFParameter.DirectSolarRadiation,
            "uvb": internal.OCFParameter.DownwardUVRadiationAtSurface,
            "hcc": internal.OCFParameter.HighCloudCover,
            "mcc": internal.OCFParameter.MediumCloudCover,
            "lcc": internal.OCFParameter.LowCloudCover,
            "clt": internal.OCFParameter.TotalCloudCover,
            "ssrd": internal.OCFParameter.DownwardShortWaveRadiationFlux,
            "strd": internal.OCFParameter.DownwardLongWaveRadiationFlux,
            "tprate": internal.OCFParameter.RainPrecipitationRate,
            "sd": internal.OCFParameter.SnowDepthWaterEquivalent,
            "u100": internal.OCFParameter.WindUComponent100m,
            "v100": internal.OCFParameter.WindVComponent100m,
            "u200": internal.OCFParameter.WindUComponent200m,
            "v200": internal.OCFParameter.WindVComponent200m,
            "vis": internal.OCFParameter.VisibilityAGL,
        }

    def _buildMarsRequest(
        self,
        *,
        list_only: bool,
        it: dt.datetime,
        target: str,
        params: list[str],
        steps: list[int],
    ) -> str:
        """Build a MARS request according to the parameters of the client.

        Args:
            list_only: Whether to build a request that only lists the files that match
                the request, or whether to build a request that downloads the files
                that match the request.
            it: The initialisation time to request data for.
            target: The path to the target file to write the data to.
            params: The parameters to request data for.
            steps: The steps to request data for.

        Returns:
            The MARS request.
        """
        marsReq: str = f"""
            {"list" if list_only else "retrieve"},
                class    = od,
                date     = {it.strftime("%Y%m%d")},
                expver   = 1,
                levtype  = sfc,
                param    = {'/'.join(params)},
                step     = {'/'.join(map(str, steps))},
                stream   = oper,
                time     = {it.strftime("%H")},
                type     = fc,
                area     = {AREA_MAP[self.area]},
                grid     = 0.1/0.1,
                target   = "{target}"
        """

        return inspect.cleandoc(marsReq)


def _parseListing(fileData: str) -> dict[str, list[str] | list[int]]:
    """Parse the response from a MARS list request.

    When calling LIST to MARS, the response is a file containing the available
    parameters, steps, times and sizes etc. This function parses the file to
    extract the available parameters.

    The files contains some metadata, followed by a table as follows:

    ```
    file length   missing offset      param   step
    0    13204588 .       149401026   20.3    0
    0    13204588 .       502365532   47.128  0
    0    13204588 .       568388472   57.128  0
    0    19804268 .       911707760   141.128 0
    0    13204588 .       1050353320  164.128 0

    Grand Total
    ```

    This function uses positive lookahead and lookbehind regex to extract the
    lines between the table header and the "Grand Total" line. The fourth
    column of each line is the parameter. The fifth is the step.

    Args:
        fileData: The data from the file.

    Returns:
        A dict of parameters and steps available in the remote file.
    """
    tablematch = re.search(
        pattern=r"(?<!step)[\s\.\d]*?(?=\n.*?\nGrand)",
        string=fileData,
    )
    out: dict = {"steps": set(), "params": set()}
    if tablematch:
        tablelines: list[str] = tablematch.group(0).split("\n")
        for line in tablelines:
            if len(line.split()) > 4:
                out["steps"].add(int(line.split()[5]))
                out["params"].add(line.split()[4])
    out = {k: sorted(list(v)) for k, v in out.items()}
    return out
