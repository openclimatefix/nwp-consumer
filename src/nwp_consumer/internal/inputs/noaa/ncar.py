"""Implements a client to fetch ICON data from DWD."""
import bz2
import datetime as dt
import pathlib
import re
import typing
import urllib.request

import requests
import structlog
import xarray as xr

from nwp_consumer import internal

from ._consts import EU_ML_VARS, EU_SL_VARS, GLOBAL_ML_VARS, GLOBAL_SL_VARS
from ._models import IconFileInfo

log = structlog.getLogger()

# See https://d-nb.info/1081305452/34 for a list of ICON parameters
PARAMETER_RENAME_MAP: dict[str, str] = {
    "t_2m": internal.OCFShortName.TemperatureAGL.value,
    "clch": internal.OCFShortName.HighCloudCover.value,
    "clcm": internal.OCFShortName.MediumCloudCover.value,
    "clcl": internal.OCFShortName.LowCloudCover.value,
    "asob_s": internal.OCFShortName.DownwardShortWaveRadiationFlux.value,
    "athb_s": internal.OCFShortName.DownwardLongWaveRadiationFlux.value,
    "w_snow": internal.OCFShortName.SnowDepthWaterEquivalent.value,
    "relhum_2m": internal.OCFShortName.RelativeHumidityAGL.value,
    "u_10m": internal.OCFShortName.WindUComponentAGL.value,
    "v_10m": internal.OCFShortName.WindVComponentAGL.value,
    "clat": "lat",  # Icon has a seperate dataset for latitude...
    "clon": "lon",  # ... and longitude (for the global model)! Go figure
}

COORDINATE_ALLOW_LIST: typing.Sequence[str] = ("time", "step", "latitude", "longitude")


class Client(internal.FetcherInterface):
    """Implements a client to fetch ICON data from DWD."""

    baseurl: str  # The base URL for the ICON model
    model: str  # The model to fetch data for
    parameters: list[str]  # The parameters to fetch
    conform: bool  # Whether to rename parameters to OCF names and clear unwanted coordinates

    def __init__(self, model: str, hours: int = 48, param_group: str = "default") -> None:
        """Create a new Icon Client.

        Exposes a client for ICON data from DWD that conforms to the FetcherInterface.

        Args:
            model: The model to fetch data for. Valid models are "europe" and "global".
            param_group: The set of parameters to fetch.
                Valid groups are "default", "full", and "basic".
        """
        self.baseurl = "https://opendata.dwd.de/weather/nwp"

        match model:
            case "europe":
                self.baseurl += "/icon-eu/grib"
            case "global":
                self.baseurl += "/icon/grib"
            case _:
                raise ValueError(
                    f"unknown icon model {model}. Valid models are 'europe' and 'global'",
                )

        match (param_group, model):
            case ("default", _):
                self.parameters = list(PARAMETER_RENAME_MAP.keys())
                self.conform = True
            case ("basic", "europe"):
                self.parameters = ["t_2m", "asob_s"]
                self.conform = True
            case ("basic", "global"):
                self.parameters = ["t_2m", "asob_s", "clat", "clon"]
                self.conform = True
            case ("full", "europe"):
                self.parameters = EU_SL_VARS + EU_ML_VARS
                self.conform = False
            case ("full", "global"):
                self.parameters = GLOBAL_SL_VARS + GLOBAL_ML_VARS + ["clat", "clon"]
                self.conform = False
            case (_, _):
                raise ValueError(
                    f"unknown parameter group {param_group}."
                    "Valid groups are 'default', 'full', 'basic'",
                )

        self.model = model
        self.hours = hours

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:  # noqa: D102
        # ICON data is only available for today's date. If data hasn't been uploaded for that init
        # time yet, then yesterday's data will still be present on the server.
        if it.date() != dt.datetime.now(dt.timezone.utc).date():
            raise ValueError("ICON data is only available on today's date")
            return []

        # The ICON model only runs on the hours [00, 06, 12, 18]
        if it.hour not in [0, 6, 12, 18]:
            return []

        files: list[internal.FileInfoModel] = []

        # Files are split per parameter, level, and step, with a webpage per parameter
        # * The webpage contains a list of files for the parameter
        # * Find these files for each parameter and add them to the list
        for param in self.parameters:
            # The list of files for the parameter
            parameterFiles: list[internal.FileInfoModel] = []

            # Fetch DWD webpage detailing the available files for the parameter
            response = requests.get(f"{self.baseurl}/{it.strftime('%H')}/{param}/", timeout=3)

            if response.status_code != 200:
                log.warn(
                    event="error fetching filelisting webpage for parameter",
                    status=response.status_code,
                    url=response.url,
                    param=param,
                    inittime=it.strftime("%Y-%m-%d %H:%M"),
                )
                continue

            # The webpage's HTML <body> contains a list of <a> tags
            # * Each <a> tag has a href, most of which point to a file)
            for line in response.text.splitlines():
                # Check if the line contains a href, if not, skip it
                refmatch = re.search(pattern=r'href="(.+)">', string=line)
                if refmatch is None:
                    continue

                # The href contains the name of a file - parse this into a FileInfo object
                fi: IconFileInfo | None = None
                # If not conforming, match all files
                # * Otherwise only match single level and time invariant
                fi = _parseIconFilename(
                    name=refmatch.groups()[0],
                    baseurl=self.baseurl,
                    match_ml=not self.conform,
                    match_pl=not self.conform,
                )
                # Ignore the file if it is not for today's date or has a step > 48 (when conforming)
                if fi is None or fi.it() != it or (fi.step > self.hours and self.conform):
                    continue

                # Add the file to the list
                parameterFiles.append(fi)

            log.debug(
                event="listed files for parameter",
                param=param,
                inittime=it.strftime("%Y-%m-%d %H:%M"),
                url=response.url,
                numfiles=len(parameterFiles),
            )

            # Add the files for the parameter to the list of all files
            files.extend(parameterFiles)

        return files

    def mapTemp(self, *, p: pathlib.Path) -> xr.Dataset:  # noqa: D102
        if p.suffix != ".grib2":
            log.warn(
                event="cannot map non-grib file to dataset",
                filepath=p.as_posix(),
            )
            return xr.Dataset()

        if p.stem.endswith("_CLAT") or p.stem.endswith("_CLON"):
            # Ignore the latitude and longitude files
            return xr.Dataset()

        log.debug(event="mapping raw file to xarray dataset", filepath=p.as_posix())

        # Load the raw file as a dataset
        try:
            ds = xr.open_dataset(
                p.as_posix(),
                engine="cfgrib",
                chunks={
                    "time": 1,
                    "step": 1,
                    "variable": -1,
                    "latitude": "auto",
                    "longitude": "auto",
                },
            )
        except Exception as e:
            log.warn(
                event="error converting raw file as dataset",
                error=e,
                filepath=p.as_posix(),
            )
            return xr.Dataset()

        # Only conform the dataset if requested (defaults to True)
        if self.conform:
            # Rename the parameters to the OCF names
            # * Only do so if they exist in the dataset
            for oldParamName, newParamName in PARAMETER_RENAME_MAP.items():
                if oldParamName in ds:
                    ds = ds.rename({oldParamName: newParamName})

            # Delete unwanted coordinates
            ds = ds.drop_vars(
                names=[c for c in ds.coords if c not in COORDINATE_ALLOW_LIST],
                errors="ignore",
            )

        # Inject latitude and longitude into the dataset if they are missing
        if "latitude" not in ds:
            rawlats: list[pathlib.Path] = list(p.parent.glob("*CLAT.grib2"))
            if len(rawlats) == 0:
                log.warn(
                    event="no latitude file found for init time",
                    filepath=p.as_posix(),
                    init_time=p.parent.name,
                )
                return xr.Dataset()
            latds = xr.open_dataset(
                rawlats[0],
                engine="cfgrib",
                backend_kwargs={"errors": "ignore"},
            )
            ds = ds.assign_coords({"latitude": ("values", latds.tlat.values)})
            del latds

        if "longitude" not in ds:
            rawlons: list[pathlib.Path] = list(p.parent.glob("*CLON.grib2"))
            if len(rawlons) == 0:
                log.warn(
                    event="no longitude file found for init time",
                    filepath=p.as_posix(),
                    init_time=p.parent.name,
                )
                return xr.Dataset()
            londs = xr.open_dataset(
                rawlons[0],
                engine="cfgrib",
                backend_kwargs={"errors": "ignore"},
            )
            ds = ds.assign_coords({"longitude": ("values", londs.tlon.values)})
            del londs

        # Create chunked Dask dataset with a single "variable" dimension
        # * Each chunk is a single time step
        ds = (
            ds.rename({"time": "init_time"})
            .expand_dims("init_time")
            .expand_dims("step")
            .to_array(dim="variable", name=f"ICON_{self.model}".upper())
            .to_dataset()
            .transpose("variable", "init_time", "step", ...)
            .sortby("step")
            .sortby("variable")
            .chunk(
                {
                    "init_time": 1,
                    "step": -1,
                    "variable": -1,
                },
            )
        )

        return ds

    def downloadToTemp(  # noqa: D102
        self,
        *,
        fi: internal.FileInfoModel,
    ) -> tuple[internal.FileInfoModel, pathlib.Path]:
        log.debug(event="requesting download of file", file=fi.filename(), path=fi.filepath())
        try:
            response = urllib.request.urlopen(fi.filepath())
        except Exception as e:
            log.warn(
                event="error calling url for file",
                url=fi.filepath(),
                filename=fi.filename(),
                error=e,
            )
            return fi, pathlib.Path()

        if response.status != 200:
            log.warn(
                event="error downloading file",
                status=response.status,
                url=fi.filepath(),
                filename=fi.filename(),
            )
            return fi, pathlib.Path()

        # Extract the bz2 file when downloading
        tfp: pathlib.Path = internal.TMP_DIR / fi.filename()
        with open(tfp, "wb") as f:
            dec = bz2.BZ2Decompressor()
            for chunk in iter(lambda: response.read(16 * 1024), b""):
                f.write(dec.decompress(chunk))
                f.flush()

        log.debug(
            event="fetched all data from file",
            filename=fi.filename(),
            url=fi.filepath(),
            filepath=tfp.as_posix(),
            nbytes=tfp.stat().st_size,
        )

        return fi, tfp


def _parseIconFilename(
    name: str,
    baseurl: str,
    match_sl: bool = True,
    match_ti: bool = True,
    match_ml: bool = False,
    match_pl: bool = False,
) -> IconFileInfo | None:
    """Parse a string of HTML into an IconFileInfo object, if it contains one.

    Args:
        name: The name of the file to parse
        baseurl: The base URL for the ICON model
        match_sl: Whether to match single-level files
        match_ti: Whether to match time-invariant files
        match_ml: Whether to match model-level files
        match_pl: Whether to match pressure-level files
    """
    # Define the regex patterns to match the different types of file; X is step, L is level
    # * Single Level: `MODEL_single-level_YYYYDDMMHH_XXX_SOME_PARAM.grib2.bz2`
    slRegex = r"single-level_(\d{10})_(\d{3})_([A-Za-z_\d]+).grib"
    # * Time Invariant: `MODEL_time-invariant_YYYYDDMMHH_SOME_PARAM.grib2.bz2`
    tiRegex = r"time-invariant_(\d{10})_([A-Za-z_\d]+).grib"
    # * Model Level: `MODEL_model-level_YYYYDDMMHH_XXX_LLL_SOME_PARAM.grib2.bz2`
    mlRegex = r"model-level_(\d{10})_(\d{3})_(\d+)_([A-Za-z_\d]+).grib"
    # * Pressure Level: `MODEL_pressure-level_YYYYDDMMHH_XXX_LLLL_SOME_PARAM.grib2.bz2`
    plRegex = r"pressure-level_(\d{10})_(\d{3})_(\d+)_([A-Za-z_\d]+).grib"

    itstring = paramstring = ""
    stepstring = "000"
    # Try to match the href to one of the regex patterns
    slmatch = re.search(pattern=slRegex, string=name)
    timatch = re.search(pattern=tiRegex, string=name)
    mlmatch = re.search(pattern=mlRegex, string=name)
    plmatch = re.search(pattern=plRegex, string=name)

    if slmatch and match_sl:
        itstring, stepstring, paramstring = slmatch.groups()
    elif timatch and match_ti:
        itstring, paramstring = timatch.groups()
    elif mlmatch and match_ml:
        itstring, stepstring, levelstring, paramstring = mlmatch.groups()
    elif plmatch and match_pl:
        itstring, stepstring, levelstring, paramstring = plmatch.groups()
    else:
        return None

    it = dt.datetime.strptime(itstring, "%Y%m%d%H").replace(tzinfo=dt.timezone.utc)

    return IconFileInfo(
        it=it,
        filename=name,
        currentURL=f"{baseurl}/{it.strftime('%H')}/{paramstring.lower()}/",
        step=int(stepstring),
    )
