"""Implements a client to fetch ICON data from DWD."""
import bz2
import datetime as dt
import pathlib
import re
import urllib.request

import numpy as np
import requests
import structlog
import xarray as xr

from nwp_consumer import internal

from ._consts import EU_ML_VARS, EU_SL_VARS, GLOBAL_ML_VARS, GLOBAL_SL_VARS
from ._models import IconFileInfo

log = structlog.getLogger()


class Client(internal.FetcherInterface):
    """Implements a client to fetch ICON data from DWD."""

    baseurl: str  # The base URL for the ICON model
    model: str  # The model to fetch data for
    parameters: list[str]  # The parameters to fetch

    def __init__(self, model: str, hours: int = 48, param_group: str = "default") -> None:
        """Create a new Icon Client.

        Exposes a client for ICON data from DWD that conforms to the FetcherInterface.

        Args:
            model: The model to fetch data for. Valid models are "europe" and "global".
            hours: The number of hours to fetch data for.
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
                self.parameters = [
                    "t_2m",
                    "clch",
                    "clcm",
                    "clcl",
                    "asob_s",
                    "athb_s",
                    "w_snow",
                    "relhum_2m",
                    "u_10m",
                    "v_10m",
                    "clat",
                    "clon",
                ]
            case ("basic", "europe"):
                self.parameters = ["t_2m", "asob_s"]
            case ("basic", "global"):
                self.parameters = ["t_2m", "asob_s", "clat", "clon"]
            case ("single-level", "europe"):
                self.parameters = EU_SL_VARS
            case ("single-level", "global"):
                self.parameters = [*GLOBAL_SL_VARS, "clat", "clon"]
            case ("multi-level", "europe"):
                self.parameters = EU_ML_VARS
            case ("multi-level", "global"):
                self.parameters = [*GLOBAL_ML_VARS, "clat", "clon"]
            case ("full", "europe"):
                self.parameters = EU_SL_VARS + EU_ML_VARS
            case ("full", "global"):
                self.parameters = [*GLOBAL_SL_VARS, *GLOBAL_ML_VARS, "clat", "clon"]
            case (_, _):
                raise ValueError(
                    f"unknown parameter group {param_group}."
                    "Valid groups are 'default', 'full', 'basic', 'single-level', 'multi-level'",
                )

        self.model = model
        self.hours = hours

    def datasetName(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return f"ICON_{self.model}".upper()

    def getInitHours(self) -> list[int]:  # noqa: D102
        return [0, 6, 12, 18]

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:  # noqa: D102
        # ICON data is only available for today's date. If data hasn't been uploaded for that init
        # time yet, then yesterday's data will still be present on the server.
        if dt.datetime.now(dt.UTC) - it > dt.timedelta(days=1):
            log.warn(
                event="requested init time is too old",
                inittime=it.strftime("%Y-%m-%d %H:%M"),
            )
            return []

        # Ignore inittimes that don't correspond to valid hours
        if it.hour not in self.getInitHours():
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
                # Find the corresponding files for the parameter
                fi = _parseIconFilename(
                    name=refmatch.groups()[0],
                    baseurl=self.baseurl,
                    match_ml=True,
                    match_pl=True,
                )
                # Ignore the file if it is not for today's date
                # or has a step > desired hours
                if fi is None or fi.it() != it or (fi.step > self.hours):
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

    def mapCachedRaw(self, *, p: pathlib.Path) -> xr.Dataset:
        """Overrides the corresponding method in the parent class."""
        if p.suffix != ".grib2":
            log.warn(
                event="cannot map non-grib file to dataset",
                filepath=p.as_posix(),
            )
            return xr.Dataset()

        if "_CLAT" in p.stem or "_CLON" in p.stem:
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
                    "latitude": "auto",
                    "longitude": "auto",
                },
                backend_kwargs={"indexpath": ""},
            )
        except Exception as e:
            log.warn(
                event="error converting raw file as dataset",
                error=e,
                filepath=p.as_posix(),
            )
            return xr.Dataset()

        # Most datasets are opened as xarray datasets with "step" as a scalar (nonindexed) coordinate
        # Some do not, so add it in manually
        if "step" not in ds.coords:
            ds = ds.assign_coords({"step": np.timedelta64(0, 'ns')})

        # The global data is stacked as a 1D values array without lat or long data
        # * Manually add it in from the CLAT and CLON files
        if self.model == "global":
            ds = _addLatLon(ds=ds, p=p)

        # Rename variables to match their listing online to prevent single/multi overlap
        # * This assumes the name of the file locally is the same as online
        pmatch = re.search(r"_\d{3}_([A-Z0-9_]+).grib", p.name)
        if pmatch is not None:
            var_name = pmatch.groups()[0]
            ds = ds.rename({list(ds.data_vars.keys())[0]: var_name.lower()})

        # Map the data to the internal dataset representation
        # * Transpose the Dataset so that the dimensions are correctly ordered
        # * Rechunk the data to a more optimal size
        ds = (
            ds.rename({"time": "init_time"})
            .expand_dims(["init_time", "step"])
            .drop_vars(["valid_time", "number", "surface", "heightAboveGround", "level", "isobaricLevel"], errors="ignore")
            .sortby("step")
            .transpose("init_time", "step", ...)
            .chunk(
                {
                    "init_time": 1,
                    "step": -1,
                },
            )
        )

        return ds

    def downloadToCache(  # noqa: D102
        self,
        *,
        fi: internal.FileInfoModel,
    ) -> pathlib.Path:
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
            return pathlib.Path()

        if response.status != 200:
            log.warn(
                event="error downloading file",
                status=response.status,
                url=fi.filepath(),
                filename=fi.filename(),
            )
            return pathlib.Path()

        # Extract the bz2 file when downloading
        cfp: pathlib.Path = internal.rawCachePath(it=fi.it(), filename=fi.filename())
        with open(str(cfp), "wb") as f:
            dec = bz2.BZ2Decompressor()
            for chunk in iter(lambda: response.read(16 * 1024), b""):
                f.write(dec.decompress(chunk))
                f.flush()

        if not cfp.exists():
            log.warn(
                event="error extracting bz2 file",
                filename=fi.filename(),
                url=fi.filepath(),
                filepath=cfp.as_posix(),
            )
            return pathlib.Path()

        log.debug(
            event="fetched all data from file",
            filename=fi.filename(),
            url=fi.filepath(),
            filepath=cfp.as_posix(),
            nbytes=cfp.stat().st_size,
        )

        return cfp

    def parameterConformMap(self) -> dict[str, internal.OCFParameter]:
        """Overrides the corresponding method in the parent class."""
        # See https://d-nb.info/1081305452/34 for a list of ICON parameters
        return {
            "t_2m": internal.OCFParameter.TemperatureAGL,
            "clch": internal.OCFParameter.HighCloudCover,
            "clcm": internal.OCFParameter.MediumCloudCover,
            "clcl": internal.OCFParameter.LowCloudCover,
            "asob_s": internal.OCFParameter.DownwardShortWaveRadiationFlux,
            "athb_s": internal.OCFParameter.DownwardLongWaveRadiationFlux,
            "w_snow": internal.OCFParameter.SnowDepthWaterEquivalent,
            "relhum_2m": internal.OCFParameter.RelativeHumidityAGL,
            "u_10m": internal.OCFParameter.WindUComponentAGL,
            "v_10m": internal.OCFParameter.WindVComponentAGL,
            "clat": "lat",  # Icon has a seperate dataset for latitude...
            "clon": "lon",  # ... and longitude (for the global model)! Go figure
        }


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

    it = dt.datetime.strptime(itstring, "%Y%m%d%H").replace(tzinfo=dt.UTC)

    return IconFileInfo(
        it=it,
        filename=name,
        currentURL=f"{baseurl}/{it.strftime('%H')}/{paramstring.lower()}/",
        step=int(stepstring),
    )


def _addLatLon(*, ds: xr.Dataset, p: pathlib.Path) -> xr.Dataset:
    """Add latitude and longitude data to the dataset.

    Global ICON files do not contain latitude and longitude data,
    opting instead for a single `values` dimension. The lats and longs are then
    accessible from seperate files. This function injects the lat and lon data
    from these files into the dataset.

    :param ds: The dataset to reshape
    :param p: The path to the file being reshaped
    """
    # Adapted from https://stackoverflow.com/a/62667154 and
    # https://github.com/SciTools/iris-grib/issues/140#issuecomment-1398634288

    # Inject latitude and longitude into the dataset if they are missing
    if "latitude" not in ds.dims:
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
        ).load()
        tiledlats = latds["tlat"].data
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
        ).load()
        tiledlons = londs["tlon"].data
        del londs

    if ds.sizes["values"] != len(tiledlats) or ds.sizes["values"] != len(tiledlons):
        raise ValueError(
            f"dataset has {ds.sizes['values']} values, "
            f"but expected {len(tiledlats) * len(tiledlons)}",
        )

    # Create new coordinates,
    # which give the `latitude` and `longitude` position for each position in the `values` dimension:

    ds = ds.assign_coords(
        {
            "latitude": ("values", tiledlats),
            "longitude": ("values", tiledlons),
        },
    )

    return ds
