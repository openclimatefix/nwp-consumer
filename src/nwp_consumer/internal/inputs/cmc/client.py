"""Implements a client to fetch GDPS/GEPS data from CMC."""
import datetime as dt
import pathlib
import re
import typing
import urllib.request

import requests
import structlog
import xarray as xr

from nwp_consumer import internal

from ._consts import GDPS_VARIABLES, GEPS_VARIABLES
from ._models import CMCFileInfo
from ... import OCFParameter

log = structlog.getLogger()


COORDINATE_ALLOW_LIST: typing.Sequence[str] = ("time", "step", "latitude", "longitude")


class Client(internal.FetcherInterface):
    """Implements a client to fetch GDPS/GEPS data from CMC."""

    baseurl: str  # The base URL for the GDPS/GEPS model
    model: str  # The model to fetch data for
    parameters: list[str]  # The parameters to fetch

    def __init__(self, model: str, hours: int = 48, param_group: str = "default") -> None:
        """Create a new GDPS Client.

        Exposes a client for GDPS and GEPS data from Canada CMC that conforms to the FetcherInterface.

        Args:
            model: The model to fetch data for. Valid models are "gdps" and "geps".
            param_group: The set of parameters to fetch.
                Valid groups are "default", "full", and "basic".
        """
        self.baseurl = "https://dd.weather.gc.ca"

        match model:
            case "gdps":
                self.baseurl += "/model_gem_global/15km/grib2/lat_lon/"
            case "geps":
                self.baseurl += "/ensemble/geps/grib2/raw/"
            case _:
                raise ValueError(
                    f"unknown GDPS/GEPS model {model}. Valid models are 'gdps' and 'geps'",
                )

        match (param_group, model):
            case ("default", _):
                self.parameters = ["t", "tclc", "dswrf", "dlwrf", "snod", "rh", "u", "v"]
            case ("full", "geps"):
                self.parameters = GEPS_VARIABLES
            case ("full", "gdps"):
                self.parameters = GDPS_VARIABLES
            case ("basic", "geps"):
                self.parameters = GEPS_VARIABLES[:2]
            case ("basic", "gdps"):
                self.parameters = GDPS_VARIABLES[:2]
            case (_, _):
                raise ValueError(
                    f"unknown parameter group {param_group}."
                    "Valid groups are 'default', 'full', 'basic'",
                )

        self.model = model
        self.hours = hours

    def datasetName(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return f"CMC_{self.model}".upper()

    def getInitHours(self) -> list[int]:  # noqa: D102
        return [0, 12]

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:  # noqa: D102
        # GDPS data is only available for today's and yesterday's date.
        # If data hasn't been uploaded for that inittime yet,
        # then yesterday's data will still be present on the server.
        if it.date() != dt.datetime.now(dt.UTC).date():
            raise ValueError("GDPS/GEPS data is only available on today's date")

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

            # Fetch CMC webpage detailing the available files for the timestep
            response = requests.get(f"{self.baseurl}/{it.strftime('%H')}/000/", timeout=3)

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
                fi: CMCFileInfo | None = None
                # If downloading all variables, match all files
                # * Otherwise only match single level and time invariant
                fi = _parseCMCFilename(
                    name=refmatch.groups()[0],
                    baseurl=self.baseurl,
                    match_pl=self.parameters in ["t", "tclc", "dswrf", "dlwrf", "snod", "rh", "u", "v"],
                    match_hl=self.parameters in ["t", "tclc", "dswrf", "dlwrf", "snod", "rh", "u", "v"],
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

    def mapCachedRaw(self, *, p: pathlib.Path) -> xr.Dataset:  # noqa: D102
        if p.suffix != ".grib2":
            log.warn(
                event="cannot map non-grib file to dataset",
                filepath=p.as_posix(),
            )
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
            )
        except Exception as e:
            log.warn(
                event="error converting raw file as dataset",
                error=e,
                filepath=p.as_posix(),
            )
            return xr.Dataset()
        # Rename variable to the value, as some have unknown as the name
        if next(iter(ds.data_vars.keys())) == "unknown":
            ds = ds.rename({"unknown": str(p.name).split("_")[2].lower()})

        # Rename variables that are both pressure level and surface
        if "surface" in list(ds.coords):
            ds = ds.rename({"surface": "heightAboveGround"})

        if "heightAboveGround" in list(ds.coords) and next(iter(ds.data_vars.keys())) in [
            "q",
            "t",
            "u",
            "v",
        ]:
            # Rename data variable to add _surface to it so merging works later
            ds = ds.rename(
                {next(iter(ds.data_vars.keys())): f"{next(iter(ds.data_vars.keys()))}_surface"},
            )

        if "isobaricInhPa" in list(ds.coords):
            if "rh" in list(ds.data_vars.keys()):
                ds = ds.rename({"isobaricInhPa": "isobaricInhPa_humidity"})
            if "absv" in list(ds.data_vars.keys()) or "vvel" in list(ds.data_vars.keys()):
                ds = ds.rename({"isobaricInhPa": "isobaricInhPa_absv_vvel"})

        # Map the data to the internal dataset representation
        # * Transpose the Dataset so that the dimensions are correctly ordered
        # * Rechunk the data to a more optimal size
        ds = (
            ds.rename({"time": "init_time"})
            .expand_dims("init_time")
            .expand_dims("step")
            .transpose("init_time", "step", ...)
            .sortby("step")
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

        cfp: pathlib.Path = internal.rawCachePath(it=fi.it(), filename=fi.filename())
        with open(cfp, "wb") as f:
            f.write(response.read())

        log.debug(
            event="fetched all data from file",
            filename=fi.filename(),
            url=fi.filepath(),
            filepath=cfp.as_posix(),
            nbytes=cfp.stat().st_size,
        )

        return cfp

    def parameterConformMap(self) -> dict[str, OCFParameter]:
        """Overrides the corresponding method in the parent class."""
        # See https://eccc-msc.github.io/open-data/msc-data/nwp_gdps/readme_gdps-datamart_en/
        # for a list of CMC parameters
        return {
            "t": internal.OCFParameter.TemperatureAGL,
            "tclc": internal.OCFParameter.TotalCloudCover,
            "dswrf": internal.OCFParameter.DownwardShortWaveRadiationFlux,
            "dlwrf": internal.OCFParameter.DownwardLongWaveRadiationFlux,
            "snod": internal.OCFParameter.SnowDepthWaterEquivalent,
            "rh": internal.OCFParameter.RelativeHumidityAGL,
            "u": internal.OCFParameter.WindUComponentAGL,
            "v": internal.OCFParameter.WindVComponentAGL,
        }



def _parseCMCFilename(
    name: str,
    baseurl: str,
    match_sl: bool = True,
    match_hl: bool = False,
    match_pl: bool = False,
) -> CMCFileInfo | None:
    """Parse a string of HTML into an CMCFileInfo object, if it contains one.

    Args:
        name: The name of the file to parse
        baseurl: The base URL for the GDPS model
        match_sl: Whether to match single-level files
        match_hl: Whether to match Height Above Ground-level files
        match_pl: Whether to match pressure-level files
    """
    # TODO: @Jacob even fixed, these do not match a lot of the files in the store, is that on purpose?  # noqa: E501
    # Define the regex patterns to match the different types of file
    # * Single Level GDPS: `CMC_<MODEL>_<PARAM>_SFC_0_latlon<GRID>_YYYYMMDD_PLLL.grib2`
    # * Sinle Level GEPS: `CMC_geps-raw_CIN_SFC_0_latlon0p5x0p5_2024011800_P000_allmbrs.grib2`
    slRegex = r"CMC_[a-z-]{3,8}_([A-Za-z_\d]+)_SFC_0_latlon[\S]{7}_(\d{10})_P(\d{3})[\S]*.grib"
    # * HeightAboveGround GDPS: `CMC_glb_ISBL_TGL_40_latlon.15x.15_2023080900_P027.grib2`
    # * HeightAboveGround GEPS: `CMC_geps-raw_SPFH_TGL_2_latlon0p5x0p5_2023080900_P027_allmbrs.grib2`  # noqa: E501
    hlRegex = r"CMC_[a-z-]{3,8}_([A-Za-z_\d]+)_TGL_(\d{1,4})_latlon[\S]{7}_(\d{10})_P(\d{3})[\S]*.grib"  # noqa: E501
    # * Pressure Level GDPS: `CMC_glb_TMP_ISBL_500_latlon.15x.15_2023080900_P027.grib2`
    # * Pressure Level GEPS: `CMC_geps-raw_TMP_ISBL_500_latlon0p5x0p5_2023080900_P027_allmbrs.grib2`
    plRegex = r"CMC_[a-z-]{3,8}_([A-Za-z_\d]+)_ISBL_(\d{1,4})_latlon[\S]{7}_(\d{10})_P(\d{3})[\S]*.grib"  # noqa: E501

    itstring = paramstring = ""
    stepstring = "000"
    # Try to match the href to one of the regex patterns
    slmatch = re.search(pattern=slRegex, string=name)
    hlmatch = re.search(pattern=hlRegex, string=name)
    plmatch = re.search(pattern=plRegex, string=name)

    if slmatch and match_sl:
        paramstring, itstring, stepstring = slmatch.groups()
    elif hlmatch and match_hl:
        paramstring, levelstring, itstring, stepstring = hlmatch.groups()
    elif plmatch and match_pl:
        paramstring, levelstring, itstring, stepstring = plmatch.groups()
    else:
        return None

    it = dt.datetime.strptime(itstring, "%Y%m%d%H").replace(tzinfo=dt.UTC)

    return CMCFileInfo(
        it=it,
        filename=name,
        currentURL=f"{baseurl}/{it.strftime('%H')}/{stepstring}/",
        step=int(stepstring),
    )
