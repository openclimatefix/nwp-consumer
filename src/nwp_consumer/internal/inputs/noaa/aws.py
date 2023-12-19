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
import cfgrib

from nwp_consumer import internal

from ._consts import EU_ML_VARS, EU_SL_VARS, GLOBAL_ML_VARS, GLOBAL_SL_VARS
from ._models import NOAAFileInfo

log = structlog.getLogger()

# See https://d-nb.info/1081305452/34 for a list of ICON parameters
PARAMETER_RENAME_MAP: dict[str, str] = {
    "t2m_instant": internal.OCFShortName.TemperatureAGL.value,
    "tcc": internal.OCFShortName.HighCloudCover.value,
    "dswrf_surface_avg": internal.OCFShortName.DownwardShortWaveRadiationFlux.value,
    "dlwrf_surface_avg": internal.OCFShortName.DownwardLongWaveRadiationFlux.value,
    "sdwe_surface_instant": internal.OCFShortName.SnowDepthWaterEquivalent.value,
    "r": internal.OCFShortName.RelativeHumidityAGL.value,
    "u10_instant": internal.OCFShortName.WindUComponentAGL.value,
    "v10_instant": internal.OCFShortName.WindVComponentAGL.value,
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
        self.baseurl = "https://noaa-gfs-bdp-pds.s3.amazonaws.com/"

        match (param_group, model):
            case ("default", _):
                self.parameters = list(PARAMETER_RENAME_MAP.keys())
                self.conform = True
            case ("basic", "global"):
                self.parameters = ["t2m_instant", "tcc",]
                self.conform = True
            case ("full", "global"):
                self.parameters = GLOBAL_SL_VARS + GLOBAL_ML_VARS
                self.conform = False
            case (_, _):
                raise ValueError(
                    f"unknown parameter group {param_group}."
                    "Valid groups are 'default', 'full', 'basic'",
                )

        self.model = model
        self.hours = hours

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:  # noqa: D102

        if it.hour not in [0, 6, 12, 18]:
            return []

        files: list[internal.FileInfoModel] = []

        # Files are split per timestep
        # And the url includes the time and init time
        # https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20201206/00/atmos/gfs.t00z.pgrb2.0p25.f000

        # Fetch AWS webpage detailing the available files for the parameter
        response = requests.get(f"{self.baseurl}/gfs.{it.strftime('%Y%m%d')}/{it.strftime('%H')}/", timeout=3)

        if response.status_code != 200:
            log.warn(
                event="error fetching filelisting webpage for parameter",
                status=response.status_code,
                url=response.url,
                inittime=it.strftime("%Y-%m-%d %H:%M"),
            )
            return []

        # The webpage's HTML <body> contains a list of <a> tags
        # * Each <a> tag has a href, most of which point to a file)
        for line in response.text.splitlines():
            # Check if the line contains a href, if not, skip it
            refmatch = re.search(pattern=r'href="(.+)">', string=line)
            if refmatch is None:
                continue

            # The href contains the name of a file - parse this into a FileInfo object
            fi: NOAAFileInfo | None = None
            # If not conforming, match all files
            # * Otherwise only match single level and time invariant
            fi = _parseAWSFilename(
                name=refmatch.groups()[0],
                baseurl=self.baseurl,
                match_aux=not self.conform,
            )
            # Ignore the file if it is not for today's date or has a step > 48 (when conforming)
            if fi is None or (fi.step > self.hours and self.conform):
                continue

            # Add the file to the list
            files.append(fi)

            log.debug(
                event="listed files for init time",
                inittime=it.strftime("%Y-%m-%d %H:%M"),
                url=response.url,
                numfiles=len(files),
            )

        return files

    def mapTemp(self, *, p: pathlib.Path) -> xr.Dataset:  # noqa: D102
        log.debug(event="mapping raw file to xarray dataset", filepath=p.as_posix())

        # Load the raw file as a dataset
        try:
            ds = cfgrib.open_datasets(
                p.as_posix(),
            )
        except Exception as e:
            log.warn(
                event="error converting raw file as dataset",
                error=e,
                filepath=p.as_posix(),
            )
            return xr.Dataset()

        # Process all the parameters into a single file
        ds = [
            d for d in ds if any(x in d.coords for x in ["surface", "heightAboveGround", "isobaricInhPa"])
        ]

        # Split into surface, heightAboveGround, and isobaricInhPa lists
        surface = [d for d in ds if "surface" in d.coords]
        heightAboveGround = [d for d in ds if "heightAboveGround" in d.coords]
        isobaricInhPa = [d for d in ds if "isobaricInhPa" in d.coords]

        # Update name of each data variable based off the attribute GRIB_stepType
        for i, d in enumerate(surface):
            for variable in d.data_vars.keys():
                d = d.rename({variable: f"{variable}_surface_{d[f'{variable}'].attrs['GRIB_stepType']}"})
            surface[i] = d
        for i, d in enumerate(heightAboveGround):
            for variable in d.data_vars.keys():
                d = d.rename({variable: f"{variable}_{d[f'{variable}'].attrs['GRIB_stepType']}"})
            heightAboveGround[i] = d

        surface = xr.merge(surface)
        # Drop unknown data variable
        surface = surface.drop_vars("unknown_surface_instant")
        heightAboveGround = xr.merge(heightAboveGround)
        isobaricInhPa = xr.merge(isobaricInhPa)

        ds = xr.merge([surface, heightAboveGround, isobaricInhPa])

        # Only conform the dataset if requested (defaults to True)
        if self.conform:
            # Rename the parameters to the OCF names
            # Drop variables that are not in the OCF list first
            ds = ds.drop_vars(
                names=[v for v in ds.data_vars if v not in PARAMETER_RENAME_MAP.keys()],
                errors="ignore",
            )
            # * Only do so if they exist in the dataset
            for oldParamName, newParamName in PARAMETER_RENAME_MAP.items():
                if oldParamName in ds:
                    ds = ds.rename({oldParamName: newParamName})

            # Delete unwanted coordinates
            ds = ds.drop_vars(
                names=[c for c in ds.coords if c not in COORDINATE_ALLOW_LIST],
                errors="ignore",
            )

        # * Each chunk is a single time step
        # Does not use teh "variable" dimension, as this makes a 86GiB dataset for a single timestamp
        # Keeping variables separate keeps the dataset small enough to fit in memory
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


def _parseAWSFilename(
    name: str,
    baseurl: str,
    match_aux: bool = True,
    match_main: bool = True
) -> NOAAFileInfo | None:
    """Parse a string of HTML into an IconFileInfo object, if it contains one.

    Args:
        name: The name of the file to parse
        baseurl: The base URL for the ICON model
    """
    # Only 2 types of file, they contain all variables in it
    # "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20231206/06/atmos/gfs.t06z.pgrb2.0p25.f002"
    # "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20231206/06/atmos/gfs.t06z.pgrb2b.0p25.f002"
    # Define the regex patterns to match the different types of file; X is step, L is level
    mainRegex = r"gfs.t(\d{2})z.pgrb2.0p25.f(\d{3})"
    # Auxiliary files have b appended to them
    auxRegex = r"gfs.t(\d{2})z.pgrb2b.0p25.f(\d{3})"
    itstring = paramstring = ""
    stepstring = "000"
    # Try to match the href to one of the regex patterns
    mainmatch = re.search(pattern=mainRegex, string=name)
    auxmatch = re.search(pattern=auxRegex, string=name)

    if mainmatch and match_main:
        itstring, stepstring = mainmatch.groups()
    elif auxmatch and match_aux:
        itstring, stepstring = auxmatch.groups()
    else:
        return None

    it = dt.datetime.strptime(itstring, "%Y%m%d%H").replace(tzinfo=dt.timezone.utc)

    return NOAAFileInfo(
        it=it,
        filename=name,
        currentURL=f"{baseurl}/gfs.{it.strftime('%Y%m%d')}/{it.strftime('%H')}/gfs.t{itstring}z.pgrb2.0p25.f{stepstring}",
        step=int(stepstring),
    )
