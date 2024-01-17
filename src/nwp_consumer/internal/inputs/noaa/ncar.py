"""Implements a client to fetch NOAA data from NCAR."""
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

from ._consts import GFS_VARIABLES
from ._models import NOAAFileInfo

log = structlog.getLogger()

# See https://www.nco.ncep.noaa.gov/pmb/products/gfs/gfs.t00z.pgrb2.0p25.f003.shtml for a list of NOAA parameters
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
    """Implements a client to fetch NOAA data from NCAR."""

    baseurl: str  # The base URL for the NOAA model
    model: str  # The model to fetch data for
    parameters: list[str]  # The parameters to fetch
    conform: bool  # Whether to rename parameters to OCF names and clear unwanted coordinates

    def __init__(self, model: str, hours: int = 48, param_group: str = "default") -> None:
        """Create a new NOAA Client.

        Exposes a client for NOAA data from NCAR that conforms to the FetcherInterface.

        Args:
            model: The model to fetch data for. Valid models are "global".
            param_group: The set of parameters to fetch.
                Valid groups are "default", "full", and "basic".
        """
        self.baseurl = "https://data.rda.ucar.edu/ds084.1"

        match (param_group, model):
            case ("default", _):
                self.parameters = list(PARAMETER_RENAME_MAP.keys())
                self.conform = True
            case ("basic", "global"):
                self.parameters = ["t2m_instant", "tcc", ]
                self.conform = True
            case ("full", "global"):
                self.parameters = GFS_VARIABLES
                self.conform = False
            case (_, _):
                raise ValueError(
                    f"unknown parameter group {param_group}."
                    "Valid groups are 'default', 'full', 'basic'",
                )

        self.model = model
        self.hours = hours

    def getInitHours(self) -> list[int]:  # noqa: D102
        return [0, 6, 12, 18]

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:  # noqa: D102

        # Ignore inittimes that don't correspond to valid hours
        if it.hour not in self.getInitHours():
            return []

        files: list[internal.FileInfoModel] = []

        # Fetch NCAR webpage detailing the available files for the parameter
        response = requests.get(
            f"{self.baseurl}/{it.strftime('%Y')}/{it.strftime('%Y%m%d')}/",
            timeout=3
        )

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
            # The baseurl has to have the time and init time added to it for GFS
            fi = _parseNCARFilename(
                name=refmatch.groups()[0],
                baseurl=f"{self.baseurl}/{it.strftime('%Y')}/{it.strftime('%Y%m%d')}",
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
        if p.suffix != ".grib2":
            log.warn(
                event="cannot map non-grib file to dataset",
                filepath=p.as_posix(),
            )
            return xr.Dataset()

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
        surface = surface.drop_vars("unknown_surface_instant", errors="ignore")
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
        if self.conform:
            ds = (
                ds.rename({"time": "init_time"})
                .expand_dims("init_time")
                .expand_dims("step")
                .to_array(dim="variable", name=f"NOAA_{self.model}".upper())
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
        else:
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
            f.write(response.read())

        log.debug(
            event="fetched all data from file",
            filename=fi.filename(),
            url=fi.filepath(),
            filepath=tfp.as_posix(),
            nbytes=tfp.stat().st_size,
        )

        return fi, tfp


def _parseNCARFilename(
        name: str,
        baseurl: str,
        match_main: bool = True,
) -> NOAAFileInfo | None:
    """Parse a string of HTML into an NOAAFileInfo object, if it contains one.

    Args:
        name: The name of the file to parse
        baseurl: The base URL for the NOAA NCAR model
    """
    # Define the regex patterns to match the different types of file; X is step, L is level
    mainRegex = r"gfs.0p25.(\d{10}).f(\d{3}).grib2"
    # Auxiliary files have b appended to them
    itstring = paramstring = ""
    stepstring = "000"
    # Try to match the href to one of the regex patterns
    mainmatch = re.search(pattern=mainRegex, string=name)

    if mainmatch and match_main:
        itstring, stepstring = mainmatch.groups()
    else:
        return None

    it = dt.datetime.strptime(itstring, "%Y%m%d%H").replace(tzinfo=dt.timezone.utc)

    return NOAAFileInfo(
        it=it,
        filename=name,
        currentURL=f"{baseurl}/gfs.0p25.{it.strftime('%Y%m%d%H')}.f{stepstring}.grib2",
        step=int(stepstring),
    )
