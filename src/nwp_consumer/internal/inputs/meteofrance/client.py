"""Implements a client to fetch Arpege data from MeteoFrance AWS."""
import datetime as dt
import pathlib
import re
import typing

import cfgrib
import s3fs
import structlog
import xarray as xr

from nwp_consumer import internal

from ._consts import ARPEGE_GLOBAL_PARAMETER_SETS, ARPEGE_GLOBAL_VARIABLES
from ._models import ArpegeFileInfo

log = structlog.getLogger()

# See https://mf-models-on-aws.org/en/doc/datasets/v1/ for a list of Arpege parameters
PARAMETER_RENAME_MAP: dict[str, str] = {
    "t2m": internal.OCFShortName.TemperatureAGL.value,
    "hcc": internal.OCFShortName.HighCloudCover.value,
    "mcc": internal.OCFShortName.MediumCloudCover.value,
    "lcc": internal.OCFShortName.LowCloudCover.value,
    "ssrd": internal.OCFShortName.DownwardShortWaveRadiationFlux.value,
    "d2m": internal.OCFShortName.RelativeHumidityAGL.value,
    "u10": internal.OCFShortName.WindUComponentAGL.value,
    "v10": internal.OCFShortName.WindVComponentAGL.value,
}

COORDINATE_ALLOW_LIST: typing.Sequence[str] = ("time", "step", "latitude", "longitude")


class Client(internal.FetcherInterface):
    """Implements a client to fetch Arpege data from AWS."""

    baseurl: str  # The base URL for the Argpege model
    model: str  # The model to fetch data for
    parameters: list[str]  # The parameters to fetch
    conform: bool  # Whether to rename parameters to OCF names and clear unwanted coordinates

    def __init__(self, model: str, hours: int = 48, param_group: str = "default") -> None:
        """Create a new Arpege Client.

        Exposes a client for Arpege data from AWS MeteoFrance that conforms to the FetcherInterface.

        Args:
            model: The model to fetch data for. Valid models are "europe" and "global".
            param_group: The set of parameters to fetch.
                Valid groups are "default", "full", and "basic".
        """
        self.baseurl = "s3://mf-nwp-models/"
        self.fs = s3fs.S3FileSystem(anon=True)

        match model:
            case "europe":
                self.baseurl += "arpege-europe/v1/"
            case "global":
                self.baseurl += "arpege-world/v1/"
            case _:
                raise ValueError(
                    f"unknown arpege model {model}. Valid models are 'europe' and 'global'",
                )

        match (param_group, model):
            case ("default", _):
                self.parameters = list(PARAMETER_RENAME_MAP.keys())
                self.conform = True
            case ("basic", "europe"):
                self.parameters = ARPEGE_GLOBAL_VARIABLES[:2]
                self.conform = True
            case ("basic", "global"):
                self.parameters = ARPEGE_GLOBAL_VARIABLES[:2]
                self.conform = True
            case ("full", "europe"):
                self.parameters = ARPEGE_GLOBAL_VARIABLES
                self.conform = False
            case ("full", "global"):
                self.parameters = ARPEGE_GLOBAL_VARIABLES
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

        # Files are split per set of parameters, and set of steps
        # The list of files for the parameter
        parameterFiles: list[internal.FileInfoModel] = []

        # Parameter sets
        for parameter_set in ARPEGE_GLOBAL_PARAMETER_SETS:
            # Fetch Arpege webpage detailing the available files for the parameter
            files = self.fs.ls(
                f"{self.baseurl}{it.strftime('%Y-%m-%d')}/{it.strftime('%H')}/{parameter_set}/"
            )

            # The webpage's HTML <body> contains a list of <a> tags
            # * Each <a> tag has a href, most of which point to a file)
            for f in files:
                if ".inv" in f:  # Ignore the .inv files
                    continue
                # The href contains the name of a file - parse this into a FileInfo object
                fi: ArpegeFileInfo | None = None
                # If not conforming, match all files
                # * Otherwise only match single level
                fi = _parseArpegeFilename(
                    name=f.split("/")[-1],
                    baseurl=f"{self.baseurl}{it.strftime('%Y-%m-%d')}/{it.strftime('%H')}/{parameter_set}/",
                    match_hl=not self.conform,
                    match_pl=not self.conform,
                )
                # Ignore the file if it is not for today's date or has a step > 48 (when conforming)
                if fi is None or fi.it() != it or (fi.step > self.hours and self.conform):
                    continue

                # Add the file to the list
                parameterFiles.append(fi)

                log.debug(
                    event="listed files for parameter",
                    param=parameter_set,
                    inittime=it.strftime("%Y-%m-%d %H:%M"),
                    url=f,
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
        # Check if datasets is more than a single dataset or not
        # * If it is, merge the datasets into a single dataset
        if len(ds) > 1:
            if "_IP" in str(p):  # Pressure levels
                for i, d in enumerate(ds):
                    if "isobaricInhPa" in d.coords and "isobaricInhPa" not in d.dims:
                        d = d.expand_dims("isobaricInhPa")
                        ds[i] = d
                ds = xr.merge([d for d in ds if "isobaricInhPa" in d.coords], compat="override")
            elif "_SP" in str(p):  # Single levels
                for i, d in enumerate(ds):
                    if "surface" in d.coords:
                        d = d.rename({"surface": "heightAboveGround"})
                    # Make heightAboveGround a coordinate
                    if "heightAboveGround" in d.coords:
                        d = d.expand_dims("heightAboveGround")
                        ds[i] = d
                # Merge all the datasets that have heightAboveGround
                ds = xr.merge([d for d in ds if "heightAboveGround" in d.coords], compat="override")
            elif "_HP" in str(p):  # Height levels
                for i, d in enumerate(ds):
                    if "heightAboveGround" in d.coords and "heightAboveGround" not in d.dims:
                        d = d.expand_dims("heightAboveGround")
                        ds[i] = d
                ds = xr.merge([d for d in ds if "heightAboveGround" in d.coords], compat="override")
        else:
            ds = ds[0]
        ds = ds.drop_vars("unknown", errors="ignore")

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
        # Create chunked Dask dataset with a single "variable" dimension
        # * Each chunk is a single time step
        if self.conform:
            ds = (
                ds.rename({"time": "init_time"})
                .expand_dims("init_time")
                .to_array(dim="variable", name=f"MeteoFrance_{self.model}".upper())
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
        # Extract the bz2 file when downloading
        tfp: pathlib.Path = internal.TMP_DIR / fi.filename()

        self.fs.get(str(fi.filepath()), str(tfp))

        log.debug(
            event="fetched all data from file",
            filename=fi.filename(),
            url=fi.filepath(),
            filepath=tfp.as_posix(),
            nbytes=tfp.stat().st_size,
        )

        return fi, tfp


def _parseArpegeFilename(
    name: str,
    baseurl: str,
    match_sl: bool = True,
    match_hl: bool = True,
    match_pl: bool = False,
) -> ArpegeFileInfo | None:
    """Parse a string of HTML into an ArpegeFileInfo object, if it contains one.

    Args:
        name: The name of the file to parse
        baseurl: The base URL for the Arpege model
        match_sl: Whether to match single-level files
        match_hl: Whether to match height-level files
        match_pl: Whether to match pressure-level files
    """
    # Defined from the href of the file, its harder to split
    # Define the regex patterns to match the different types of file; X is step, L is level
    # * Single Level: `MODEL_single-level_YYYYDDMMHH_XXX_SOME_PARAM.grib2.bz2`
    slRegex = r"s3://mf-nwp-models/arpege-([A-Za-z_\d]+)/v1/(\d{4})-(\d{2})-(\d{2})/(\d{2})/SP(\d{1})/(\d{2})H(\d{2})H.grib2"
    # * Height Level: `MODEL_time-invariant_YYYYDDMMHH_SOME_PARAM.grib2.bz2`
    hlRegex = r"s3://mf-nwp-models/arpege-([A-Za-z_\d]+)/v1/(\d{4})-(\d{2})-(\d{2})/(\d{2})/HP(\d{1})/(\d{2})H(\d{2})H.grib2"
    # * Pressure Level: `MODEL_model-level_YYYYDDMMHH_XXX_LLL_SOME_PARAM.grib2.bz2`
    plRegex = r"s3://mf-nwp-models/arpege-([A-Za-z_\d]+)/v1/(\d{4})-(\d{2})-(\d{2})/(\d{2})/IP(\d{1})/(\d{2})H(\d{2})H.grib2"

    itstring_year = itstring_month = itstring_day = itstring_hour = paramstring = ""
    stepstring_start = stepstring_end = "00"
    # Try to match the href to one of the regex patterns
    slmatch = re.search(pattern=slRegex, string=baseurl + name)
    hlmatch = re.search(pattern=hlRegex, string=baseurl + name)
    plmatch = re.search(pattern=plRegex, string=baseurl + name)

    if slmatch and match_sl:
        (
            _,
            itstring_year,
            itstring_month,
            itstring_day,
            itstring_hour,
            paramstring,
            stepstring_start,
            stepstring_end,
        ) = slmatch.groups()
    elif hlmatch and match_hl:
        (
            _,
            itstring_year,
            itstring_month,
            itstring_day,
            itstring_hour,
            paramstring,
            stepstring_start,
            stepstring_end,
        ) = hlmatch.groups()
    elif plmatch and match_pl:
        (
            _,
            itstring_year,
            itstring_month,
            itstring_day,
            itstring_hour,
            paramstring,
            stepstring_start,
            stepstring_end,
        ) = plmatch.groups()
    else:
        return None

    it = dt.datetime.strptime(
        itstring_year + itstring_month + itstring_day + itstring_hour, "%Y%m%d%H"
    ).replace(tzinfo=dt.UTC)

    # TODO Construct the public URL from S3 path?

    return ArpegeFileInfo(
        it=it,
        filename=name,
        currentURL=f"{baseurl}",
        step=int(stepstring_start),
    )
