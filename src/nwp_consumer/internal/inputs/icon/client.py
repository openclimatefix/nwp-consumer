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
    "clat": "lat", # Icon has a seperate dataset for latitude...
    "clon": "lon", # ... and longitude (for the global model)! Go figure
}

COORDINATE_ALLOW_LIST: typing.Sequence[str] = (
    "time", "step", "latitude", "longitude"
)

class Client(internal.FetcherInterface):
    """Implements a client to fetch ICON data from DWD."""

    baseurl: str
    model: str

    def __init__(self, model: str) -> None:
        """Create a new client."""
        self.baseurl = "https://opendata.dwd.de/weather/nwp"
        self.model = model

        match model:
            case "europe": self.baseurl += "/icon-eu/grib"
            case "global": self.baseurl += "/icon/grib"
            case _: raise ValueError(f"unknown icon model {model}. Valid models are 'eu' and 'global'")

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:  # noqa: D102

        # ICON data is only available for today's date. If data hasn't been uploaded for that init
        # time yet, then yesterday's data will still be present on the server.
        if it.date() != dt.datetime.now(dt.timezone.utc).date():
            log.warn(
                event="ICON data is only available on today's date",
                attempteddate=it.strftime("%Y-%m-%d"),
                currentdate=dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d"),
            )
            return []

        # The ICON model only runs on the hours [00, 06, 12, 18]
        if it.hour not in [0, 6, 12, 18]:
            return []

        files: list[internal.FileInfoModel] = []

        # Files are split per parameter, level, and step, with a webpage per parameter
        for param, _ in PARAMETER_RENAME_MAP.items():

            # Fetch DWD webpage detailing the available files for the parameter
            response = requests.get(f"{self.baseurl}/{it.strftime('%H')}/{param}/", timeout=3)

            if response.status_code != 200:
                if self.model == "eu" and param == "clat":
                    # The latitude and longitude files are not available for the EU model
                    continue
                log.warn(
                    event="error fetching filelisting webpage for parameter",
                    status=response.status_code,
                    url=response.url,
                    param=param,
                    init_time=it.strftime("%Y-%m-%d %H:%M"),
                )
                continue

            # The webpage's HTML <body> contains a list of <a> tags
            # * Each <a> tag has a href, most of which point to a file)
            for line in response.text.splitlines():

                # Check if the line contains a href
                refmatch = re.search(r'href="(.+)">', line)
                if refmatch:
                    # Try to extract file information from the href.

                    # First match the single level files and extract the datetime, step, and parameter.
                    # Their file links are formatted as follows, where L is level and X is step:
                    #   * icon_MODEL_single-level_YYYYDDMMHH_XXX_SOME_PARAM.grib2.bz2
                    slmatch = re.search(r'single-level_(\d{10})_(\d{3})_([A-Za-z_\d]+).grib', line)
                    if slmatch:
                        itstring, stepstring, paramstring = slmatch.groups()
                        # Ignore the file if it is not for today's date or has a step > 48
                        if (itstring != it.strftime("%Y%m%d%H")) or (stepstring > "048"):
                            continue
                        files.append(IconFileInfo(
                            it=dt.datetime.strptime(itstring, "%Y%m%d%H"),
                            filename=refmatch.groups()[0],
                            currentURL=f"{self.baseurl}/{it.strftime('%H')}/{param}",
                        ))

                    # Next match time-invariant files and extract as before.
                    # Time invariant files are formatted as follows:
                    # * icon_MODEL_time-invariant_YYYYDDMMHH_SOME_PARAM.grib2.bz2
                    timatch = re.search(r'time-invariant_(\d{10})_([A-Za-z_\d]+).grib', line)
                    if timatch:
                        itstring, paramstring = timatch.groups()
                        if itstring != it.strftime("%Y%m%d%H"):
                            continue
                        files.append(IconFileInfo(
                            it=dt.datetime.strptime(itstring, "%Y%m%d%H"),
                            filename=refmatch.groups()[0],
                            currentURL=f"{self.baseurl}/{it.strftime('%H')}/{param}",
                        ))

        return files

    def mapTemp(self, *, p: pathlib.Path) -> xr.Dataset:  # noqa: D102
        if p.suffix != '.grib2':
            log.warn(
                event="cannot map non-grib file to dataset",
                filepath=p.as_posix(),
            )
            return xr.Dataset()

        if p.stem.endswith("_CLAT") or p.stem.endswith("_CLON"):
            # Ignore the latitude and longitude files
            return xr.Dataset()

        log.debug(
            event="mapping raw file to xarray dataset",
            filepath=p.as_posix()
        )

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

        # Rename the parameters to the OCF names
        # * Only do so if they exist in the dataset
        for oldParamName, newParamName in PARAMETER_RENAME_MAP.items():
            if oldParamName in ds:
                ds = ds.rename({oldParamName: newParamName})

        # Delete unwanted coordinates
        ds = ds.drop_vars(
            names=[c for c in ds.coords if c not in COORDINATE_ALLOW_LIST],
            errors="ignore"
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
        ds = ds \
            .rename({"time": "init_time"}) \
            .expand_dims("init_time") \
            .expand_dims("step") \
            .to_array(dim="variable", name=f"ICON_{self.model}".upper()) \
            .to_dataset() \
            .transpose("variable", "init_time", "step", ...) \
            .sortby("step") \
            .sortby("variable") \
            .chunk({
                "init_time": 1,
                "step": -1,
                "variable": -1,
            })

        return ds

    def downloadToTemp(self, *, fi: internal.FileInfoModel) -> tuple[internal.FileInfoModel, pathlib.Path]:  # noqa: D102
        log.debug(
            event="requesting download of file",
            file=fi.filename(),
            path=fi.filepath()
        )
        try:
            response = urllib.request.urlopen(fi.filepath())
        except Exception as e:
            log.warn(
                event="error calling url for file",
                url=fi.filepath(),
                filename=fi.filename(),
                error=e
            )
            return fi, pathlib.Path()

        if response.status != 200:
            log.warn(
                event="error downloading file",
                status=response.status,
                url=fi.filepath(),
                filename=fi.filename()
            )
            return fi, pathlib.Path()

        # Extract the bz2 file when downloading
        tfp: pathlib.Path = internal.TMP_DIR / fi.filename()
        with open(tfp, "wb") as f:
            dec = bz2.BZ2Decompressor()
            for chunk in iter(lambda: response.read(16 * 1024), b''):
                f.write(dec.decompress(chunk))
                f.flush()

        log.debug(
            event="fetched all data from file",
            filename=fi.filename(),
            url=fi.filepath(),
            filepath=tfp.as_posix(),
            nbytes=tfp.stat().st_size
        )

        return fi, tfp

