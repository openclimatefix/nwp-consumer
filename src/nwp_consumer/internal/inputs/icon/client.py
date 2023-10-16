"""Implements a client to fetch ICON data from DWD."""

import datetime as dt
import pathlib
import re
import urllib.request

import requests
import structlog
import xarray as xr

from nwp_consumer import internal

from ._models import IconFileInfo

log = structlog.getLogger()

# See https://d-nb.info/1081305452/34 for a list of ICON parameters
PARAMATER_RENAME_MAP: dict[str, str] = {
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
}

class Client(internal.FetcherInterface):
    """Implements a client to fetch ICON data from DWD."""

    baseurl: str

    def __init__(self) -> None:
        """Create a new client."""
        self.baseurl = "https://opendata.dwd.de/weather/nwp"

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
        for param, _ in PARAMATER_RENAME_MAP.items():
            # Fetch DWD webpage detailing the available files for the parameter
            response = requests.get(f"{self.baseurl}/icon/grib/{it.strftime('%H')}/{param}/", timeout=3)
            # The webpage's HTML <body> contains a list of <a> tags
            # * Each <a> tag has a href, most of which point to a file)
            for line in response.text.splitlines():
                # Check if the line contains a href
                ref = re.search(r'href="(.+)">', line)
                if ref:
                    # Try to extract the datetime, step, and parameter from the href.
                    # The file links are formatted as follows, where L is level and X is step:
                    #   * icon_global_icosahedral_model-level_YYYYDDMMHH_LLL_XXX_PARAM.grib2.bz2
                    #   * icon_global_icosahedral_single-level_YYYYDDMMHH_XXX_ANOTHER_PARAM.grib2.bz2
                    # First match the single level files
                    slmatch = re.search(r'single-level_(\d{10})_(\d{3})_([A-Za-z_\d]+).grib', line)
                    if slmatch:
                        itstring, stepstring, paramstring = slmatch.groups()
                        # Ignore the file if it is not for today's date or has a step > 48
                        if (itstring != it.strftime("%Y%m%d%H")) or (stepstring > "048"):
                            continue
                        files.append(IconFileInfo(
                            it=dt.datetime.strptime(itstring, "%Y%m%d%H"),
                            filename=ref.groups()[0],
                            currentURL=f"{self.baseurl}/icon/grib/{it.strftime('%H')}/{param}",
                        ))

        return files

    def mapTemp(self, *, p: pathlib.Path) -> xr.Dataset:  # noqa: D102
        if p.suffixes != ['.grib2', '.bz2']:
            log.warn(
                event="cannot map non- zipped-grib file to dataset",
                filepath=p.as_posix(),
            )
            return xr.Dataset()

        log.debug(
            event="mapping raw file to xarray dataset",
            filepath=p.as_posix()
        )

        # Load the raw file as a dataset
        try:
            parameterStepDataset = xr.open_dataset(
                p.as_posix(),
                engine="cfgrib",
                chunks={
                    "time": 1,
                },
            )
        except Exception as e:
            log.warn(
                event="error loading raw file as dataset",
                error=e,
                filepath=p.as_posix(),
            )
            return xr.Dataset()

        return xr.Dataset()

    def downloadToTemp(self, *, fi: internal.FileInfoModel) -> tuple[internal.FileInfoModel, pathlib.Path]:  # noqa: D102
        log.debug(
            event="requesting download of file",
file=fi.filename(),
            path=fi.filepath()
        )
        try:
            response = urllib.urlopen(url=fi.filepath())
        except Exception as e:
            log.warn(
                event="error calling url for file",
                url=fi.filepath(),
                filename=fi.filename(),
                error=e
            )
            return fi, pathlib.Path()

        tfp: pathlib.Path = internal.TMP_DIR / fi.filename()
        with open(tfp, "wb") as f:
            for chunk in iter(lambda: response.read(16 * 1024), b''):
                f.write(chunk)
                f.flush()

        log.debug(
            event="fetched all data from file",
            filename=fi.filename(),
            url=fi.filepath(),
            filepath=tfp.as_posix(),
            nbytes=tfp.stat().st_size
        )

        return fi, tfp

