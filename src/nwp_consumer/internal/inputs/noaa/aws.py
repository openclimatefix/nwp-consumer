"""Implements a client to fetch NOAA data from AWS."""
import datetime as dt
import pathlib
import typing
import urllib.request

import cfgrib
import structlog
import xarray as xr

from nwp_consumer import internal

from ._consts import GFS_VARIABLES
from ._models import NOAAFileInfo

log = structlog.getLogger()

COORDINATE_ALLOW_LIST: typing.Sequence[str] = ("init_time", "step", "latitude", "longitude")


class Client(internal.FetcherInterface):
    """Implements a client to fetch NOAA data from AWS."""

    baseurl: str  # The base URL for the NOAA model
    model: str  # The model to fetch data for
    parameters: list[str]  # The parameters to fetch

    def __init__(self, model: str, hours: int = 48, param_group: str = "default") -> None:
        """Create a new NOAA Client.

        Exposes a client for NOAA data from AWS that conforms to the FetcherInterface.

        Args:
            model: The model to fetch data for. Valid models is "global".
            param_group: The set of parameters to fetch.
                Valid groups are "default", "full", and "basic".
        """
        self.baseurl = "https://noaa-gfs-bdp-pds.s3.amazonaws.com"

        match (param_group, model):
            case ("default", _):
                self.parameters = [
                    "t2m",
                    "tcc",
                    "mcc",
                    "hcc",
                    "lcc",
                    "dswrf",
                    "dlwrf",
                    "prate",
                    "sdwe",
                    "r",
                    "vis",
                    "u10",
                    "v10",
                    "u100",
                    "v100",
                ]
            case ("basic", "global"):
                self.parameters = ["t2m", "dswrf"]
            case ("full", "global"):
                raise ValueError("full parameter group is not yet implemented for GFS")
            case (_, _):
                raise ValueError(
                    f"unknown parameter group {param_group}."
                    "Valid groups are 'default', 'full', 'basic'",
                )

        self.model = model
        self.hours = hours

    def datasetName(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return f"NOAA_{self.model}".upper()

    def getInitHours(self) -> list[int]:  # noqa: D102
        return [0, 6, 12, 18]

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:  # noqa: D102
        # Ignore inittimes that don't correspond to valid hours
        if it.hour not in self.getInitHours():
            return []

        files: list[internal.FileInfoModel] = []

        # Files are split per timestep
        # And the url includes the time and init time
        # https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20201206/00/atmos/gfs.t00z.pgrb2.1p00.f000
        for step in range(0, self.hours + 1, 3):
            files.append(
                NOAAFileInfo(
                    it=it,
                    filename=f"gfs.t{it.hour:02}z.pgrb2.1p00.f{step:03}",
                    currentURL=f"{self.baseurl}/gfs.{it.strftime('%Y%m%d')}/{it.hour:02}/atmos",
                    step=step,
                ),
            )

        log.debug(
            event="listed files for init time",
            inittime=it.strftime("%Y-%m-%d %H:%M"),
            numfiles=len(files),
        )

        return files

    def mapCachedRaw(self, *, p: pathlib.Path) -> xr.Dataset:  # noqa: D102
        log.debug(event="mapping raw file to xarray dataset", filepath=p.as_posix())

        # Load the raw file as a dataset
        try:
            ds = cfgrib.open_datasets(
                p.as_posix(),
                backend_kwargs={
                    "indexpath": "",
                    "errors": "ignore",
                },
            )
        except Exception as e:
            log.warn(
                event="error converting raw file as dataset",
                error=e,
                filepath=p.as_posix(),
            )
            return xr.Dataset()

        log.debug(event=f"Loaded the file {p.as_posix()}, and now processing it")
        # Process all the parameters into a single file
        ds = [
            d
            for d in ds
            if any(x in d.coords for x in ["surface", "heightAboveGround", "isobaricInhPa"])
        ]

        # Split into surface, heightAboveGround, and isobaricInhPa lists
        surface = [d for d in ds if "surface" in d.coords]
        heightAboveGround = [d for d in ds if "heightAboveGround" in d.coords]
        isobaricInhPa = [d for d in ds if "isobaricInhPa" in d.coords]

        # * Drop any variables we are not intrested in keeping
        for i, d in enumerate(surface):
            unwanted_variables = [v for v in d.data_vars if v not in self.parameters]
            surface[i] = d.drop_vars(unwanted_variables)
        for i, d in enumerate(heightAboveGround):
            unwanted_variables = [v for v in d.data_vars if v not in self.parameters]
            heightAboveGround[i] = d.drop_vars(unwanted_variables)
        for i, d in enumerate(isobaricInhPa):
            unwanted_variables = [v for v in d.data_vars if v not in self.parameters]
            isobaricInhPa[i] = d.drop_vars(unwanted_variables)

        surface_merged = xr.merge(surface, compat="override").drop_vars(
            ["unknown_surface_instant", "valid_time"],
            errors="ignore",
        )
        del surface
        # Drop unknown data variable
        hag_merged = xr.merge(heightAboveGround).drop_vars("valid_time", errors="ignore")
        del heightAboveGround
        iso_merged = xr.merge(isobaricInhPa).drop_vars("valid_time", errors="ignore")
        del isobaricInhPa

        log.debug(event='Merging surface, hag and iso backtogether')

        total_ds = (
            xr.merge([surface_merged, hag_merged, iso_merged])
            .rename({"time": "init_time"})
            .expand_dims("init_time")
            .expand_dims("step")
            .transpose("init_time", "step", ...)
            .sortby("step")
            .chunk({"init_time": 1, "step": 1})
        )
        del surface_merged, hag_merged, iso_merged

        ds = total_ds.drop_dims([c for c in list(total_ds.sizes.keys()) if c not in COORDINATE_ALLOW_LIST])

        log.debug(event='Finished mapping raw file to xarray', filename=p.as_posix())

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

    def parameterConformMap(self) -> dict[str, internal.OCFParameter]:
        """Overrides the corresponding method in the parent class."""
        # See https://www.nco.ncep.noaa.gov/pmb/products/gfs/gfs.t00z.pgrb2.0p25.f003.shtml for a list of NOAA GFS
        return {
            "t2m_instant": internal.OCFParameter.TemperatureAGL,
            "tcc": internal.OCFParameter.HighCloudCover,
            "dswrf_surface_avg": internal.OCFParameter.DownwardShortWaveRadiationFlux,
            "dlwrf_surface_avg": internal.OCFParameter.DownwardLongWaveRadiationFlux,
            "sdwe_surface_instant": internal.OCFParameter.SnowDepthWaterEquivalent,
            "r": internal.OCFParameter.RelativeHumidityAGL,
            "u10_instant": internal.OCFParameter.WindUComponentAGL,
            "v10_instant": internal.OCFParameter.WindVComponentAGL,
            "u100_instant": internal.OCFParameter.WindUComponent100m,
            "v100_instant": internal.OCFParameter.WindVComponent100m,
        }

