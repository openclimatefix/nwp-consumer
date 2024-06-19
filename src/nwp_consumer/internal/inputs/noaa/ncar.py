"""Implements a client to fetch NOAA data from NCAR."""
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

COORDINATE_ALLOW_LIST: typing.Sequence[str] = ("time", "step", "latitude", "longitude")


class Client(internal.FetcherInterface):
    """Implements a client to fetch NOAA data from NCAR."""

    baseurl: str  # The base URL for the NOAA model
    model: str  # The model to fetch data for
    parameters: list[str]  # The parameters to fetch

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
                self.parameters = ["t2m_instant", "tcc", "dswrf_surface_avg", "dlwrf_surface_avg",
                                   "sdwe_surface_instant", "r", "u10_instant", "v10_instant"]
            case ("basic", "global"):
                self.parameters = ["t2m_instant", "dswrf_surface_avg"]
            case ("full", "global"):
                self.parameters = GFS_VARIABLES
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

        # The GFS dataset goes from 2015-01-15 to present
        # * https://rda.ucar.edu/datasets/ds084.1/
        if it < dt.datetime(2015, 1, 15, tzinfo=dt.UTC):
            return []

        files: list[internal.FileInfoModel] = []

        # The GFS dataset has data in hour jumps of 3 up to 240
        for step in range(0, self.hours + 1, 3):
            filename = f"gfs.0p25.{it.strftime('%Y%m%d%H')}.f{step:03}.grib2"
            files.append(
                NOAAFileInfo(
                    it=it,
                    filename=filename,
                    currentURL=f"{self.baseurl}/{it.strftime('%Y')}/{it.strftime('%Y%m%d')}",
                    step=step,
                ),
            )

        return files

    def mapCachedRaw(self, *, p: pathlib.Path) -> xr.Dataset:  # noqa: D102
        if p.suffix != ".grib2":
            log.warn(
                event="cannot map non-grib file to dataset",
                filepath=p.as_posix(),
            )
            return xr.Dataset()

        log.debug(event="mapping raw file to xarray dataset", filepath=p.as_posix())

        # Load the raw file as a list of datasets
        try:
            ds: list[xr.Dataset] = cfgrib.open_datasets(
                p.as_posix(),
            )
        except Exception as e:
            log.error(
                event="error converting raw file as dataset",
                error=e,
                filepath=p.as_posix(),
            )
            return xr.Dataset()

        # Process all the parameters into a single file
        ds = [
            d for d in ds
            if any(x in d.coords for x in ["surface", "heightAboveGround", "isobaricInhPa"])
        ]

        # Split into surface, heightAboveGround, and isobaricInhPa lists
        surface: list[xr.Dataset] = [d for d in ds if "surface" in d.coords]
        heightAboveGround: list[xr.Dataset] = [d for d in ds if "heightAboveGround" in d.coords]
        isobaricInhPa: list[xr.Dataset] = [d for d in ds if "isobaricInhPa" in d.coords]
        del ds

        # Update name of each data variable based off the attribute GRIB_stepType
        for i, d in enumerate(surface):
            for variable in d.data_vars:
                d = d.rename({variable: f"{variable}_surface_{d[f'{variable}'].attrs['GRIB_stepType']}"})
            surface[i] = d
        for i, d in enumerate(heightAboveGround):
            for variable in d.data_vars:
                d = d.rename({variable: f"{variable}_{d[f'{variable}'].attrs['GRIB_stepType']}"})
            heightAboveGround[i] = d

        surface_merged: xr.Dataset = xr.merge(surface).drop_vars(
            ["unknown_surface_instant", "valid_time"], errors="ignore",
        )
        del surface
        heightAboveGround_merged: xr.Dataset = xr.merge(heightAboveGround).drop_vars(
            ["valid_time"], errors="ignore",
        )
        del heightAboveGround
        isobaricInhPa_merged: xr.Dataset = xr.merge(isobaricInhPa).drop_vars(
            ["valid_time"], errors="ignore",
        )
        del isobaricInhPa

        total_ds = xr.merge([surface_merged, heightAboveGround_merged, isobaricInhPa_merged])
        del surface_merged, heightAboveGround_merged, isobaricInhPa_merged

        # Map the data to the internal dataset representation
        # * Transpose the Dataset so that the dimensions are correctly ordered
        # * Rechunk the data to a more optimal size
        total_ds = (
            total_ds.rename({"time": "init_time"})
            .expand_dims("init_time")
            .expand_dims("step")
            .transpose("init_time", "step", ...)
            .sortby("step")
            .chunk({"init_time": 1, "step": 1})
        )

        return total_ds

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
        # See https://www.nco.ncep.noaa.gov/pmb/products/gfs/gfs.t00z.pgrb2.0p25.f003.shtml
        # for a list of NOAA parameters
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
