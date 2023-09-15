"""Implements a client to fetch data from ECMWF."""
import datetime as dt
import os
import pathlib
import tempfile
import time
import typing
from contextlib import redirect_stdout

import cfgrib
import ecmwfapi.api
import structlog
import xarray as xr
from ecmwfapi import ECMWFService

from nwp_consumer import internal

from ._models import ECMWFMarsFileInfo

log = structlog.getLogger()

PARAMETER_RENAME_MAP: dict[str, str] = {
    "tas": internal.OCFShortName.TemperatureAGL.value,
    "uas": internal.OCFShortName.WindUComponentAGL.value,
    "vas": internal.OCFShortName.WindVComponentAGL.value,
    "dsrp": internal.OCFShortName.DirectSolarRadiation.value,
    "uvb": internal.OCFShortName.DownwardUVRadiationAtSurface.value,
    "hcc": internal.OCFShortName.HighCloudCover.value,
    "mcc": internal.OCFShortName.MediumCloudCover.value,
    "lcc": internal.OCFShortName.LowCloudCover.value,
    "ssrd": internal.OCFShortName.DownwardShortWaveRadiationFlux.value,
    "strd": internal.OCFShortName.DownwardLongWaveRadiationFlux.value,
    "tprate": internal.OCFShortName.RainPrecipitationRate.value,
    "sd": internal.OCFShortName.SnowDepthWaterEquivalent.value,
    "u100": internal.OCFShortName.WindUComponent100m.value,
    "v100": internal.OCFShortName.WindVComponent100m.value,
    "u200": internal.OCFShortName.WindUComponent200m.value,
    "v200": internal.OCFShortName.WindVComponent200m.value,
}

# Mapping from ECMWF eccode to ECMWF short name
# * https://codes.ecmwf.int/grib/param-db/?filter=All
PARAMETER_ECMWFCODE_MAP: dict[str, str] = {
    "167.128": "tas",  # 2 metre temperature
    "165.128": "uas",  # 10 metre U-component of wind
    "166.128": "vas",  # 10 metre V-component of wind
    "47.128": "dsrp",  # Direct solar radiation
    "57.128": "uvb",  # Downward uv radiation at surface
    "188.128": "hcc",  # High cloud cover
    "187.128": "mcc",  # Medium cloud cover
    "186.128": "lcc",  # Low cloud cover
    "164.128": "clt",  # Cloud area fraction
    "169.128": "ssrd",  # Surface shortwave radiation downward
    "175.128": "strd",  # Surface longwave radiation downward
    "260048": "tprate", # Total precipitation rate
    "141.128": "sd",    # Snow depth, m
    "246.228": "u100",  # 100 metre U component of wind
    "247.228": "v100",  # 100 metre V component of wind
    "239.228": "u200",  # 200 metre U component of wind
    "240.228": "v200",  # 200 metre V component of wind
}

AREA_MAP: dict[str, str] = {
    "uk": "60/-12/48/3",
    "eu": "E",
    "global": "G",
}

COORDINATE_ALLOW_LIST: typing.Sequence[str] = (
    "time", "step", "latitude", "longitude"
)

def marsLogger(msg: str) -> None:
    """Logging function to pass to the ECMWFService."""
    debugSubstrings: list[str] = ["Requesting", "Transfering", "efficiency", "Done"]
    errorSubstrings: list[str] = ["ERROR", "FATAL"]
    if any(map(msg.__contains__, debugSubstrings)):
        log.debug(event=msg, caller="mars")
    if any(map(msg.__contains__, errorSubstrings)):
        log.warning(event=msg, caller="mars")


class MARSClient(internal.FetcherInterface):
    """Implements a client to fetch data from ECMWF's MARS API."""

    server: ecmwfapi.api.ECMWFService
    area: str

    def __init__(self, area: str) -> None:
        """Create a new ECMWFMarsClient."""
        self.server = ECMWFService(
            service="mars",
            log=marsLogger
        )

        if area not in AREA_MAP:
            raise KeyError(f"area must be one of {list(AREA_MAP.keys())}")

        self.area = area

    def listRawFilesForInitTime(self, *, it: dt.datetime) \
            -> list[internal.FileInfoModel]:  # noqa: D102
        # For the model we are pulling from, there are only files for 00:00 and 12:00
        # * Hence, only check requests for these times

        if it.hour not in [0, 12]:
            return []

        with tempfile.NamedTemporaryFile(suffix=".txt", mode="w") as tf:
            try:
                self.server.execute(
                    req=f"""
                        list,
                            class    = od,
                            date     = {it.strftime("%Y%m%d")},
                            expver   = 1,
                            levtype  = sfc,
                            param    = {'/'.join(list(PARAMETER_ECMWFCODE_MAP.keys()))},
                            step     = 0/to/48/by/1,
                            stream   = oper,
                            time     = {it.strftime("%H")},
                            type     = fc,
                            area     = {AREA_MAP[self.area]},
                            grid     = 0.05/0.05,
                            target   = "{tf.name}"
                    """,
                    target=tf.name
                )
            except ecmwfapi.api.APIException as e:
                log.warn("error listing ECMWF MARS inittime data", error=e)
                return []

            if os.stat(tf.name).st_size < 100 and "0 bytes" in tf.read():
                return []

        return [ECMWFMarsFileInfo(inittime=it, area=self.area)]

    def downloadToTemp(self, *, fi: internal.FileInfoModel) \
            -> tuple[internal.FileInfoModel, pathlib.Path]:  # noqa: D102
        tfp: pathlib.Path = internal.TMP_DIR / fi.filename()
        try:
            self.server.execute(
                req=f"""
                    retrieve,
                        class    = od,
                        date     = {fi.it().strftime("%Y%m%d")},
                        expver   = 1,
                        levtype  = sfc,
                        param    = {'/'.join(list(PARAMETER_ECMWFCODE_MAP.keys()))},
                        step     = 0/to/48/by/1,
                        stream   = oper,
                        time     = {fi.it().strftime("%H")},
                        type     = fc,
                        area     = {AREA_MAP[self.area]},
                        grid     = 0.05/0.05,
                        target   = "{tfp.as_posix()}"
                """,
                target=tfp.as_posix()
            )
        except ecmwfapi.api.APIException as e:
            log.warn("error fetching ECMWF MARS data", error=e)
            return fi, pathlib.Path()

        if tfp.exists() is False:
            log.warn("error fetching ECMWF MARS data", error=e)
            return fi, pathlib.Path()

        log.debug(
            event="fetched all data from MARS",
            filename=fi.filename(),
            filepath=tfp.as_posix(),
            nbytes=tfp.stat().st_size
        )

        return fi, tfp

    def mapTemp(self, *, p: pathlib.Path) -> xr.Dataset:  # noqa: D102
        if p.suffix != '.grib':
            log.warn(
                event="cannot map non-grib file to dataset",
                filepath=p.as_posix()
            )
            return xr.Dataset()

        log.debug(
            event="mapping raw file to xarray dataset",
            filepath=p.as_posix()
        )

        # Load the wholesale file as a list of datasets
        # * cfgrib loads multiple hypercubes for a single multi-parameter grib file
        # * Can also set backend_kwargs={"indexpath": ""}, to avoid the index file
        try:
            datasets: list[xr.Dataset] = cfgrib.open_datasets(
                path=p.as_posix(),
                chunks={
                    "time": 1,
                    "step": -1,
                    "variable": -1,
                    "longitude": "auto",
                    "latitude": "auto"
                },
                backend_kwargs={"indexpath": ""}
            )
        except Exception as e:
            log.warn(
                event="error converting raw file to dataset",
                filepath=p.as_posix(),
                error=e
            )
            return xr.Dataset()

        for i, ds in enumerate(datasets):

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

            # Put the modified dataset back in the list
            datasets[i] = ds

        # Merge the datasets back into one
        wholesaleDataset = xr.merge(
            objects=datasets,
            compat='override',
            combine_attrs='drop_conflicts'
        )

        # Create a chunked Dask Dataset from the input multi-variate Dataset.
        # *  Converts the input multivariate DataSet (with different DataArrays for
        #     each NWP variable) to a single DataArray with a `variable` dimension.
        # * This allows each Zarr chunk to hold multiple variables (useful for loading
        #     many/all variables at once from disk).
        # * The chunking is done in such a way that each chunk is a single time step
        #     for a single variable.
        # * Transpose the Dataset so that the dimensions are correctly ordered
        wholesaleDataset = wholesaleDataset \
            .rename({"time": "init_time"}) \
            .expand_dims("init_time") \
            .to_array(dim="variable", name="UKV") \
            .to_dataset() \
            .transpose("variable", "init_time", "step", "latitude", "longitude") \
            .sortby("step") \
            .sortby("variable") \
            .chunk({
                "init_time": 1,
                "step": -1,
                "variable": -1,
                "latitude": len(wholesaleDataset.latitude) // 2,
                "longitude": len(wholesaleDataset.longitude) // 2,
            })

        del datasets

        return wholesaleDataset
