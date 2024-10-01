"""Input covering an OCF-specific use case of pulling ECMWF data from an s3 bucket."""

import datetime as dt
import pathlib
import typing

import cfgrib
import s3fs
import structlog
import xarray as xr

from nwp_consumer import internal

from ._models import ECMWFLiveFileInfo
from ... import OCFParameter

log = structlog.getLogger()

COORDINATE_ALLOW_LIST: typing.Sequence[str] = ("time", "step", "latitude", "longitude")


class S3Client(internal.FetcherInterface):
    """Implements a client to fetch ECMWF data from S3."""

    area: str
    desired_params: list[str]
    bucket: pathlib.Path

    __fs: s3fs.S3FileSystem

    bucketPath: str = "ecmwf"

    def __init__(
            self,
            bucket: str,
            region: str,
            area: str = "uk",
            key: str | None = "",
            secret: str | None = "",
            endpointURL: str = "",
    ) -> None:
        """Creates a new ECMWF S3 client.

        Exposes a client for fetching ECMWF data from an S3 bucket conforming to the
        FetcherInterface. ECMWF S3 data is order-based, so parameters and steps cannot be
        requested by this client.

        Args:
            bucket: The name of the S3 bucket to fetch data from.
            region: The AWS region to connect to.
            key: The AWS access key to use for authentication.
            secret: The AWS secret key to use for authentication.
            area: The area for which to fetch data.
            endpointURL: The endpoint URL to use for the S3 connection.
        """
        if (key, secret) == ("", ""):
            log.info(
                event="attempting AWS connection using default credentials",
            )
            key, secret = None, None

        self.__fs: s3fs.S3FileSystem = s3fs.S3FileSystem(
            key=key,
            secret=secret,
            client_kwargs={
                "region_name": region,
                "endpoint_url": None if endpointURL == "" else endpointURL,
            },
        )
        self.area = area
        self.bucket = pathlib.Path(bucket)

    def datasetName(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return f"ECMWF_{self.area}".upper()

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:
        """Overrides the corresponding method in the parent class."""
        allFiles: list[str] = self.__fs.ls((self.bucket / self.bucketPath).as_posix())
        # List items are of the form "bucket/folder/filename, so extract just the filename
        initTimeFiles: list[internal.FileInfoModel] = [
            ECMWFLiveFileInfo(fname=pathlib.Path(file).name)
            for file in allFiles
            if it.strftime("A1D%m%d%H") in file
        ]
        return initTimeFiles

    def downloadToCache(
            self,
            *,
            fi: internal.FileInfoModel,
    ) -> pathlib.Path:
        """Overrides the corresponding method in the parent class."""
        cfp: pathlib.Path = internal.rawCachePath(it=fi.it(), filename=fi.filename())
        with open(cfp, "wb") as f, self.__fs.open(
                (self.bucket / fi.filepath()).as_posix(),
                "rb",
        ) as s:
            for chunk in iter(lambda: s.read(12 * 1024), b""):
                f.write(chunk)
                f.flush()

        if not cfp.exists():
            log.warn(event="Failed to download file", filepath=fi.filepath())
            return pathlib.Path()

        # Check the sizes are the same
        s3size = self.__fs.info((self.bucket / fi.filepath()).as_posix())["size"]
        if cfp.stat().st_size != s3size:
            log.warn(
                event="Downloaded file size does not match expected size",
                expected=s3size,
                actual=cfp.stat().st_size,
            )
            return pathlib.Path()

        return cfp

    def mapCachedRaw(self, *, p: pathlib.Path) -> xr.Dataset:
        """Overrides the corresponding method in the parent class."""
        all_dss: list[xr.Dataset] = cfgrib.open_datasets(p.as_posix())
        area_dss: list[xr.Dataset] = _filterDatasetsByArea(all_dss, self.area)
        ds: xr.Dataset = xr.merge(area_dss, combine_attrs="drop_conflicts")
        del area_dss, all_dss

        ds = ds.drop_vars(
            names=[v for v in ds.coords if v not in COORDINATE_ALLOW_LIST],
            errors="ignore",
        )

        # Map the data to the internal dataset representation
        # * Transpose the Dataset so that the dimensions are correctly ordered
        # * Rechunk the data to a more optimal size
        ds = (
            ds.rename({"time": "init_time"})
            .expand_dims("init_time")
            .expand_dims("step")
            .transpose("init_time", "step", "latitude", "longitude")
            .sortby("step")
            .chunk(
                {
                    "init_time": 1,
                    "step": -1,
                    "latitude": len(ds.latitude) // 2,
                    "longitude": len(ds.longitude) // 2,
                },
            )
        )

        return ds

    def getInitHours(self) -> list[int]:
        """Overrides the corresponding method in the parent class."""
        return [0, 6, 12, 18]

    def parameterConformMap(self) -> dict[str, OCFParameter]:
        """Overrides the corresponding method in the parent class."""
        return {
            "dsrp": internal.OCFParameter.DirectSolarRadiation,
            "uvb": internal.OCFParameter.DownwardUVRadiationAtSurface,
            "sd": internal.OCFParameter.SnowDepthWaterEquivalent,
            "tcc": internal.OCFParameter.TotalCloudCover,
            "clt": internal.OCFParameter.TotalCloudCover,
            "u10": internal.OCFParameter.WindUComponentAGL,
            "v10": internal.OCFParameter.WindVComponentAGL,
            "t2m": internal.OCFParameter.TemperatureAGL,
            "ssrd": internal.OCFParameter.DownwardShortWaveRadiationFlux,
            "strd": internal.OCFParameter.DownwardLongWaveRadiationFlux,
            "lcc": internal.OCFParameter.LowCloudCover,
            "mcc": internal.OCFParameter.MediumCloudCover,
            "hcc": internal.OCFParameter.HighCloudCover,
            "vis": internal.OCFParameter.VisibilityAGL,
            "u200": internal.OCFParameter.WindUComponent200m,
            "v200": internal.OCFParameter.WindVComponent200m,
            "u100": internal.OCFParameter.WindUComponent100m,
            "v100": internal.OCFParameter.WindVComponent100m,
            "tprate": internal.OCFParameter.RainPrecipitationRate,
        }


def _filterDatasetsByArea(dss: list[xr.Dataset], area: str) -> list[xr.Dataset]:
    """Filters a list of datasets by area."""
    if area == "uk":
        return list(filter(lambda ds: ds.coords["latitude"].as_numpy().max() == 60, dss))
    elif area == "nw-india":
        return list(filter(lambda ds: ds.coords["latitude"].as_numpy().max() == 31, dss))
    else:
        log.warn(event="Unknown area", area=area)
        return []
