"""The service class for the NWP Consumer."""

import datetime as dt
import pathlib
import shutil
from typing import TYPE_CHECKING

import dask.bag
import pandas as pd
import psutil
import structlog
import xarray as xr
import zarr
from ocf_blosc2 import Blosc2

if TYPE_CHECKING:
    import numpy as np


from nwp_consumer import internal

log = structlog.getLogger()


class NWPConsumerService:
    """The service class for the NWP Consumer.

    Each method on the class is a business use case for the consumer
    """

    # Dependency-injected attributes
    fetcher: internal.FetcherInterface
    storer: internal.StorageInterface
    rawstorer: internal.StorageInterface
    # Configuration options
    rawdir: pathlib.Path
    zarrdir: pathlib.Path

    def __init__(
        self,
        *,
        fetcher: internal.FetcherInterface,
        storer: internal.StorageInterface,
        rawdir: str,
        zarrdir: str,
        rawstorer: internal.StorageInterface | None = None,
    ) -> None:
        """Create a consumer service with the given dependencies.

        Args:
            fetcher: The fetcher to use for downloading data
            storer: The storer to use for saving data
            rawdir: The directory to store raw data
            zarrdir: The directory to store zarr data
            rawstorer: The storer to use for saving raw data. Defaults to the storer.
        """
        self.fetcher = fetcher
        self.storer = storer
        self.rawstorer = rawstorer if rawstorer is not None else storer
        self.rawdir = pathlib.Path(rawdir)
        self.zarrdir = pathlib.Path(zarrdir)

    def DownloadRawDataset(self, *, start: dt.datetime, end: dt.datetime) -> list[pathlib.Path]:
        """Fetch raw data for each initTime in the given range.

        :param start: The start date of the time range to download
        :param end: The end date of the time range to download
        """
        # Get the list of init times valid for the fetcher
        # between the start and end times (inclusive)
        allInitTimes: list[dt.datetime] = [
            pdt.to_pydatetime()
            for pdt in pd.date_range(
                start=start,
                end=end,
                inclusive="left",
                freq="h",
                tz=dt.UTC,
            ).tolist()
            if pdt.to_pydatetime().hour in self.fetcher.getInitHours()
        ]

        # For each init time, get the list of files that need to be downloaded
        # * Itertools chain is used to flatten the list of lists
        allWantedFileInfos: list[internal.FileInfoModel] = (
            dask.bag.from_sequence(allInitTimes, npartitions=len(allInitTimes))
            .map(lambda it: self.fetcher.listRawFilesForInitTime(it=it))
            .flatten()
            .compute()
        )

        # Check which files are already downloaded
        # * If the file is already downloaded, remove it from the list of files to download
        newWantedFileInfos: list[internal.FileInfoModel] = [
            fi
            for fi in allWantedFileInfos
            if not self.rawstorer.exists(
                dst=self.rawdir / fi.it().strftime(internal.IT_FOLDER_STRUCTURE_RAW) / fi.filename(),
            )
        ]

        if not newWantedFileInfos:
            log.info(
                event="no new files to download",
                startDate=start.strftime("%Y-%m-%d %H:%M"),
                endDate=end.strftime("%Y-%m-%d %H:%M"),
            )
            return [
                self.rawdir / fi.it().strftime(internal.IT_FOLDER_STRUCTURE_RAW) / fi.filename()
                for fi in allWantedFileInfos
            ]
        else:
            log.info(
                event="downloading files",
                startDate=start.strftime("%Y-%m-%d %H:%M"),
                endDate=end.strftime("%Y-%m-%d %H:%M"),
                numFiles=len(newWantedFileInfos),
            )

        # Create a dask pipeline to download the files
        storedFiles: list[pathlib.Path] = (
            dask.bag.from_sequence(seq=newWantedFileInfos, npartitions=len(newWantedFileInfos))
            .map(lambda fi: self.fetcher.downloadToCache(fi=fi))
            .filter(lambda infoPathTuple: infoPathTuple[1] != pathlib.Path())
            .map(
                lambda infoPathTuple: self.rawstorer.store(
                    src=infoPathTuple[1],
                    dst=self.rawdir / infoPathTuple[1].relative_to(internal.CACHE_DIR_RAW),
                ),
            )
            .compute()
        )

        return storedFiles

    def ConvertRawDatasetToZarr(
        self,
        *,
        start: dt.datetime,
        end: dt.datetime,
    ) -> list[pathlib.Path]:
        """Convert raw data for the given time range to Zarr.

        :param start: The start date of the time range to convert
        :param end: The end date of the time range to convert
        """
        # Get a list of all the init times that are stored locally between the start and end dates
        desiredInitTimes: list[dt.datetime] = []
        allInitTimes: list[dt.datetime] = self.rawstorer.listInitTimes(prefix=self.rawdir)
        for it in allInitTimes:
            # Don't convert files that already exist
            if self.storer.exists(
                dst=self.zarrdir / it.strftime(f"{internal.IT_FULLPATH_ZARR}.zip"),
            ):
                log.debug(
                    "zarr for initTime already exists; skipping",
                    inittime=it.strftime("%Y/%m/%d %H:%M"),
                    path=(
                        self.zarrdir / it.strftime(f"{internal.IT_FULLPATH_ZARR}.zip")
                    ).as_posix(),
                )
                continue
            if start <= it <= end:
                desiredInitTimes.append(it)

        if not desiredInitTimes:
            log.info(
                "no new files to convert to zarr",
                startDate=start.strftime("%Y/%m/%d %H:%M"),
                endDate=end.strftime("%Y/%m/%d %H:%M"),
            )
            return [
                self.zarrdir / it.strftime(f"{internal.IT_FULLPATH_ZARR}.zip")
                for it in allInitTimes
                if start <= it <= end
            ]
        else:
            log.info(
                event=f"converting {len(desiredInitTimes)} init times to zarr.",
                num=len(desiredInitTimes),
            )

        # Create a pipeline to carry out the conversion
        # * Build a bag from the sequence of init times
        # * Partition the bag by init time
        bag = dask.bag.from_sequence(desiredInitTimes, npartitions=len(desiredInitTimes))
        storedfiles = (
            bag.map(lambda time: self.rawstorer.copyITFolderToCache(prefix=self.rawdir, it=time))
            .filter(lambda cachedpaths: len(cachedpaths) != 0)
            .map(lambda cachedpaths: [self.fetcher.mapCachedRaw(p=p) for p in cachedpaths])
            .map(lambda datasets: _mergeDatasets(datasets=datasets))
            .filter(_dataQualityFilter)
            .map(lambda ds: _cacheAsZipZarr(ds=ds))
            .map(
                lambda path: self.storer.store(
                    src=path,
                    dst=self.zarrdir / path.relative_to(internal.CACHE_DIR_ZARR),
                ),
            )
            .compute()
        )

        if not isinstance(storedfiles, list):
            storedfiles = [storedfiles]

        return storedfiles

    def DownloadAndConvert(
        self,
        *,
        start: dt.datetime,
        end: dt.datetime,
    ) -> tuple[list[pathlib.Path], list[pathlib.Path]]:
        """Fetch and save as Zarr a dataset for each initTime in the given time range.

        :param start: The start date of the time range to download and convert
        :param end: The end date of the time range to download and convert
        """
        downloadedFiles = self.DownloadRawDataset(start=start, end=end)
        convertedFiles = self.ConvertRawDatasetToZarr(start=start, end=end)

        return downloadedFiles, convertedFiles

    def CreateLatestZarr(self) -> list[pathlib.Path]:
        """Create a Zarr file for the latest init time."""
        # Get the latest init time
        allInitTimes: list[dt.datetime] = self.rawstorer.listInitTimes(prefix=self.rawdir)
        if not allInitTimes:
            log.info(event="no init times found", within=self.rawdir)
            return []
        latestInitTime = allInitTimes[-1]

        # Load the latest init time as a dataset
        cachedPaths = self.rawstorer.copyITFolderToCache(it=latestInitTime, prefix=self.rawdir)
        log.info(
            event="creating latest zarr for initTime",
            inittime=latestInitTime.strftime("%Y/%m/%d %H:%M"),
            path=(self.zarrdir / "latest.zarr.zip").as_posix(),
        )

        # Create a pipeline to convert the raw files and merge them as a dataset
        # * Then cache the dataset as a zarr file and store it in the store
        bag: dask.bag.Bag = dask.bag.from_sequence(cachedPaths)
        cachedZarrs = (
            bag.map(lambda tfp: self.fetcher.mapCachedRaw(p=tfp))
            .fold(lambda ds1, ds2: xr.merge([ds1, ds2], combine_attrs="drop_conflicts"))
            .compute()
        )

        datasets = dask.bag.from_sequence([cachedZarrs])

        # Save as zipped zarr
        if self.storer.exists(dst=self.zarrdir / "latest.zarr.zip"):
            self.storer.delete(p=self.zarrdir / "latest.zarr.zip")
        storedFiles = (
            datasets.map(lambda ds: _cacheAsZipZarr(ds=ds))
            .map(lambda path: self.storer.store(src=path, dst=self.zarrdir / "latest.zarr.zip"))
            .compute()
        )

        # Save as regular zarr
        if self.storer.exists(dst=self.zarrdir / "latest.zarr"):
            self.storer.delete(p=self.zarrdir / "latest.zarr")
        storedFiles += (
            datasets.map(lambda ds: _cacheAsZarr(ds=ds))
            .map(lambda path: self.storer.store(src=path, dst=self.zarrdir / "latest.zarr"))
            .compute()
        )

        # Delete the cached files
        for f in cachedPaths:
            f.unlink(missing_ok=True)

        return storedFiles

    def Check(self) -> int:
        """Perform a healthcheck on the service."""
        unhealthy = False

        # Check eccodes is installed
        try:
            from cfgrib.messages import eccodes_version

            log.info(event="HEALTH: eccodes is installed", version=eccodes_version)
        except Exception as e:
            log.error(event="HEALTH: eccodes binary is not installed", error=str(e))
            unhealthy = True

        # Check the raw directory exists
        if not self.storer.exists(dst=self.rawdir):
            log.error(event="HEALTH: raw directory does not exist", path=self.rawdir.as_posix())
            unhealthy = True
        else:
            log.info(event="HEALTH: found raw directory", path=self.rawdir.as_posix())

        # Check the zarr directory exists
        if not self.storer.exists(dst=self.zarrdir):
            log.error(event="HEALTH: zarr directory does not exist", path=self.zarrdir.as_posix())
            unhealthy = True
        else:
            log.info(event="HEALTH: found zarr directory", path=self.zarrdir.as_posix())

        # Check that the cache directory is not approaching capacity
        internal.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_usage = shutil.disk_usage(internal.CACHE_DIR.as_posix())
        if cache_usage.free < 1e9:
            log.error(
                event="HEALTH: cache directory is full",
                free=cache_usage.free,
                total=cache_usage.total,
                used=cache_usage.used,
            )
            unhealthy = True
        else:
            log.info(
                event="HEALTH: found cache directory",
                free=cache_usage.free,
                total=cache_usage.total,
                used=cache_usage.used,
                path=internal.CACHE_DIR.as_posix(),
            )

        # Check the ram usage
        ram_usage = psutil.virtual_memory()
        if ram_usage.percent > 95:
            log.error(
                event="HEALTH: ram usage is high",
                available=ram_usage.available,
                total=ram_usage.total,
                used=ram_usage.used,
                percent=ram_usage.percent,
            )
            unhealthy = True
        else:
            log.info(
                event="HEALTH: found ram usage",
                free=ram_usage.free,
                total=ram_usage.total,
                used=ram_usage.used,
                percent=ram_usage.percent,
            )

        # Check the CPU usage
        cpu_usage = psutil.cpu_percent()
        if cpu_usage > 95:
            log.error(event="HEALTH: cpu usage is high", percent=cpu_usage)
            unhealthy = True
        else:
            log.info(event="HEALTH: found cpu usage", percent=cpu_usage)

        if unhealthy:
            return 1

        return 0


def _cacheAsZipZarr(ds: xr.Dataset) -> pathlib.Path:
    """Save the dataset to the cache as a zipped zarr file."""
    # Get the name of the zarr file from the inittime and the zarr format string
    dt64: np.datetime64 = ds.coords["init_time"].values[0]
    initTime: dt.datetime = dt.datetime.fromtimestamp(dt64.astype(int) / 1e9, tz=dt.UTC)
    cachePath: pathlib.Path = internal.zarrCachePath(it=initTime).with_suffix(".zarr.zip")
    # Delete the cached zarr if it already exists
    if cachePath.exists():
        cachePath.unlink()
    cachePath.parent.mkdir(parents=True, exist_ok=True)
    # Save the dataset to a zarr file
    with zarr.ZipStore(path=cachePath.as_posix(), mode="w") as store:
        ds.to_zarr(
            store=store,
            encoding=_generate_encoding(ds=ds),
        )

    log.debug("Saved as zipped zarr", path=cachePath.as_posix())
    return cachePath


def _cacheAsZarr(ds: xr.Dataset) -> pathlib.Path:
    """Save the dataset to the cache as a zarr file."""
    # Get the name of the zarr file from the inittime and the zarr format string
    dt64: np.datetime64 = ds.coords["init_time"].values[0]
    initTime: dt.datetime = dt.datetime.fromtimestamp(dt64.astype(int) / 1e9, tz=dt.UTC)
    cachePath: pathlib.Path = internal.zarrCachePath(it=initTime)
    if cachePath.exists() and cachePath.is_dir():
        shutil.rmtree(cachePath.as_posix())
    ds.to_zarr(
        store=cachePath.as_posix(),
        encoding=_generate_encoding(ds=ds),
    )
    return cachePath


def _generate_encoding(ds: xr.Dataset) -> dict[str, dict[str, str] | dict[str, Blosc2]]:
    encoding = {"init_time": {"units": "nanoseconds since 1970-01-01"}}
    for var in ds.data_vars.keys():
        encoding[var] = {"compressor": Blosc2(cname="zstd", clevel=5)}
    return encoding


def _dataQualityFilter(ds: xr.Dataset) -> bool:
    """Filter out data that is not of sufficient quality."""
    if ds == xr.Dataset():
        return False

    # Carry out a basic data quality check
    if "variable" not in dict(ds.coords.items()):
        log.warn(
            event="Dataset for is missing variable coord, checking other data variables",
            initTime=str(ds.coords["init_time"].values[0])[:16],
            coords=dict(ds.coords.items()),
        )
        for data_var in ds.data_vars.keys():
            if True in ds[f"{data_var}"].isnull():
                log.warn(
                    event=f"Dataset has NaNs in variable {data_var}",
                    initTime=str(ds.coords["init_time"].values[0])[:16],
                    variable=data_var,
                )

    for var in ds.coords["variable"].values:
        if True in ds.sel(variable=var).isnull():
            log.warn(
                event=f"Dataset has NaNs in variable {var}",
                initTime=str(ds.coords["init_time"].values[0])[:16],
                variable=var,
            )

    return True


def _mergeDatasets(datasets: list[xr.Dataset]) -> xr.Dataset:
    """Merge a list of datasets into a single dataset."""
    try:
        ds: xr.Dataset = xr.merge(objects=datasets, combine_attrs="drop_conflicts")
    except (xr.MergeError, ValueError, Exception) as e:
        log.warn(
            event="Merging datasets failed, trying to insert zeros for missing variables",
            exception=str(e),
            dataset1={
                "data_vars": list(datasets[0].data_vars.keys()),
                "dimensions": datasets[0].sizes,
            },
            dataset2={
                "data_vars": list(datasets[1].data_vars.keys()),
                "dimensions": datasets[1].sizes,
            },
        )
        ds = xr.merge(
            objects=datasets,
            combine_attrs="drop_conflicts",
            fill_value=0,
            compat="override",
        )
    return ds

