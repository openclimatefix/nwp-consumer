"""The service class for the NWP Consumer."""

import datetime as dt
import pathlib
import shutil
from collections.abc import Callable
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
    rename_vars: bool
    variable_dim: bool


    def __init__(
        self,
        *,
        fetcher: internal.FetcherInterface,
        storer: internal.StorageInterface,
        rawdir: str,
        zarrdir: str,
        rawstorer: internal.StorageInterface | None = None,
        rename_vars: bool = True,
        variable_dim: bool = True,
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
        self.rename_vars = rename_vars
        self.variable_dim = variable_dim

    def DownloadRawDataset(self, *, start: dt.datetime, end: dt.datetime) -> list[pathlib.Path]:
        """Download and convert raw data for a given time range.

        Args:
            start: The start of the time range
            end: The end of the time range
        Returns:
            A list of the paths to the downloaded files
        """
        return self._performFuncForMultipleInitTimes(
            func=self._downloadSingleInitTime,
            start=start,
            end=end,
        )

    def ConvertRawDatasetToZarr(
        self, *, start: dt.datetime, end: dt.datetime,
    ) -> list[pathlib.Path]:
        """Convert raw data for a given time range.

        Args:
            start: The start of the time range
            end: The end of the time range
        Returns:
            A list of the paths to the converted files
        """
        return self._performFuncForMultipleInitTimes(
            func=self._convertSingleInitTime,
            start=start,
            end=end,
        )

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
        latestDataset = (
            bag.map(lambda tfp: self.fetcher.mapCachedRaw(p=tfp))
            .fold(lambda ds1, ds2: _mergeDatasets([ds1, ds2]))
            .compute()
        )
        if not _dataQualityFilter(ds=latestDataset):
            return []
        if self.rename_vars:
            for var in latestDataset.data_vars:
                if var in self.fetcher.parameterConformMap():
                    latestDataset = latestDataset.rename(
                        {var: self.fetcher.parameterConformMap()[var].value}
                    )
        if self.variable_dim:
            latestDataset = (
                latestDataset.to_array(dim="variable", name=self.fetcher.datasetName())
                .to_dataset()
                .transpose("variable", ...)
            )
        datasets = dask.bag.from_sequence([latestDataset])
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

    def _downloadSingleInitTime(self, it: dt.datetime) -> list[pathlib.Path]:
        """Download and convert raw data for a given init time.

        Args:
            it: The init time to download
        Returns:
            A list of the paths to the downloaded files
        """
        # Check the init time is valid for the fetcher
        if it.hour not in self.fetcher.getInitHours():
            log.error(
                event="init time not valid for chosen source",
                inittime=it.strftime("%Y-%m-%d %H:%M"),
                validHours=self.fetcher.getInitHours(),
            )
            return []

        # Get the list of files available from the source
        allSourceFiles: list[internal.FileInfoModel] = self.fetcher.listRawFilesForInitTime(
            it=it,
        )
        # Cache any existing files from the raw storer
        cachedFiles: list[pathlib.Path] = self.rawstorer.copyITFolderToCache(
            prefix=self.rawdir, it=it
        )

        # Create a dask pipeline from the available files
        rb = dask.bag.from_sequence(allSourceFiles)
        # Download the files to the cache, filtering any already cached or failed downloads
        rb = rb.map(
                lambda fi: self.fetcher.downloadToCache(fi=fi)
                if fi.filename() not in [cf.name for cf in cachedFiles]
                else cachedFiles.pop(cachedFiles.index(internal.rawCachePath(it=it, filename=fi.filename())))
            ).filter(
            lambda p: p != pathlib.Path()
        )
        # Store the files using the raw storer
        rb = rb.map(
            lambda p: self.rawstorer.store(
                src=p,
                dst=self.rawdir / p.relative_to(internal.CACHE_DIR_RAW),
            )
        )
        storedFiles: list[pathlib.Path] = rb.compute()
        return storedFiles

    def _convertSingleInitTime(self, it: dt.datetime) -> list[pathlib.Path]:
        """Convert raw data for a single init time to zarr.

        Args:
            it: The init time to convert
        Returns:
            List of paths to converted files
        """
        # Get the raw files for the init time
        zbag = dask.bag.from_sequence(self.rawstorer.copyITFolderToCache(prefix=self.rawdir, it=it))
        # Load the raw files as xarray datasets
        zbag = zbag.map(lambda p: self.fetcher.mapCachedRaw(p=p))
        # Merge the datasets into a single dataset for the init time
        # * Bag.fold is a parallelized version of the reduce function, so
        # * in this case, first the partitions are merged, followed by the results
        zbag = zbag.fold(lambda a, b: _mergeDatasets([a, b]))
        ds = zbag.compute()

        # Filter out datasets that are not of sufficient quality
        if not _dataQualityFilter(ds=ds):
            return []

        if self.rename_vars:
            for var in ds.data_vars:
                if var in self.fetcher.parameterConformMap():
                    ds = ds.rename({var: self.fetcher.parameterConformMap()[var].value})

        if self.variable_dim:
            ds = (
                ds.to_array(dim="variable", name=self.fetcher.datasetName())
                .to_dataset()
                .transpose("variable", ...)
            )
        # Cache the dataset as a zarr file
        zpath = _cacheAsZipZarr(ds=ds)
        # Store the zarr file using the storer
        return [self.storer.store(src=zpath, dst=self.zarrdir / zpath.name)]


    def _performFuncForMultipleInitTimes(
        self,
        *,
        func=Callable[[dt.datetime], list[pathlib.Path]],
        start: dt.datetime,
        end: dt.datetime,
    ):
        """Perform a function for each init time in the fetcher's range."""
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

        log.info(
            event="Carrying out function for multiple init times",
            func=func.__name__,
            start=start.strftime("%Y-%m-%d %H:%M"),
            end=end.strftime("%Y-%m-%d %H:%M"),
            num=len(allInitTimes),
        )

        paths: list[pathlib.Path] = []
        for it in allInitTimes:
            paths.extend(func(it))

        return paths


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
    for var in ds.data_vars:
        encoding[var] = {"compressor": Blosc2(cname="zstd", clevel=5)}
    return encoding


def _dataQualityFilter(ds: xr.Dataset) -> bool:
    """Filter out data that is not of sufficient quality."""
    if ds == xr.Dataset():
        return False

    # Carry out a basic data quality check
    for data_var in ds.data_vars:
        if ds[f"{data_var}"].isnull().any():
            log.warn(
                event=f"Dataset has NaNs in variable {data_var}",
                initTime=str(ds.coords["init_time"].values[0])[:16],
                variable=data_var,
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
            numdatasets=len(datasets),
            datasets={
                i: {
                    "data_vars": list(datasets[i].data_vars.keys()),
                    "dimensions": datasets[i].sizes,
                    "indexes": list(datasets[i].indexes.keys()),
                }
                for i, ds in enumerate(datasets)
            },
        )
        ds = xr.merge(
            objects=datasets,
            combine_attrs="drop_conflicts",
            fill_value=0,
            compat="override",
        )
    del datasets
    return ds
