"""The service class for the NWP Consumer."""

import datetime as dt
import itertools
import pathlib
import shutil

import dask.bag
import pandas as pd
import psutil
import structlog
import xarray as xr
import zarr
from ocf_blosc2 import Blosc2

from nwp_consumer import internal

log = structlog.getLogger()


class NWPConsumerService:
    """The service class for the NWP Consumer.

    Each method on the class is a business use case for the consumer
    """

    def __init__(self, *, fetcher: internal.FetcherInterface, storer: internal.StorageInterface,
                 rawdir: str, zarrdir: str) -> None:
        """Initialise the service."""
        self.fetcher = fetcher
        self.storer = storer
        self.rawdir = pathlib.Path(rawdir)
        self.zarrdir = pathlib.Path(zarrdir)

    def DownloadRawDataset(self, *, start: dt.date, end: dt.date) -> int:
        """Fetch raw data for each initTime in the given range.

        :param start: The start date of the time range to download
        :param end: The end date of the time range to download
        """
        nbytes = 0

        # Get the list of init times as datetime objects
        # * This spans every hour between the start and end dates up to 11:00pm on the end date
        allInitTimes: list[dt.datetime] = pd.date_range(
            start=start,
            end=end + dt.timedelta(days=1),
            inclusive='left',
            freq='H').to_pydatetime().tolist()

        # For each init time, get the list of files that need to be downloaded
        # * Itertools chain is used to flatten the list of lists
        allWantedFileInfos: list[internal.FileInfoModel] = list(itertools.chain.from_iterable(
            [self.fetcher.listRawFilesForInitTime(it=initTime) for initTime in allInitTimes]
        ))

        # Check which files are already downloaded
        # * If the file is already downloaded, remove it from the list of files to download
        newWantedFileInfos: list[internal.FileInfoModel] = [
            fi for fi in allWantedFileInfos
            if not self.storer.exists(
                dst=self.rawdir / fi.it().strftime(internal.IT_FOLDER_FMTSTR) / fi.filename()
            )
        ]

        if not newWantedFileInfos:
            log.info("no new files to download",
                     startDate=start.strftime("%Y-%m-%d %H:%M"),
                     endDate=end.strftime("%Y-%m-%d %H:%M"))
            return nbytes
        else:
            log.info("downloading files",
                     startDate=start.strftime("%Y-%m-%d %H:%M"),
                     endDate=end.strftime("%Y-%m-%d %H:%M"),
                     numFiles=len(newWantedFileInfos))

        # Create a dask pipeline to download the files
        nbytes = dask.bag.from_sequence(newWantedFileInfos, npartitions=len(newWantedFileInfos)) \
            .map(lambda fi: self.fetcher.downloadToTemp(fi=fi)) \
            .filter(lambda infoPathTuple: infoPathTuple[1] != pathlib.Path()) \
            .map(lambda infoPathTuple: self.storer.store(
                src=infoPathTuple[1],
                dst=self.rawdir/infoPathTuple[0].it().strftime(internal.IT_FOLDER_FMTSTR)/(infoPathTuple[0].filename())
            )) \
            .sum() \
            .compute()

        return nbytes

    def ConvertRawDatasetToZarr(self, *, start: dt.date, end: dt.date) -> int:
        """Convert raw data for the given time range to Zarr.

        :param start: The start date of the time range to convert
        :param end: The end date of the time range to convert
        """
        # Get a list of all the init times that are stored locally between the start and end dates
        desiredInitTimes: list[dt.datetime] = []
        allInitTimes: list[dt.datetime] = self.storer.listInitTimes(prefix=self.rawdir)
        for it in allInitTimes:
            # Don't convert files that already exist
            if self.storer.exists(dst=self.zarrdir / it.strftime(f'{internal.ZARR_FMTSTR}.zarr.zip')):
                log.debug(
                    "zarr for initTime already exists; skipping",
                    inittime=it.strftime("%Y/%m/%d %H:%M"),
                    path=(self.zarrdir / it.strftime(f'{internal.ZARR_FMTSTR}.zarr.zip')).as_posix()
                )
                continue
            if start <= it.date() <= end:
                desiredInitTimes.append(it)

        if not desiredInitTimes:
            log.info("no new files to convert to zarr",
                     startDate=start.strftime("%Y/%m/%d %H:%M"),
                     endDate=end.strftime("%Y/%m/%d %H:%M"))
            return 0
        else:
            log.info(
                event=f"converting {len(desiredInitTimes)} init times to zarr.",
                num=len(desiredInitTimes)
            )

        # Create a pipeline to carry out the conversion
        # * Build a bag from the sequence of init times
        # * Partition the bag by init time
        bag = dask.bag.from_sequence(desiredInitTimes, npartitions=len(desiredInitTimes))
        nbytes = bag.map(lambda time: self.storer.copyITFolderToTemp(prefix=self.rawdir, it=time)) \
            .filter(lambda temppaths: len(temppaths) != 0) \
            .map(lambda temppaths: [self.fetcher.mapTemp(p=p) for p in temppaths]) \
            .map(lambda datasets: xr.merge(objects=datasets, combine_attrs="drop_conflicts")) \
            .filter(_dataQualityFilter) \
            .map(lambda ds: _saveAsTempZipZarr(ds=ds)) \
            .map(lambda path: self.storer.store(src=path, dst=self.zarrdir / path.name)) \
            .sum() \
            .compute(num_workers=1)  # AWS ECS only has 1 CPU which amounts to half a physical core

        return nbytes

    def DownloadAndConvert(self, *, start: dt.date, end: dt.date) -> tuple[int, int]:
        """Fetch and save as Zarr a dataset for each initTime in the given time range.

        :param start: The start date of the time range to download and convert
        :param end: The end date of the time range to download and convert
        """
        downloadedBytes = self.DownloadRawDataset(
            start=start,
            end=end
        )
        storedBytes = self.ConvertRawDatasetToZarr(
            start=start,
            end=end
        )

        return downloadedBytes, storedBytes

    def CreateLatestZarr(self) -> int:
        """Create a Zarr file for the latest init time."""
        nbytes = 0

        # Get the latest init time
        allInitTimes: list[dt.datetime] = self.storer.listInitTimes(prefix=self.rawdir)
        if not allInitTimes:
            log.info(event="no init times found", within=self.rawdir)
            return nbytes
        latestInitTime = allInitTimes[-1]

        # Load the latest init time as a dataset
        tempPaths = self.storer.copyITFolderToTemp(it=latestInitTime, prefix=self.rawdir)
        log.info(
            event="creating latest zarr for initTime",
            inittime=latestInitTime.strftime("%Y/%m/%d %H:%M"),
            path=(self.zarrdir / 'latest.zarr.zip').as_posix()
        )

        # Create a pipeline to convert the raw files and merge them as a dataset
        # * Then save the dataset to a temp zarr file and store it in the store
        bag: dask.bag.Bag = dask.bag.from_sequence(tempPaths)
        tempZarrs = bag.map(lambda tfp: self.fetcher.mapTemp(p=tfp)) \
            .fold(lambda ds1, ds2: xr.merge([ds1, ds2], combine_attrs="drop_conflicts")) \
            .compute()

        datasets = dask.bag.from_sequence([tempZarrs])

        # Save as zipped zarr
        if self.storer.exists(dst=self.zarrdir / 'latest.zarr.zip'):
            self.storer.delete(p=self.zarrdir / 'latest.zarr.zip')
        nbytes1 = datasets.map(lambda ds: _saveAsTempZipZarr(ds=ds)) \
            .map(lambda path: self.storer.store(src=path, dst=self.zarrdir / 'latest.zarr.zip')) \
            .sum() \
            .compute()

        # Save as regular zarr
        if self.storer.exists(dst=self.zarrdir / 'latest.zarr'):
            self.storer.delete(p=self.zarrdir / 'latest.zarr')
        _ = datasets.map(lambda ds: _saveAsTempRegularZarr(ds=ds)) \
            .map(lambda path: self.storer.store(src=path, dst=self.zarrdir / 'latest.zarr')) \
            .sum() \
            .compute()

        # Delete the temporary files
        for f in tempPaths:
            f.unlink(missing_ok=True)

        return nbytes1

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

        # Check that the temporary directory is not approaching capacity
        internal.TMP_DIR.mkdir(parents=True, exist_ok=True)
        tmp_usage = shutil.disk_usage(internal.TMP_DIR.as_posix())
        if tmp_usage.free < 1e9:
            log.error(
                event="HEALTH: temporary directory is full",
                free=tmp_usage.free,
                total=tmp_usage.total,
                used=tmp_usage.used
            )
            unhealthy = True
        else:
            log.info(
                event="HEALTH: found temporary directory",
                free=tmp_usage.free,
                total=tmp_usage.total,
                used=tmp_usage.used,
                path=internal.TMP_DIR.as_posix()
            )

        # Check the ram usage
        ram_usage = psutil.virtual_memory()
        if ram_usage.percent > 95:
            log.error(
                event="HEALTH: ram usage is high",
                available=ram_usage.available,
                total=ram_usage.total,
                used=ram_usage.used,
                percent=ram_usage.percent
            )
            unhealthy = True
        else:
            log.info(
                event="HEALTH: found ram usage",
                free=ram_usage.free,
                total=ram_usage.total,
                used=ram_usage.used,
                percent=ram_usage.percent
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


def _saveAsTempZipZarr(ds: xr.Dataset) -> pathlib.Path:
    # Save the dataset to a temp zarr file
    initTime = dt.datetime.utcfromtimestamp(int(ds.coords["init_time"].values[0]) / 1e9)
    tempZarrPath = internal.TMP_DIR / (initTime.strftime(internal.ZARR_FMTSTR.split("/")[-1]) \
                                       + ".zarr.zip")
    if tempZarrPath.exists():
        tempZarrPath.unlink()
    with zarr.ZipStore(path=tempZarrPath.as_posix(), mode='w') as store:
        ds.to_zarr(
            store=store,
            encoding={
                "init_time": {"units": "nanoseconds since 1970-01-01"},
                "UKV": {
                    "compressor": Blosc2(cname="zstd", clevel=5),
                },
            },
        )
    return tempZarrPath


def _saveAsTempRegularZarr(ds: xr.Dataset) -> pathlib.Path:
    # Save the dataset to a temp zarr file
    initTime = dt.datetime.utcfromtimestamp(int(ds.coords["init_time"].values[0]) / 1e9)
    tempZarrPath = internal.TMP_DIR / (initTime.strftime(internal.ZARR_FMTSTR.split("/")[-1]) \
                                       + ".zarr")
    if tempZarrPath.exists() and tempZarrPath.is_dir():
        shutil.rmtree(tempZarrPath.as_posix())
    ds.to_zarr(
        store=tempZarrPath.as_posix(),
        encoding={
            "init_time": {"units": "nanoseconds since 1970-01-01"},
            "UKV": {
                "compressor": Blosc2(cname="zstd", clevel=5),
            },
        },
    )
    return tempZarrPath


def _dataQualityFilter(ds: xr.Dataset) -> bool:
    """Filter out data that is not of sufficient quality."""
    if ds == xr.Dataset():
        return False

    # Carry out a basic data quality check
    if "variable" not in dict(ds.coords.items()).keys():
        log.warn(
            event="Dataset for is missing variable coord",
            initTime=str(ds.coords['init_time'].values[0])[:16],
            coords=dict(ds.coords.items())
        )
        return False

    for var in ds.coords['variable'].values:
        if True in ds.sel(variable=var).isnull():
            log.warn(
                event=f"Dataset has NaNs in variable {var}",
                initTime=str(ds.coords['init_time'].values[0])[:16],
                variable=var
            )

    return True
