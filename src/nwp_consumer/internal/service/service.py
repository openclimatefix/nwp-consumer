"""The service class for the NWP Consumer."""

import concurrent.futures
import datetime as dt
import itertools
import pathlib
import shutil
from concurrent.futures import ProcessPoolExecutor as PoolExecutor

import dask.bag
import pandas as pd
import psutil
import structlog
import xarray as xr
import zarr
from ocf_blosc2 import Blosc2
from typeid import TypeID

from nwp_consumer import internal

log = structlog.getLogger()


class NWPConsumerService:
    """The service class for the NWP Consumer.

    Each method on the class is a business use case for the consumer
    """

    def __init__(self, fetcher: internal.FetcherInterface, storer: internal.StorageInterface, rawdir: str, zarrdir: str):
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
            end=end+dt.timedelta(days=1),
            inclusive='left',
            freq='H').to_pydatetime().tolist()

        # For each init time, get the list of files that need to be downloaded
        allWantedFileInfos: list[internal.FileInfoModel] = list(itertools.chain.from_iterable(
            [self.fetcher.listRawFilesForInitTime(it=initTime) for initTime in allInitTimes]
        ))

        # Check which files are already downloaded
        # * If the file is already downloaded, remove it from the list of files to download
        allWantedFileInfos: list[internal.FileInfoModel] = [
            p for p in allWantedFileInfos
            if not self.storer.exists(
                dst=self.rawdir / p.initTime().strftime(internal.IT_FOLDER_FMTSTR) / (p.fname() + ".grib")
            )
        ]

        if not allWantedFileInfos:
            log.info("no new files to download",
                     startDate=start.strftime("%Y-%m-%d %H:%M"),
                     endDate=end.strftime("%Y-%m-%d %H:%M"))
            return nbytes
        else:
            log.info("downloading files",
                     startDate=start.strftime("%Y-%m-%d %H:%M"),
                     endDate=end.strftime("%Y-%m-%d %H:%M"),
                     numFiles=len(allWantedFileInfos))

        # Download the files to temp in parallel
        # * CEDA has a concurrent connection limit so limit the number of workers
        with PoolExecutor(max_workers=5) as pe:
            futures: list[concurrent.futures.Future[tuple[internal.FileInfoModel, pathlib.Path]]] = [
                pe.submit(self.fetcher.downloadToTemp, fi=fi) for fi in allWantedFileInfos
            ]
            # Save the files to store as their downloads are completed
            # * This deletes the temporary files
            for future in concurrent.futures.as_completed(futures):
                fileInfo, tempFile = future.result()
                if tempFile == pathlib.Path():
                    continue
                nbytes += self.storer.store(
                    src=tempFile,
                    dst=self.rawdir / fileInfo.initTime().strftime(internal.IT_FOLDER_FMTSTR) / (fileInfo.fname() + ".grib")
                )

        return nbytes

    def ConvertRawDatasetToZarr(self, *, start: dt.date, end: dt.date) -> int:
        """Convert raw data for the given time range to Zarr.

        :param start: The start date of the time range to convert
        :param end: The end date of the time range to convert
        """
        nbytes = 0

        # Get a list of all the init times that are stored locally between the start and end dates
        desiredInitTimes: list[dt.datetime] = []
        allInitTimes: list[dt.datetime] = self.storer.listInitTimes(prefix=self.rawdir)
        for it in allInitTimes:
            # Don't convert files that already exist
            if self.storer.exists(dst=self.zarrdir / it.strftime('%Y%m%d%H%M.zarr.zip')):
                log.debug(
                    f"zarr for initTime already exists; skipping",
                    inittime=it.strftime("%Y/%m/%d %H:%M"),
                    path=(self.zarrdir / it.strftime('%Y%m%d%H%M.zarr.zip')).as_posix()
                )
                continue
            if start <= it.date() <= end:
                desiredInitTimes.append(it)

        if not desiredInitTimes:
            log.info("no new files to convert to zarr",
                     startDate=start.strftime("%Y/%m/%d %H:%M"),
                     endDate=end.strftime("%Y/%m/%d %H:%M"))
            return nbytes
        else:
            log.info(
                event=f"converting {len(desiredInitTimes)} init times to zarr.",
                num=len(desiredInitTimes)
            )

        # For each init time, load the files from the store to temp and map them to a dataset
        with PoolExecutor(max_workers=2) as pe:
            futures: list[concurrent.futures.Future[tuple[dt.datetime, list[pathlib.Path]]]] = [
                pe.submit(self.storer.copyITFolderToTemp, prefix=self.rawdir, it=it) for it in desiredInitTimes
            ]
            # Convert the files once they are read in
            for future in concurrent.futures.as_completed(futures):
                initTime, tempPaths = future.result()

                if not tempPaths:
                    log.warn(
                        event=f"no files for initTime",
                        initTime=initTime.strftime("%Y/%m/%d %H:%M")
                    )
                    continue

                log.debug(
                    event=f"creating zarr for initTime",
                    initTime=initTime.strftime("%Y/%m/%d %H:%M")
                )

                # Create a pipeline to convert the raw files and merge them as a dataset
                bag: dask.bag.Bag = dask.bag.from_sequence(tempPaths)
                dataset = bag.map(func=lambda tfp: self.fetcher.mapTemp(p=tfp)) \
                    .fold(binop=lambda ds1, ds2: xr.merge([ds1, ds2], combine_attrs="drop_conflicts")) \
                    .compute()

                # Carry out a basic data quality check
                if dataset == xr.Dataset():
                    log.warn(
                        event=f"Dataset for initTime is empty",
                        initTime=initTime.strftime("%Y/%m/%d %H:%M")
                    )
                    continue

                for var in dataset.coords['variable'].values:
                    if True in dataset.sel(variable=var).isnull():
                        log.warn(
                            event=f"Dataset for initTime {initTime.strftime('%Y/%m/%d %H:%M')}"
                            f" has NaNs in variable {var}",
                            initTime=initTime.strftime("%Y/%m/%d %H:%M"),
                            variable=var
                        )

                # Save the dataset to a temp zarr file
                initTime = pd.Timestamp(dataset.coords["init_time"].values[0])
                tempZarrPath = internal.TMP_DIR / str(TypeID(prefix="nwpc"))
                with zarr.ZipStore(path=tempZarrPath.as_posix(), mode='w') as store:
                    dataset.to_zarr(
                        store=store,
                        encoding={
                            "init_time": {"units": "nanoseconds since 1970-01-01"},
                            "UKV": {
                                "compressor": Blosc2(cname="zstd", clevel=5),
                            },
                        },
                        compute=True
                    )

                # Move the temp zarr file to the store
                nbytes += self.storer.store(
                    src=tempZarrPath,
                    dst=self.zarrdir / initTime.strftime('%Y%m%d%H%M.zarr.zip')
                )

                # Delete the raw temporary files
                for f in tempPaths:
                    f.unlink(missing_ok=True)

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

        # Check if the latest init time is already stored as a Zarr file
        if self.storer.exists(dst=self.zarrdir / 'latest.zarr.zip'):
            self.storer.delete(p=self.zarrdir / 'latest.zarr.zip')

        # Load the latest init time as a dataset
        _, tempPaths = self.storer.copyITFolderToTemp(it=latestInitTime, prefix=self.rawdir)
        log.info(
            event=f"creating latest zarr for initTime",
            inittime=latestInitTime.strftime("%Y/%m/%d %H:%M"),
            path=(self.zarrdir / 'latest.zarr.zip').as_posix()
        )

        # Create a pipeline to convert the raw files and merge them as a dataset
        bag: dask.bag.Bag = dask.bag.from_sequence(tempPaths)
        dataset = bag.map(lambda tfp: self.fetcher.mapTemp(p=tfp)) \
            .fold(lambda ds1, ds2: xr.merge([ds1, ds2], combine_attrs="drop_conflicts")) \
            .compute()

        # Save the dataset to a temp zarr file
        tempZarrPath = internal.TMP_DIR / str(TypeID(prefix="nwpc"))
        dataset.to_zarr(
            store=zarr.ZipStore(path=tempZarrPath.as_posix(), mode='w'),
            encoding={
                "init_time": {"units": "nanoseconds since 1970-01-01"},
                "UKV": {
                    "compressor": Blosc2(cname="zstd", clevel=5),
                },
            },
        )

        # Move the temp zarr file to the store
        nbytes = self.storer.store(
            src=tempZarrPath,
            dst=self.zarrdir / 'latest.zarr.zip'
        )

        # Delete the temporary files
        for f in tempPaths:
            f.unlink(missing_ok=True)

        return nbytes

    def Check(self) -> int:
        """Perform a healthcheck on the service"""

        unhealthy = False

        # Check eccodes is installed
        import eccodes
        log.info(event="HEALTH: eccodes is installed", version=eccodes.codes_get_api_version())

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
                used=tmp_usage.used
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
