"""The service class for the NWP Consumer."""

import concurrent.futures
import datetime as dt
import gc
import itertools
import pathlib
from concurrent.futures import ProcessPoolExecutor as PoolExecutor

import pandas as pd
import structlog

from nwp_consumer import internal

log = structlog.getLogger()


class NWPConsumerService:
    """The service class for the NWP Consumer.

    Each method on the class is a business use case for the consumer
    """

    def __init__(self, fetcher: internal.FetcherInterface, storer: internal.StorageInterface):
        self.fetcher = fetcher
        self.storer = storer

    def DownloadRawDataset(self, *, start: dt.date, end: dt.date) -> list[pathlib.Path]:
        """Fetch raw data for each initTime in the given range.

        :param start: The start date of the time range to download
        :param end: The end date of the time range to download
        """
        downloadedPaths: list[pathlib.Path] = []

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
            if not self.storer.rawFileExistsForInitTime(
                name=p.fname() + ".grib",
                it=p.initTime()
            )
        ]

        if not allWantedFileInfos:
            log.info("No new files to download, exiting.",
                     startDate=start.strftime("%Y-%m-%d %H:%M"),
                     endDate=end.strftime("%Y-%m-%d %H:%M"))
            return downloadedPaths

        # Download the files in parallel
        # * CEDA has a concurrent connection limit so limit the number of workers
        with PoolExecutor(max_workers=5) as pe:
            futures: list[concurrent.futures.Future[tuple[internal.FileInfoModel, bytes]]] = [
                pe.submit(self.fetcher.fetchRawFileBytes, fi=fi) for fi in allWantedFileInfos
            ]
            # Save the files as their downloads are completed
            for future in concurrent.futures.as_completed(futures):
                fileInfo, fileBytes = future.result()
                del future
                savedFilePath = self.storer.writeBytesToRawFile(
                    name=fileInfo.fname() + ".grib",
                    it=fileInfo.initTime(),
                    b=fileBytes
                )
                downloadedPaths.append(savedFilePath)
                del fileBytes
                gc.collect()

        return downloadedPaths

    def ConvertRawDatasetToZarr(self, *, start: dt.date, end: dt.date) -> list[pathlib.Path]:
        """Convert raw data for the given time range to Zarr.

        :param start: The start date of the time range to convert
        :param end: The end date of the time range to convert
        """
        savedPaths: list[pathlib.Path] = []

        # Get a list of all the init times that are stored locally between the start and end dates
        desiredInitTimes: list[dt.datetime] = []
        allInitTimes: list[dt.datetime] = self.storer.listInitTimesInRawDir()
        for it in allInitTimes:
            # Don't convert files that already exist
            if self.storer.zarrExistsForInitTime(name=it.strftime('%Y%m%d%H%M.zarr'), it=it):
                log.debug(
                    f"Zarr for initTime {it.strftime('%Y/%m/%d %H:%M')} already exists, skipping.",
                    initTime=it.strftime("%Y/%m/%d %H:%M")
                )
                continue
            if start <= it.date() <= end:
                desiredInitTimes.append(it)

        if not desiredInitTimes:
            log.info("No new files to convert to Zarr, exiting.",
                     startDate=start.strftime("%Y/%m/%d %H:%M"),
                     endDate=end.strftime("%Y/%m/%d %H:%M"))
            return savedPaths
        else:
            log.info(
                event=f"Converting {len(desiredInitTimes)} init times to zarr.",
                num=len(desiredInitTimes)
            )

        # For each init time, load the files from the storer and convert them to a dataset
        with PoolExecutor(max_workers=2) as pe:
            futures: list[concurrent.futures.Future[list[bytes]]] = [
                pe.submit(self.storer.readRawFilesForInitTime, it=it) for it in desiredInitTimes
            ]
            # Convert the files once they are read in
            for future in concurrent.futures.as_completed(futures):
                initTime, fileBytesList = future.result()
                del future

                log.debug(
                    f"Creating Zarr for initTime {initTime.strftime('%Y/%m/%d %H:%M')}",
                    initTime=initTime.strftime("%Y/%m/%d %H:%M")
                )
                dataset = self.fetcher.loadRawInitTimeDataAsOCFDataset(fbl=fileBytesList)
                del fileBytesList
                gc.collect()

                # Carry out a basic data quality check
                for var in dataset.data_vars:
                    if True in dataset[var].isnull():
                        log.warn(
                            event=f"Dataset for initTime {initTime.strftime('%Y/%m/%d %H:%M')}"
                            f" has NaNs in variable {var}",
                            initTime=initTime.strftime("%Y/%m/%d %H:%M"),
                            variable=var
                        )

                # Save the dataset to a zarr file
                initTime = pd.Timestamp(dataset.coords["init_time"].values[0])
                savedZarrPath = self.storer.writeDatasetAsZarr(
                    name=initTime.strftime('%Y%m%d%H%M.zarr'),
                    it=initTime,
                    ds=dataset
                )
                savedPaths.append(savedZarrPath)
                del dataset
                gc.collect()

        return savedPaths

    def DownloadAndConvert(self, *, start: dt.date, end: dt.date) -> list[pathlib.Path]:
        """Fetch and save as Zarr a dataset for each initTime in the given time range.

        :param start: The start date of the time range to download and convert
        :param end: The end date of the time range to download and convert
        """
        _ = self.DownloadRawDataset(
            start=start,
            end=end
        )
        paths = self.ConvertRawDatasetToZarr(
            start=start,
            end=end
        )

        return paths

    def CreateLatestZarr(self) -> pathlib.Path:
        """Create a Zarr file for the latest init time."""
        # Get the latest init time
        allInitTimes: list[dt.datetime] = self.storer.listInitTimesInRawDir()
        if not allInitTimes:
            log.info(event="No init times found in raw directory")
            return pathlib.Path()
        latestInitTime = allInitTimes[-1]

        # Check if the latest init time is already stored as a Zarr file
        if self.storer.zarrExistsForInitTime(name='latest.zarr', it=latestInitTime):
            self.storer.deleteZarrForInitTime(name='latest.zarr', it=latestInitTime)

        # Load the latest init time as a dataset
        _, fileBytesList = self.storer.readRawFilesForInitTime(it=latestInitTime)
        log.info(
            event=f"Creating Latest Zarr for initTime {latestInitTime.strftime('%Y/%m/%d %H:%M')}",
            initTime=latestInitTime.strftime("%Y/%m/%d %H:%M")
        )
        dataset = self.fetcher.loadRawInitTimeDataAsOCFDataset(fbl=fileBytesList)

        # Save the dataset to a zarr file
        savedZarrPath = self.storer.writeDatasetAsZarr(
            name='latest.zarr',
            it=latestInitTime,
            ds=dataset
        )
        del dataset

        return savedZarrPath
