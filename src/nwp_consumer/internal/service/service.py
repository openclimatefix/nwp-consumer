import datetime as dt
import pathlib
from concurrent.futures import ProcessPoolExecutor as PoolExecutor
import concurrent.futures
import xarray as xr
import pandas as pd
import structlog
import itertools

from nwp_consumer import internal

log = structlog.stdlib.get_logger()


class NWPConsumerService:
    """The service class for the NWP Consumer.

    Each method on the class is a business use case for the consumer
    """

    def __init__(self, fetcher: internal.FetcherInterface, storer: internal.StorageInterface):
        self.fetcher = fetcher
        self.storer = storer

    def DownloadRawDataset(self, startDate: dt.date, endDate: dt.date) -> list[pathlib.Path]:
        """Fetches a dataset for each initTime in the given time range and saves it to the given store."""

        downloadedPaths: list[pathlib.Path] = []

        # Get the list of init times as datetime objects
        # * This spans every hour between the start and end dates inclusive
        allInitTimes: list[dt.datetime] = pd.date_range(startDate, endDate, freq='H').to_pydatetime().tolist()

        # For each init time, get the list of files that need to be downloaded
        allWantedFileInfos: list[internal.FileInfoModel] = list(itertools.chain.from_iterable(
            [self.fetcher.listRawFilesForInitTime(initTime=initTime) for initTime in allInitTimes]
        ))

        # Check which files are already downloaded
        # * If the file is already downloaded, remove it from the list of files to download
        allWantedFileInfos: list[internal.FileInfoModel] = [
            p for p in allWantedFileInfos if not self.storer.existsInRawDir(fileName=p.fname(), initTime=p.initTime())
        ]

        # Download the files in parallel
        # * CEDA can only handle 10 concurrent connections so limit the number of workers to 10
        with PoolExecutor(max_workers=10) as pe:
            futures: list[concurrent.futures.Future[tuple[internal.FileInfoModel, bytes]]] = [
                pe.submit(self.fetcher.fetchRawFileBytes, fileInfo=fi) for fi in allWantedFileInfos
            ]
            # Save the files as their downloads are completed
            for future in concurrent.futures.as_completed(futures):
                fileInfo, fileBytes = future.result()
                savedFilePath = self.storer.writeBytesToRawDir(
                    fileName=fileInfo.fname(), initTime=fileInfo.initTime(), data=fileBytes
                )
                downloadedPaths.append(savedFilePath)

        return downloadedPaths

    def ConvertRawDatasetToZarr(self, startDate: dt.date, endDate: dt.date) -> list[pathlib.Path]:
        """Fetches a dataset for each initTime in the given time range and saves it as Zarr to the given store."""
        savedPaths: list[pathlib.Path] = []

        # Get a list of all the files present in the raw directory


        # Get model init times for each day in the given time range
        for d in (startDate + dt.timedelta(days=n) for n in range((endDate - startDate).days + 1)):
            initTimes: list[dt.datetime] = [
                dt.datetime(year=d.year, month=d.month, day=d.day, hour=x) for x in range(0, 24, 3)
            ]

            for initTime in initTimes:
                # Download the raw data if it is not already present
                initTimePaths: list[pathlib.Path] = self.fetcher.downloadRawDataForInitTime(initTime=initTime)
                # Convert the data for the given init time into a dataset
                initTimeDataset: xr.Dataset = self.fetcher.loadRawInitTimeDataAsOCFDataset(rawRelativePaths=initTimePaths, initTime=initTime)
                # Save the dataset to the store
                zarrPath: pathlib.Path = pathlib.Path(f"{initTime:%Y/%m/UKV_%Y%m}.zarr")
                if not self.storer.existsInZarrDir(relativePath=zarrPath):
                    self.storer.saveDataset(dataset=initTimeDataset, relativePath=zarrPath)
                    savedPaths.append(zarrPath)
                else:
                    self.storer.appendDataset(dataset=initTimeDataset, relativePath=zarrPath)

                del initTimeDataset

        return savedPaths
