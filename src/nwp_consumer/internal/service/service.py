import datetime as dt
import pathlib
from concurrent.futures import ProcessPoolExecutor
import xarray as xr

from nwp_consumer import internal


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
        # Get model init times for each day in the given time range
        for d in (startDate + dt.timedelta(days=n) for n in range((endDate - startDate).days + 1)):
            initTimes: list[dt.datetime] = [
                dt.datetime(year=d.year, month=d.month, day=d.day, hour=x) for x in range(0, 24, 3)
            ]

            # Download the wholesale files given by the file infos
            # * There are two files of interest per inittime that are processed concurrently
            # * CEDA can have 10 connections per user, so we can download 10 files concurrently
            # * Hence download 4 initTimes in parallel
            with ProcessPoolExecutor(4) as p:
                dayPaths: list[list[pathlib.Path]] = [x for x in p.map(self.fetcher.downloadRawDataForInitTime, initTimes)]

            # Shutdown the pool after all files have downloaded
            p.shutdown(wait=True, cancel_futures=False)

            downloadedPaths.extend([x for y in dayPaths for x in y])

        return downloadedPaths

    def ConvertRawDatasetToZarr(self, startDate: dt.date, endDate: dt.date) -> list[pathlib.Path]:
        """Fetches a dataset for each initTime in the given time range and saves it as Zarr to the given store."""
        savedPaths: list[pathlib.Path] = []

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
                zarrPath: pathlib.Path = pathlib.Path(f"{initTime:%Y%m}.zarr")
                if not self.storer.existsInZarrDir(relativePath=zarrPath):
                    self.storer.saveDataset(dataset=initTimeDataset, relativePath=zarrPath)
                    savedPaths.append(zarrPath)
                else:
                    self.storer.saveDataset(dataset=initTimeDataset, relativePath=zarrPath)

                del initTimeDataset

        return savedPaths
