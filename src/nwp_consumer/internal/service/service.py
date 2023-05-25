import datetime as dt
import pathlib
from concurrent.futures import ProcessPoolExecutor as PoolExecutor
import concurrent.futures
import xarray as xr
import pandas as pd
import numpy as np
import structlog
import itertools
import tensorstore as ts

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
        # TODO
        raise NotImplementedError()

        return []
