import datetime as dt
import pathlib
import unittest

import xarray as xr

from .. import FileInfoModel
from .. import models as internal
from . import NWPConsumerService

# Two days, four init times per day -> 8 init times
DAYS = [1, 2]
INIT_HOURS = [0, 6, 12, 18]
INIT_TIME_FILES = ["file1", "file2"]
testInitTimes = [dt.datetime(2021, 1, d, h, 0, 0, tzinfo=None)
                 for h in INIT_HOURS
                 for d in DAYS]


class DummyStorer(internal.StorageInterface):

    def rawFileExistsForInitTime(self, name: str, it: dt.datetime) -> bool:
        if "exists" in name:
            return True
        return False

    def writeBytesToRawFile(
            self, name: str, it: dt.datetime, b: bytes) -> pathlib.Path:
        return pathlib.Path(
            f"{it.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING)}/{name}"
        )

    def listInitTimesInRawDir(self) -> list[dt.datetime]:
        return testInitTimes

    def zarrExistsForInitTime(self, name: str, it: dt.datetime) -> bool:
        if "exists" in name:
            return True
        return False

    def readRawFilesForInitTime(self, it: dt.datetime) -> tuple[dt.datetime, list[bytes]]:
        return it, [
            bytes(it.strftime("%Y%m%d%H%M"), "utf-8") for _ in INIT_TIME_FILES
        ]

    def writeDatasetAsZarr(
            self, name: str, it: dt.datetime, ds: xr.Dataset) -> pathlib.Path:
        return pathlib.Path(name)


class DummyFileInfo(internal.FileInfoModel):
    def __init__(self, fileName: str, initTime: dt.datetime):
        self.f = fileName
        self.it = initTime

    def fname(self) -> str:
        return self.f

    def initTime(self) -> dt.datetime:
        return self.it


class DummyFetcher(internal.FetcherInterface):

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[FileInfoModel]:
        return [
            DummyFileInfo(file, it)
            for file in INIT_TIME_FILES
            if it in testInitTimes
        ]

    def fetchRawFileBytes(self, *, fi: FileInfoModel) -> tuple[FileInfoModel, bytes]:
        return fi, bytes(fi.initTime().strftime("%Y%m%d%H%M"), "utf-8")

    def loadRawInitTimeDataAsOCFDataset(self, *, fbl: list[bytes]) -> xr.Dataset:
        initTime = dt.datetime.strptime(fbl[0].decode("utf-8"), "%Y%m%d%H%M")
        return xr.Dataset(
            data_vars={
                'wdir10': (('init_time', 'step', 'values'), [[[1, 2, 3, 4], [5, 6, 7, 8]]]),
                'prate': (('init_time', 'step', 'values'), [[[1, 2, 3, 4], [5, 6, 7, 8]]])
            },
            coords={
                'init_time': [initTime],
                'step': [0, 1],
            }
        )


class TestNWPConsumerService(unittest.TestCase):

    def setUp(self) -> None:
        self.testStorer = DummyStorer()
        self.testFetcher = DummyFetcher()

    def test_downloadRawDataset(self):
        service = NWPConsumerService(fetcher=self.testFetcher, storer=self.testStorer)

        startDate = testInitTimes[0].date()
        endDate = testInitTimes[-1].date()

        paths = service.DownloadRawDataset(start=startDate, end=endDate)

        # 2 files per init time, all init times except the last one
        self.assertEqual(2 * len(INIT_HOURS) * (len(DAYS) - 1), len(paths))

    def test_convertRawDataset(self):
        service = NWPConsumerService(fetcher=self.testFetcher, storer=self.testStorer)

        startDate = testInitTimes[0].date()
        endDate = testInitTimes[-1].date()

        paths = service.ConvertRawDatasetToZarr(start=startDate, end=endDate)

        # 1 Dataset per init time, all init times per day, all days
        self.assertEqual(1 * len(INIT_HOURS) * (len(DAYS)), len(paths))
