import datetime as dt
import json
import pathlib
import random
import unittest

import numpy as np
import xarray as xr

from .. import FileInfoModel
from .. import models as internal
from . import NWPConsumerService

# Two days, four init times per day -> 8 init times
DAYS = [1, 2]
INIT_HOURS = [0, 6, 12, 18]
INIT_TIME_FILES = ["wdir10", "prate"]
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
            f"{it.strftime(internal.IT_FOLDER_FMTSTR)}/{name}"
        )

    def listInitTimesInRawDir(self) -> list[dt.datetime]:
        return testInitTimes

    def zarrExistsForInitTime(self, name: str, it: dt.datetime) -> bool:
        if "exists" in name:
            return True
        return False

    def readRawFilesForInitTime(self, it: dt.datetime) -> tuple[dt.datetime, list[bytes]]:
        return it, [
            json.dumps({"it": it.strftime("%Y%m%d%H%M"), "name": f}).encode("utf-8") for f in INIT_TIME_FILES
        ]

    def writeDatasetAsZarr(
            self, name: str, it: dt.datetime, ds: xr.Dataset) -> pathlib.Path:
        return pathlib.Path(name)

    def deleteZarrForInitTime(self, *, name: str, it: dt.datetime) -> None:
        pass


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
        return fi, bytes("testfile", "utf-8")

    def convertRawFileToDataset(self, *, b: bytes) -> xr.Dataset:
        data = json.loads(b.decode('utf-8'))
        initTime = dt.datetime.strptime(data["it"], "%Y%m%d%H%M")
        name = data["name"]
        return xr.Dataset(
            data_vars={
                'UKV': (('init_time', 'variable', 'step', 'x', 'y'), np.random.rand(1, 1, 12, 100, 100)),
            },
            coords={
                'init_time': [initTime],
                'variable': [name],
                'step': range(12),
                'x': range(100),
                'y': range(100),
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

        # 2 files per init time, all init times
        self.assertEqual(2 * len(INIT_HOURS) * (len(DAYS)), len(paths))

    def test_convertRawDataset(self):
        service = NWPConsumerService(fetcher=self.testFetcher, storer=self.testStorer)

        startDate = testInitTimes[0].date()
        endDate = testInitTimes[-1].date()

        paths = service.ConvertRawDatasetToZarr(start=startDate, end=endDate)

        # 1 Dataset per init time, all init times per day, all days
        self.assertEqual(1 * len(INIT_HOURS) * (len(DAYS)), len(paths))

    def test_createLatestZarr(self):
        service = NWPConsumerService(fetcher=self.testFetcher, storer=self.testStorer)

        path = service.CreateLatestZarr()
        self.assertEqual(pathlib.Path("latest.zarr"), path)
