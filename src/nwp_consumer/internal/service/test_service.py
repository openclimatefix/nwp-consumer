import datetime as dt
import pathlib
import unittest

import numpy as np
import xarray as xr

from .. import FileInfoModel
from .. import models as internal
from . import NWPConsumerService

# Two days, four init times per day -> 8 init times
DAYS = [1, 2]
INIT_HOURS = [0, 6, 12, 18]
INIT_TIME_FILES = ["dswrf", "prate"]
testInitTimes = [dt.datetime(2021, 1, d, h, 0, 0, tzinfo=None)
                 for h in INIT_HOURS
                 for d in DAYS]


class DummyStorer(internal.StorageInterface):

    def exists(self, *, dst: pathlib.Path) -> bool:
        if "exists" in dst.name:
            return True
        return False

    def store(self, *, src: pathlib.Path, dst: pathlib.Path) -> int:
        src.unlink(missing_ok=True)
        return len(dst.name)

    def listInitTimes(self, prefix: pathlib.Path) -> list[dt.datetime]:
        return testInitTimes

    def copyITFolderToTemp(self, *, prefix: pathlib.Path, it: dt.datetime) \
            -> tuple[dt.datetime, list[pathlib.Path]]:
        return it, [pathlib.Path(f'{it:%Y%m%d%H%M}/{f}')
                    for f in INIT_TIME_FILES
                    for it in testInitTimes]

    def delete(self, *, dst: pathlib.Path) -> None:
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
        raw_files = [
            DummyFileInfo(file, it)
            for file in INIT_TIME_FILES
            if it in testInitTimes
        ]
        return raw_files

    def downloadToTemp(self, *, fi: FileInfoModel) -> tuple[FileInfoModel, pathlib.Path]:
        return fi, pathlib.Path(f'{fi.initTime():%Y%m%d%H%M}/{fi.fname()}')

    def mapTemp(self, *, p: pathlib.Path) -> xr.Dataset:
        initTime = dt.datetime.strptime(p.parent.as_posix(), "%Y%m%d%H%M")
        name = p.name
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

    @classmethod
    def setUpClass(cls) -> None:
        testStorer = DummyStorer()
        testFetcher = DummyFetcher()

        cls.service = NWPConsumerService(
            fetcher=testFetcher,
            storer=testStorer,
            rawdir="raw",
            zarrdir="zarr"
        )

    def test_downloadRawDataset(self):

        startDate = testInitTimes[0].date()
        endDate = testInitTimes[-1].date()

        n = self.service.DownloadRawDataset(start=startDate, end=endDate)

        # 2 files per init time, all init times
        self.assertEqual(2 * len(INIT_HOURS) * (len(DAYS)) * len("xxxxx.grib"), n)

    def test_convertRawDataset(self):
        startDate = testInitTimes[0].date()
        endDate = testInitTimes[-1].date()

        n = self.service.ConvertRawDatasetToZarr(start=startDate, end=endDate)

        # 1 Dataset per init time, all init times per day, all days
        self.assertEqual(1 * len(INIT_HOURS) * (len(DAYS)) * len("YYYYMMDDHHMM.zarr.zip"), n)

    def test_createLatestZarr(self):

        n = self.service.CreateLatestZarr()
        self.assertEqual(len("latest.zarr.zip"), n)
