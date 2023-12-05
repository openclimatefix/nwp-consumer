import datetime as dt
import pathlib
import shutil
import unittest

import numpy as np
import structlog
import xarray as xr

from .. import FileInfoModel
from .. import models as internal
from .service import NWPConsumerService, _saveAsTempZipZarr

# Two days, four init times per day -> 8 init times
DAYS = [1, 2]
INIT_HOURS = [0, 6, 12, 18]
INIT_TIME_FILES = ["dswrf.grib", "prate.grib"]
testInitTimes = [
    dt.datetime(2021, 1, d, h, 0, 0, tzinfo=dt.timezone.utc) for h in INIT_HOURS for d in DAYS
]

log = structlog.getLogger()


class DummyStorer(internal.StorageInterface):
    def exists(self, *, dst: pathlib.Path) -> bool:
        log.info("exists", dst=dst)
        if "exists" in dst.name:
            return True
        return False

    def store(self, *, src: pathlib.Path, dst: pathlib.Path) -> pathlib.Path:
        if src.exists():
            if src.is_dir():
                shutil.rmtree(src.as_posix(), ignore_errors=True)
            else:
                src.unlink(missing_ok=True)
        return dst

    def listInitTimes(self, prefix: pathlib.Path) -> list[dt.datetime]:
        log.info("listInitTimes", prefix=prefix)
        return testInitTimes

    def copyITFolderToTemp(self, *, prefix: pathlib.Path, it: dt.datetime) -> list[pathlib.Path]:
        log.info("copyITFolderToTemp", prefix=prefix, it=it)
        return [pathlib.Path(f"{it:%Y%m%d%H%M}/{f}.grib") for f in INIT_TIME_FILES]

    def delete(self, *, p: pathlib.Path) -> None:
        if p.exists():
            if p.is_dir():
                shutil.rmtree(p.as_posix(), ignore_errors=True)
            else:
                p.unlink(missing_ok=True)


class DummyFileInfo(internal.FileInfoModel):
    def __init__(self, fileName: str, initTime: dt.datetime):
        self.f = fileName
        self.t = initTime

    def filename(self) -> str:
        return self.f

    def it(self) -> dt.datetime:
        return self.t

    def filepath(self) -> str:
        return self.f


class DummyFetcher(internal.FetcherInterface):
    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[FileInfoModel]:
        raw_files = [DummyFileInfo(file, it) for file in INIT_TIME_FILES if it in testInitTimes]
        return raw_files

    def downloadToTemp(self, *, fi: FileInfoModel) -> tuple[FileInfoModel, pathlib.Path]:
        return fi, pathlib.Path(f"{fi.it():%Y%m%d%H%M}/{fi.filename()}")

    def mapTemp(self, *, p: pathlib.Path) -> xr.Dataset:
        initTime = dt.datetime.strptime(p.parent.as_posix(), "%Y%m%d%H%M").replace(
            tzinfo=dt.timezone.utc,
        )
        return xr.Dataset(
            data_vars={
                "UKV": (
                    ("init_time", "variable", "step", "x", "y"),
                    np.random.rand(1, 1, 12, 100, 100),
                ),
            },
            coords={
                "init_time": [np.datetime64(initTime, "s")],
                "variable": [p.name],
                "step": range(12),
                "x": range(100),
                "y": range(100),
            },
        )


# ------------- Client Methods -------------- #


class TestNWPConsumerService(unittest.TestCase):
    service: NWPConsumerService

    @classmethod
    def setUpClass(cls) -> None:
        testStorer = DummyStorer()
        testFetcher = DummyFetcher()

        cls.service = NWPConsumerService(
            fetcher=testFetcher,
            storer=testStorer,
            rawdir="raw",
            zarrdir="zarr",
        )

    def test_downloadRawDataset(self):
        startDate = testInitTimes[0].date()
        endDate = testInitTimes[-1].date()

        files = self.service.DownloadRawDataset(start=startDate, end=endDate)

        # 2 files per init time, all init times
        self.assertEqual(2 * len(INIT_HOURS) * (len(DAYS)), len(files))

    def test_convertRawDataset(self):
        startDate = testInitTimes[0].date()
        endDate = testInitTimes[-1].date()

        files = self.service.ConvertRawDatasetToZarr(start=startDate, end=endDate)

        # 1 Dataset per init time, all init times per day, all days
        self.assertEqual(1 * len(INIT_HOURS) * (len(DAYS)), len(files))

    def test_createLatestZarr(self):
        files = self.service.CreateLatestZarr()
        # 1 zarr, 1 zipped zarr
        self.assertEqual(2, len(files))


# ------------ Static Methods ----------- #


class TestSaveAsZippedZarr(unittest.TestCase):
    def test_createsValidZipZarr(self):
        ds = DummyFetcher().mapTemp(p=pathlib.Path("202101010000/dswrf.grib"))
        file = _saveAsTempZipZarr(ds=ds)
        outds = xr.open_zarr(f"zip::{file.as_posix()}")
        self.assertEqual(ds.dims, outds.dims)
