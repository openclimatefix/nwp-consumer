import datetime as dt
import pathlib
import shutil
import unittest

import numpy as np
import structlog
import xarray as xr

from nwp_consumer import internal

from .consumer import NWPConsumerService, _cacheAsZipZarr, _mergeDatasets

# Two days, four init times per day -> 8 init times
DAYS = [1, 2]
INIT_HOURS = [0, 6, 12, 18]
INIT_TIME_FILES = ["dswrf.grib", "prate.grib"]
testInitTimes = [dt.datetime(2021, 1, d, h, 0, 0, tzinfo=dt.UTC) for h in INIT_HOURS for d in DAYS]

log = structlog.getLogger()


class DummyStorer(internal.StorageInterface):
    def name(self) -> str:
        return "dummy"

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
        return testInitTimes

    def copyITFolderToCache(self, *, prefix: pathlib.Path, it: dt.datetime) -> list[pathlib.Path]:
        return [
            pathlib.Path(f"{internal.CACHE_DIR_RAW}/{it:%Y/%m/%d/%H%M}/{f}.grib")
            for f in INIT_TIME_FILES
        ]

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

    def variables(self) -> list[str]:
        raise NotImplementedError()

    def steps(self) -> list[int]:
        return list(range(100))


class DummyFetcher(internal.FetcherInterface):
    def getInitHours(self) -> list[int]:
        return INIT_HOURS

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:
        raw_files = [DummyFileInfo(file, it) for file in INIT_TIME_FILES if it in testInitTimes]
        return raw_files

    def downloadToCache(
        self,
        *,
        fi: internal.FileInfoModel,
    ) -> tuple[internal.FileInfoModel, pathlib.Path]:
        return fi, pathlib.Path(f"{internal.CACHE_DIR_RAW}/{fi.it():%Y/%m/%d/%H%M}/{fi.filename()}")

    def mapCachedRaw(self, *, p: pathlib.Path) -> xr.Dataset:
        initTime = dt.datetime.strptime(
            p.parent.relative_to(internal.CACHE_DIR_RAW).as_posix(),
            "%Y/%m/%d/%H%M",
        ).replace(tzinfo=dt.UTC)
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

    def test_downloadRawDataset(self) -> None:
        start = testInitTimes[0]
        end = testInitTimes[-1]

        files = self.service.DownloadRawDataset(start=start, end=end)

        # 2 files per init time, all init times except the last
        # one so none of the init time files for that one
        self.assertEqual((2 * len(INIT_HOURS) * len(DAYS)) - 1 * len(INIT_TIME_FILES), len(files))

    def test_convertRawDataset(self) -> None:
        start = testInitTimes[0]
        end = testInitTimes[-1]

        files = self.service.ConvertRawDatasetToZarr(start=start, end=end)

        # 1 Dataset per init time, all init times per day, all days
        self.assertEqual(1 * len(INIT_HOURS) * (len(DAYS)), len(files))

    def test_createLatestZarr(self) -> None:
        files = self.service.CreateLatestZarr()
        # 1 zarr, 1 zipped zarr
        self.assertEqual(2, len(files))


# ------------ Static Methods ----------- #


class TestCacheAsZipZarr(unittest.TestCase):
    def test_createsValidZipZarr(self) -> None:
        ds = DummyFetcher().mapCachedRaw(
            p=pathlib.Path(f"{internal.CACHE_DIR_RAW}/2021/01/01/0000/dswrf.grib"),
        )
        file = _cacheAsZipZarr(ds=ds)
        outds = xr.open_zarr(f"zip::{file.as_posix()}")
        self.assertEqual(ds.dims, outds.dims)


class TestMergeDatasets(unittest.TestCase):
    def test_mergeDifferrentDataVars(self) -> None:
        """Test merging datasets with different data variables.

        This targets a bug seen in merging large ICON datasets, whereby
        two datasets with different variables and number of steps would
        not merge correctly.

        """
        # Create a list of datasets
        # * Dataset1: 1 variable, 12 steps
        # * Dataset2: 1 variable, 96 steps
        datasets = [
            xr.Dataset(
                data_vars={
                    "vis": (
                        ("init_time", "step", "x", "y"),
                        np.random.rand(1, 96, 100, 100),
                    ),
                },
                coords={
                    "init_time": [np.datetime64("2021-01-01T00:00:00")],
                    "step": range(96),
                    "x": range(100),
                    "y": range(100),
                },
            ),
            xr.Dataset(
                data_vars={
                    "ahum_s": (
                        ("init_time", "step", "x", "y"),
                        np.random.rand(1, 1, 100, 100),
                    ),
                },
                coords={
                    "init_time": [np.datetime64("2021-01-01T06:00:00")],
                    "step": [1],
                    "x": range(100),
                    "y": range(100),
                },
            ),
        ]
        # Merge the datasets
        merged = _mergeDatasets(datasets)
        # Check the merged dataset
        self.assertListEqual(
            list(merged.data_vars),
            list(datasets[0].data_vars) + list(datasets[1].data_vars),
        )
        self.assertEqual(merged.attrs, datasets[0].attrs)

        # Then merge again with the next step of the second dataset
        datasets = [
            merged,
            xr.Dataset(
                data_vars={
                    "ahum_s": (
                        ("init_time", "step", "x", "y"),
                        np.random.rand(1, 1, 100, 100),
                    ),
                },
                coords={
                    "init_time": [np.datetime64("2021-01-01T06:00:00")],
                    "step": [2],
                    "x": range(100),
                    "y": range(100),
                },
            ),
        ]

        merged = _mergeDatasets(datasets)
        self.assertEqual(merged.sizes, datasets[0].sizes)
