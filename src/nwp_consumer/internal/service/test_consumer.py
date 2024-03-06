import datetime as dt
import pathlib
import unittest

import numpy as np
import structlog
import xarray as xr

from nwp_consumer import internal
from .consumer import NWPConsumerService, _cacheAsZipZarr, _mergeDatasets

log = structlog.getLogger()

IT = dt.datetime(2021, 1, 1, tzinfo=dt.UTC)
FILES = ["dswrf.grib", "prate.grib", "t2m.grib"]


class DummyStorer(internal.StorageInterface):
    def name(self) -> str:
        return "dummy"

    def exists(self, *, dst: pathlib.Path) -> bool:
        return True

    def store(self, *, src: pathlib.Path, dst: pathlib.Path) -> pathlib.Path:
        return dst

    def listInitTimes(self, prefix: pathlib.Path) -> list[dt.datetime]:
        return [IT]

    def copyITFolderToCache(self, *, prefix: pathlib.Path, it: dt.datetime) -> list[pathlib.Path]:
        return [
            pathlib.Path(internal.rawCachePath(it=it, filename=f))
            for f in FILES
        ]

    def delete(self, *, p: pathlib.Path) -> None:
        pass


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
        return [0, 6, 12, 18]

    def datasetName(self) -> str:
        return "dummy"

    def listRawFilesForInitTime(self, *, it: dt.datetime) -> list[internal.FileInfoModel]:
        return [DummyFileInfo(file, it) for file in FILES]

    def downloadToCache(self, *, fi: internal.FileInfoModel) -> pathlib.Path:
        return internal.rawCachePath(it=fi.it(), filename=fi.filename())

    def mapCachedRaw(self, *, p: pathlib.Path) -> xr.Dataset:
        initTime = dt.datetime.strptime(
            p.parent.relative_to(internal.CACHE_DIR_RAW).as_posix(),
            "%Y/%m/%d/%H%M",
        ).replace(tzinfo=dt.UTC)
        return xr.Dataset(
            data_vars={
                f"{p.stem}": (
                    ("init_time", "step", "x", "y"),
                    np.random.rand(1, 12, 100, 100),
                )
            },
            coords={
                "init_time": [np.datetime64(initTime)],
                "step": range(12),
                "x": range(100),
                "y": range(100),
            },
        )

    def parameterConformMap(self) -> dict[str, internal.OCFParameter]:
        return {
            "t2m": internal.OCFParameter.TemperatureAGL,
        }


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

    def test_downloadSingleInitTime(self) -> None:
        files = self.service._downloadSingleInitTime(it=IT)
        self.assertEqual(3, len(files))

    def test_convertSingleInitTime(self) -> None:
        files = self.service._convertSingleInitTime(it=IT)
        self.assertEqual(1, len(files))

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
