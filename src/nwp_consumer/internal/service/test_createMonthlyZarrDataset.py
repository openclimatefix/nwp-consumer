import datetime as dt
import pathlib
import shutil
import unittest

import numpy as np
import xarray as xr

from src.nwp_consumer import internal
from src.nwp_consumer.internal.outputs import localfs

from .monthlyZarrDataset import CreateMonthlyZarrDataset


class DummyFetcher(internal.FetcherInterface):
    def getDatasetForInitTime(self, initTime: dt.datetime) -> xr.Dataset:
        return xr.Dataset(
            data_vars={
                "vis": (["init_time", "step", "x", "y"], np.random.rand(1, 3, 100, 100)),
                "t": (["init_time", "step", "x", "y"], np.random.rand(1, 3, 100, 100)),
            },
            coords={
                "init_time": [np.datetime64(initTime)],
                "step_time": np.arange(np.datetime64(initTime), np.datetime64(initTime + dt.timedelta(hours=4)), dt.timedelta(hours=1)).astype(np.datetime64),
                "x": np.arange(100),
                "y": np.arange(100),
            }
        )

    def loadSingleParameterGRIBAsOCFDataArray(self, path: pathlib.Path, initTime: dt.datetime) -> xr.DataArray:
        return xr.DataArray()


class TestCreateMonthlyZarrDataset(unittest.TestCase):

    def setUp(self):
        # Create a directory to store the zarr files
        self.zarrDir: pathlib.Path = pathlib.Path(__file__).parent / "zarr"
        self.zarrDir.mkdir(exist_ok=True)

    def test_createsDatasetPerMonth(self):

        CreateMonthlyZarrDataset(
            fetcher=DummyFetcher(),
            storer=localfs.LocalFSClient(
                rawDir="",
                zarrDir=self.zarrDir.as_posix(),
                createDirs=True,
            ),
            startDate=dt.date(2021, 1, 1),
            endDate=dt.date(2021, 3, 1),
        )

        # Check that there is a zarr file for each month between the start and end dates
        self.assertTrue((pathlib.Path(__file__).parent / "zarr" / "UKV-202101.zarr").exists())
        self.assertTrue((pathlib.Path(__file__).parent / "zarr" / "UKV-202102.zarr").exists())
        self.assertTrue((pathlib.Path(__file__).parent / "zarr" / "UKV-202103.zarr").exists())

    def tearDown(self) -> None:
        # Remove the zarr directory and all files in it
        shutil.rmtree(self.zarrDir)
