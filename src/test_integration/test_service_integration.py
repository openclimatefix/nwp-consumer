"""Integration tests for the NWPConsumerService class.

WARNING: Requires environment variables to be set for the MetOffice and CEDA APIs.
Will download up to a GB of data. Costs may apply for usage of the APIs.

Runs the main function of the consumer as it would appear externally imported
"""

import datetime as dt
import os
import shutil
import unittest
import unittest.mock

import numpy as np
import ocf_blosc2  # noqa: F401
import xarray as xr
from nwp_consumer.cmd.main import run


class TestNWPConsumerService_MetOffice(unittest.TestCase):
    """Integration tests for the NWPConsumerService class."""

    def setUp(self) -> None:
        self.rawdir = "data/me_raw"
        self.zarrdir = "data/me_zarr"

    def test_downloadAndConvertDataset(self) -> None:
        initTime: dt.datetime = dt.datetime.now(tz=dt.UTC)

        raw_files, zarr_files = run(
            [
                "consume",
                "--source=metoffice",
                "--rdir=" + self.rawdir,
                "--zdir=" + self.zarrdir,
                "--from=" + initTime.strftime("%Y-%m-%dT00:00"),
            ],
        )

        self.assertGreater(len(raw_files), 0)
        self.assertEqual(len(zarr_files), 1)

        for path in zarr_files:
            ds = xr.open_zarr(store=f"zip::{path.as_posix()}")

            # The number of variables in the dataset depends on the order from MetOffice
            numVars = len(ds.coords["variable"].values)

            # Ensure the dimensions have the right sizes
            self.assertDictEqual(
                {"variable": numVars, "init_time": 1, "step": 5, "y": 639, "x": 455},
                dict(ds.dims.items()),
            )
            # Ensure the dimensions of the variables are in the correct order
            self.assertEqual(("variable", "init_time", "step", "y", "x"), ds["UKV"].dims)
            # Ensure the init time is correct
            self.assertEqual(
                np.datetime64(initTime.strftime("%Y-%m-%dT00:00")),
                ds.coords["init_time"].values[0],
            )

        shutil.rmtree(self.rawdir)
        shutil.rmtree(self.zarrdir)


class TestNWPConsumerService_CEDA(unittest.TestCase):
    """Integration tests for the NWPConsumerService class."""

    def setUp(self) -> None:
        self.rawdir = "data/cd_raw"
        self.zarrdir = "data/cd_zarr"

    def test_downloadAndConvertDataset(self) -> None:
        raw_files, zarr_files = run(
            [
                "consume",
                "--source=ceda",
                "--rdir=" + self.rawdir,
                "--zdir=" + self.zarrdir,
                "--from=2022-01-01T12:00",
            ],
        )

        self.assertGreater(len(raw_files), 0)
        self.assertEqual(len(zarr_files), 1)

        for path in zarr_files:
            ds = xr.open_zarr(store=f"zip::{path.as_posix()}").compute()

            # Enusre the data variables are correct
            self.assertEqual(["UKV"], list(ds.data_vars))
            # Ensure the dimensions have the right sizes
            self.assertEqual(
                {"variable": 12, "init_time": 1, "step": 37, "y": 704, "x": 548},
                dict(ds.dims.items()),
            )
            # Ensure the init time is correct
            self.assertEqual(
                np.datetime64("2022-01-01T12:00"),
                ds.coords["init_time"].values[0],
            )

        shutil.rmtree(self.rawdir)
        shutil.rmtree(self.zarrdir)


class TestNWPConverterService_ECMWFMARS(unittest.TestCase):
    def setUp(self) -> None:
        self.rawdir = "data/ec_raw"
        self.zarrdir = "data/ec_zarr"

    @unittest.mock.patch.dict(os.environ, {"ECMWF_PARAMETER_GROUP": "basic", "ECMWF_HOURS": "3"})
    def test_downloadAndConvertDataset(self) -> None:
        initTime: dt.datetime = dt.datetime(year=2022, month=1, day=1, tzinfo=dt.UTC)

        raw_files, zarr_files = run(
            [
                "consume",
                "--source=ecmwf-mars",
                "--rdir=" + self.rawdir,
                "--zdir=" + self.zarrdir,
                "--from=" + initTime.strftime("%Y-%m-%dT00:00"),
            ],
        )

        self.assertGreater(len(raw_files), 0)
        self.assertEqual(len(zarr_files), 1)

        for path in zarr_files:
            ds = xr.open_zarr(store=f"zip::{path.as_posix()}").compute()

            # Ensure the data variables are correct
            self.assertEqual(["ECMWF_UK"], list(ds.data_vars))
            # Ensure the dimensions have the right sizes.
            # * Should be two variables due to the "basic" parameter group
            # * Should be 4 steps due to the "3" hours
            self.assertEqual(
                {
                    "variable": 2,
                    "init_time": 1,
                    "step": 4,
                    "latitude": 241,
                    "longitude": 301,
                },
                dict(ds.dims.items()),
            )
            # Ensure the init time is correct
            self.assertEqual(
                np.datetime64(initTime.strftime("%Y-%m-%dT00:00")),
                ds.coords["init_time"].values[0],
            )

        shutil.rmtree(self.rawdir)
        shutil.rmtree(self.zarrdir)


class TestNWPConsumerService_ICON(unittest.TestCase):
    """Integration tests for the NWPConsumerService class."""

    def setUp(self) -> None:
        self.rawdir = "data/ic_raw"
        self.zarrdir = "data/ic_zarr"

    @unittest.mock.patch.dict(os.environ, {"ICON_PARAMETER_GROUP": "basic", "ICON_HOURS": "3"})
    def test_downloadAndConvertDataset(self) -> None:
        initTime: dt.datetime = dt.datetime.now(tz=dt.UTC)

        raw_files, zarr_files = run(
            [
                "consume",
                "--source=icon",
                "--rdir=" + self.rawdir,
                "--zdir=" + self.zarrdir,
                "--from=" + initTime.strftime("%Y-%m-%dT00:00"),
            ],
        )

        self.assertGreater(len(raw_files), 0)
        self.assertEqual(len(zarr_files), 1)

        for path in zarr_files:
            ds = xr.open_zarr(store=f"zip::{path.as_posix()}").compute()

            # Enusre the data variables are correct
            self.assertEqual(["ICON_EUROPE"], list(ds.data_vars))
            # Ensure the dimensions have the right sizes
            # * Should be two variables due to the "basic" parameter group
            # * Should be 4 steps due to the "3" hours
            self.assertEqual(
                {"variable": 2, "init_time": 1, "step": 4, "latitude": 657, "longitude": 1377},
                dict(ds.dims.items()),
            )
            # Ensure the init time is correct
            self.assertEqual(
                np.datetime64(initTime.strftime("%Y-%m-%dT00:00")),
                ds.coords["init_time"].values[0],
            )

        shutil.rmtree(self.rawdir)
        shutil.rmtree(self.zarrdir)
