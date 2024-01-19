"""Integration tests for the NWPConsumerService class.

WARNING: Requires environment variables to be set for the MetOffice and CEDA APIs.
Will download up to a GB of data. Costs may apply for usage of the APIs.

Runs the main function of the consumer as it would appear externally imported
"""

import datetime as dt
import pathlib
import shutil
import unittest

import numpy as np
import ocf_blosc2  # noqa: F401
import xarray as xr
from nwp_consumer.cmd.main import run
from nwp_consumer.internal import ZARR_GLOBSTR, config, inputs, outputs, service


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
                "--set=basic",
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
            self.assertEqual(np.datetime64(initTime), ds.coords["init_time"].values[0])

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
                "--set=basic",
                "--rdir=" + self.rawdir,
                "--zdir=" + self.zarrdir,
                "--from=2022-01-01T00:00",
            ],
        )

        self.assertGreater(len(raw_files), 0)
        self.assertEqual(len(zarr_files), 1)

        for path in pathlib.Path(self.zarrdir).glob(ZARR_GLOBSTR + ".zarr.zip"):
            ds = xr.open_zarr(store=f"zip::{path.as_posix()}").compute()

            # Enusre the data variables are correct
            self.assertEqual(["UKV"], list(ds.data_vars))
            # Ensure the dimensions have the right sizes
            self.assertEqual(
                {"variable": 12, "init_time": 1, "step": 37, "y": 704, "x": 548},
                dict(ds.dims.items()),
            )
            # Ensure the init time is correct
            self.assertEqual(np.datetime64("2022-01-01"), ds.coords["init_time"].values[0])

        shutil.rmtree(self.rawdir)
        shutil.rmtree(self.zarrdir)


class TestNWPConverterService_ECMWFMARS(unittest.TestCase):
    def setUp(self) -> None:
        storageClient = outputs.localfs.Client()

        # Test downloading the basic parameter set for the UK model
        _ = config.ECMWFMARSEnv()
        ecmwfMarsClient = inputs.ecmwf.mars.Client(
            area="uk",
            hours=4,
            param_group="basic",
        )

        self.rawdir = "data/ec_raw"
        self.zarrdir = "data/ec_zarr"

        self.testService = service.NWPConsumerService(
            fetcher=ecmwfMarsClient,
            storer=storageClient,
            rawdir=self.rawdir,
            zarrdir=self.zarrdir,
        )

    def test_downloadAndConvertDataset(self) -> None:
        initTime: dt.datetime = dt.datetime(year=2022, month=1, day=1, tzinfo=dt.UTC)

        out = self.testService.DownloadRawDataset(start=initTime, end=initTime)
        self.assertGreater(len(out), 0)

        out = self.testService.ConvertRawDatasetToZarr(start=initTime, end=initTime)
        self.assertEqual(len(out), 1)

        for path in out:
            ds = xr.open_zarr(store=f"zip::{path.as_posix()}").compute()

            # Ensure the data variables are correct
            self.assertEqual(["ECMWF_UK"], list(ds.data_vars))
            # Ensure the dimensions have the right sizes
            self.assertEqual(
                {"variable": 2, "init_time": 1, "step": 5, "latitude": 241, "longitude": 301},
                dict(ds.dims.items()),
            )
            # Ensure the init time is correct
            self.assertEqual(
                str(np.datetime64(initTime))[:10],
                str(ds.coords["init_time"].values[0])[:10],
            )

        shutil.rmtree(self.rawdir)
        shutil.rmtree(self.zarrdir)


class TestNWPConsumerService_ICON(unittest.TestCase):
    """Integration tests for the NWPConsumerService class."""

    def setUp(self) -> None:
        storageClient = outputs.localfs.Client()

        # Test downloading the basic parameter set for the global model
        iconClient = inputs.icon.Client(
            model="global",
            hours=4,
            param_group="basic",
        )

        self.rawdir = "data/ic_raw"
        self.zarrdir = "data/ic_zarr"

        self.testService = service.NWPConsumerService(
            fetcher=iconClient,
            storer=storageClient,
            rawdir=self.rawdir,
            zarrdir=self.zarrdir,
        )

    def test_downloadAndConvertDataset(self) -> None:
        initTime: dt.datetime = dt.datetime.now(tz=dt.UTC).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

        out = self.testService.DownloadRawDataset(start=initTime, end=initTime)
        self.assertGreater(len(out), 0)

        out = self.testService.ConvertRawDatasetToZarr(start=initTime, end=initTime)
        self.assertEqual(len(out), 1)

        for path in out:
            ds = xr.open_zarr(store=f"zip::{path.as_posix()}").compute()

            # Enusre the data variables are correct
            self.assertEqual(["ICON_GLOBAL"], list(ds.data_vars))
            # Ensure the dimensions have the right sizes
            self.assertEqual(
                {"variable": 2, "init_time": 1, "step": 5, "values": 2949120},
                dict(ds.dims.items()),
            )
            # Ensure the init time is correct
            dt64: np.datetime64 = ds.coords["init_time"].values[0]
            it: dt.datetime = dt.datetime.fromtimestamp(dt64.astype(int) / 1e9, tz=dt.UTC)
            self.assertEqual(initTime, it)

        shutil.rmtree(self.rawdir)
        shutil.rmtree(self.zarrdir)
