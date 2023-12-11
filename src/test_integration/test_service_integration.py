"""Integration tests for the NWPConsumerService class.

WARNING: Requires environment variables to be set for the MetOffice and CEDA APIs.
Will download up to a GB of data. Costs may apply for usage of the APIs.
"""

import datetime as dt
import pathlib
import shutil
import unittest

import numpy as np
import ocf_blosc2  # noqa: F401
import xarray as xr
from nwp_consumer.internal import ZARR_GLOBSTR, config, inputs, outputs, service


class TestNWPConsumerService_MetOffice(unittest.TestCase):
    """Integration tests for the NWPConsumerService class."""

    def setUp(self) -> None:
        storageClient = outputs.localfs.Client()
        env = config.MetOfficeEnv()
        metOfficeClient = inputs.metoffice.Client(
            orderID=env.METOFFICE_ORDER_ID,
            clientID=env.METOFFICE_CLIENT_ID,
            clientSecret=env.METOFFICE_CLIENT_SECRET,
        )

        self.rawdir = "data/me_raw"
        self.zarrdir = "data/me_zarr"

        self.testService = service.NWPConsumerService(
            fetcher=metOfficeClient,
            storer=storageClient,
            rawdir=self.rawdir,
            zarrdir=self.zarrdir,
        )

    def test_downloadAndConvertDataset(self) -> None:
        initTime: dt.date = dt.datetime.now(tz=dt.UTC).date()

        out = self.testService.DownloadRawDataset(start=initTime, end=initTime)
        self.assertGreater(len(out), 0)

        out = self.testService.ConvertRawDatasetToZarr(start=initTime, end=initTime)
        self.assertGreater(len(out), 0)

        for path in out:
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
        storageClient = outputs.localfs.Client()
        env = config.CEDAEnv()
        cedaClient = inputs.ceda.Client(
            ftpUsername=env.CEDA_FTP_USER,
            ftpPassword=env.CEDA_FTP_PASS,
        )

        self.rawdir = "data/cd_raw"
        self.zarrdir = "data/cd_zarr"

        self.testService = service.NWPConsumerService(
            fetcher=cedaClient,
            storer=storageClient,
            rawdir=self.rawdir,
            zarrdir=self.zarrdir,
        )

    def test_downloadAndConvertDataset(self) -> None:
        initTime: dt.date = dt.date(year=2022, month=1, day=1)

        out = self.testService.DownloadRawDataset(start=initTime, end=initTime)
        self.assertGreater(len(out), 0)

        out = self.testService.ConvertRawDatasetToZarr(start=initTime, end=initTime)
        self.assertGreater(len(out), 0)

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
            self.assertEqual(np.datetime64(initTime), ds.coords["init_time"].values[0])

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
        initTime: dt.date = dt.date(year=2022, month=1, day=1)

        out = self.testService.DownloadRawDataset(start=initTime, end=initTime)
        self.assertGreater(len(out), 0)

        out = self.testService.ConvertRawDatasetToZarr(start=initTime, end=initTime)
        self.assertGreater(len(out), 0)

        for path in out:
            ds = xr.open_zarr(store=f"zip::{path.as_posix()}").compute()

            # Enusre the data variables are correct
            self.assertEqual(["ECMWF_UK"], list(ds.data_vars))
            # Ensure the dimensions have the right sizes
            self.assertEqual(
                {"variable": 2, "init_time": 1, "step": 5, "latitude": 241, "longitude": 301},
                dict(ds.dims.items()),
            )
            # Ensure the init time is correct
            self.assertEqual(
                str(np.datetime64(initTime))[:10], str(ds.coords["init_time"].values[0])[:10],
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
        initTime: dt.date = dt.datetime.now(tz=dt.UTC).date()

        out = self.testService.DownloadRawDataset(start=initTime, end=initTime)
        #  self.assertGreater(len(out), 0)

        out = self.testService.ConvertRawDatasetToZarr(start=initTime, end=initTime)
        self.assertGreater(len(out), 0)

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
            self.assertEqual(initTime, it.date())

        shutil.rmtree(self.rawdir)
        shutil.rmtree(self.zarrdir)
