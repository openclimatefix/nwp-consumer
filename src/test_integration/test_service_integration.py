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
from nwp_consumer.internal import ZARR_FMTSTR, config, inputs, outputs, service


class TestNWPConsumerService_MetOffice(unittest.TestCase):
    """Integration tests for the NWPConsumerService class."""

    def setUp(self) -> None:
        storageClient = outputs.localfs.Client()
        mc = config.MetOfficeConfig()
        metOfficeClient = inputs.metoffice.Client(
            orderID=mc.METOFFICE_ORDER_ID,
            clientID=mc.METOFFICE_CLIENT_ID,
            clientSecret=mc.METOFFICE_CLIENT_SECRET,
        )

        self.rawdir = "data/me_raw"
        self.zarrdir = "data/me_zarr"

        self.testService = service.NWPConsumerService(
            fetcher=metOfficeClient,
            storer=storageClient,
            rawdir=self.rawdir,
            zarrdir=self.zarrdir,
        )

    def test_downloadAndConvertDataset(self):
        initTime: dt.date = dt.datetime.now().date()

        nbytes = self.testService.DownloadRawDataset(start=initTime, end=initTime)
        self.assertGreater(nbytes, 0)

        nbytes = self.testService.ConvertRawDatasetToZarr(start=initTime, end=initTime)
        self.assertGreater(nbytes, 0)

        for path in pathlib.Path(self.zarrdir).glob('*.zarr.zip'):

            ds = xr.open_zarr(store=f"zip::{path.as_posix()}")

            # The number of variables in the dataset depends on the order from MetOffice
            numVars = len(ds.coords["variable"].values)

            # Ensure the dimensions have the right sizes
            self.assertDictEqual(
                {"variable": numVars, 'init_time': 1,  "step": 13, "y": 639, "x": 455},
                dict(ds.dims.items())
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
        cc = config.CEDAConfig()
        cedaClient = inputs.ceda.Client(
            ftpUsername=cc.CEDA_FTP_USER,
            ftpPassword=cc.CEDA_FTP_PASS,
        )

        self.rawdir = 'data/cd_raw'
        self.zarrdir = 'data/cd_zarr'

        self.testService = service.NWPConsumerService(
            fetcher=cedaClient,
            storer=storageClient,
            rawdir=self.rawdir,
            zarrdir=self.zarrdir,
        )

    def test_downloadAndConvertDataset(self):
        initTime: dt.date = dt.date(year=2022, month=1, day=1)

        nbytes = self.testService.DownloadRawDataset(start=initTime, end=initTime)
        self.assertGreater(nbytes, 0)

        nbytes = self.testService.ConvertRawDatasetToZarr(start=initTime, end=initTime)
        self.assertGreater(nbytes, 0)

        for path in pathlib.Path(self.zarrdir).glob('*.zarr.zip'):
            ds = xr.open_zarr(store=f"zip::{path.as_posix()}").compute()

            # Enusre the data variables are correct
            self.assertEqual(["UKV"], list(ds.data_vars))
            # Ensure the dimensions have the right sizes
            self.assertEqual(
                {'variable': 12,  'init_time': 1, 'step': 37, 'y': 704, 'x': 548},
                dict(ds.dims.items())
            )
            # Ensure the init time is correct
            self.assertEqual(
                np.datetime64(dt.datetime.strptime(path.with_suffix('').stem, ZARR_FMTSTR)),
                ds.coords["init_time"].values[0]
            )

        shutil.rmtree(self.rawdir)
        shutil.rmtree(self.zarrdir)


class TestNWPConverterService_ECMWFMARS(unittest.TestCase):
    def setUp(self):
        storageClient = outputs.localfs.Client()
        c = config.ECMWFMARSConfig()
        ecmwfMarsClient = inputs.ecmwf.MARSClient(
            area=c.ECMWF_AREA,
        )

        self.rawdir = 'data/ec_raw'
        self.zarrdir = 'data/ec_zarr'

        self.testService = service.NWPConsumerService(
            fetcher=ecmwfMarsClient,
            storer=storageClient,
            rawdir=self.rawdir,
            zarrdir=self.zarrdir,
        )

    def test_downloadAndConvertDataset(self):
        initTime: dt.date = dt.date(year=2022, month=1, day=1)

        nbytes = self.testService.DownloadRawDataset(start=initTime, end=initTime)
        self.assertGreater(nbytes, 0)

        nbytes = self.testService.ConvertRawDatasetToZarr(start=initTime, end=initTime)
        self.assertGreater(nbytes, 0)

        for path in pathlib.Path(self.zarrdir).glob('*.zarr.zip'):
            ds = xr.open_zarr(store=f"zip::{path.as_posix()}").compute()

            # Enusre the data variables are correct
            self.assertEqual(["UKV"], list(ds.data_vars))
            # Ensure the dimensions have the right sizes
            self.assertEqual(
                {'variable': 17, 'init_time': 1, 'step': 49, 'latitude': 241, 'longitude': 301},
                dict(ds.dims.items())
            )
            # Ensure the init time is correct
            self.assertEqual(
                np.datetime64(dt.datetime.strptime(path.with_suffix('').stem, ZARR_FMTSTR)),
                ds.coords["init_time"].values[0]
            )

        shutil.rmtree(self.rawdir)
        shutil.rmtree(self.zarrdir)

