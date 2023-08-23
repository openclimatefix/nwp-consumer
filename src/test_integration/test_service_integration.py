"""Integration tests for the NWPConsumerService class.

WARNING: Requires environment variables to be set for the MetOffice and CEDA APIs.
Will download up to a GB of data. Costs may apply for usage of the APIs.
"""

import datetime as dt
import os
import pathlib
import shutil
import unittest

import numpy as np
import ocf_blosc2  # noqa: F401
import xarray as xr

from nwp_consumer.internal import config, inputs, outputs, service, ZARR_FMTSTR


@unittest.skipIf(os.environ.get('CI') is True, "Skip test on CI.")
class TestNWPConsumerService_MetOffice(unittest.TestCase):
    """Integration tests for the NWPConsumerService class."""

    def setUp(self) -> None:
        storageClient = outputs.localfs.LocalFSClient()
        mc = config.MetOfficeConfig()
        metOfficeClient = inputs.metoffice.MetOfficeClient(
            orderID=mc.METOFFICE_ORDER_ID,
            clientID=mc.METOFFICE_CLIENT_ID,
            clientSecret=mc.METOFFICE_CLIENT_SECRET,
        )

        self.testService = service.NWPConsumerService(
            fetcher=metOfficeClient,
            storer=storageClient,
            rawdir='data/raw',
            zarrdir='data/zarr',
        )

    def test_downloadAndConvertDataset(self):
        initTime: dt.date = dt.datetime.now().date()

        nbytes = self.testService.DownloadRawDataset(start=initTime, end=initTime)
        self.assertGreater(nbytes, 0)

        nbytes = self.testService.ConvertRawDatasetToZarr(start=initTime, end=initTime)
        self.assertGreater(nbytes, 0)

        for path in pathlib.Path('data/zarr').glob('*.zarr.zip'):

            ds = xr.open_zarr(store=f"zip::{path.as_posix()}")

            # The number of variables in the dataset depends on the order from MetOffice
            numVars = len(ds.coords["variable"].values)

            # Ensure the dimensions have the right sizes
            self.assertDictEqual({"init_time": 1, "step": 13, "variable": numVars, "y": 639, "x": 455}, dict(ds.dims.items()))
            # Ensure the dimensions of the variables are in the correct order
            self.assertEqual(("init_time", "step", "variable", "y", "x"), ds["UKV"].dims)
            # Ensure the init time is correct
            self.assertEqual(np.datetime64(initTime), ds.coords["init_time"].values[0])

    def tearDown(self) -> None:
        pass
        shutil.rmtree('data/raw', ignore_errors=True)
        shutil.rmtree('data/zarr', ignore_errors=True)


class TestNWPConsumerService_CEDA(unittest.TestCase):
    """Integration tests for the NWPConsumerService class."""

    def setUp(self) -> None:
        storageClient = outputs.localfs.LocalFSClient()
        cc = config.CEDAConfig()
        cedaClient = inputs.ceda.CEDAClient(
            ftpUsername=cc.CEDA_FTP_USER,
            ftpPassword=cc.CEDA_FTP_PASS,
        )

        self.testService = service.NWPConsumerService(
            fetcher=cedaClient,
            storer=storageClient,
            rawdir='data/raw',
            zarrdir='data/zarr',
        )

    def test_downloadAndConvertDataset(self):
        initTime: dt.date = dt.date(year=2022, month=1, day=1)

        nbytes = self.testService.DownloadRawDataset(start=initTime, end=initTime)
        self.assertGreater(nbytes, 0)

        nbytes = self.testService.ConvertRawDatasetToZarr(start=initTime, end=initTime)
        self.assertGreater(nbytes, 0)

        for path in pathlib.Path('data/zarr').glob('*.zarr.zip'):
            ds = xr.open_zarr(store=f"zip::{path.as_posix()}").compute()

            # Enusre the data variables are correct
            self.assertEqual(["UKV"], list(ds.data_vars))
            # Ensure the dimensions have the right sizes
            self.assertEqual({'init_time': 1, 'step': 37, 'variable': 12, 'y': 704, 'x': 548}, dict(ds.dims.items()))
            # Ensure the init time is correct
            self.assertEqual(
                np.datetime64(dt.datetime.strptime(path.with_suffix('').stem, ZARR_FMTSTR)),
                ds.coords["init_time"].values[0]
            )

    def tearDown(self) -> None:
        pass
        shutil.rmtree('data/raw', ignore_errors=True)
        shutil.rmtree('data/zarr', ignore_errors=True)
