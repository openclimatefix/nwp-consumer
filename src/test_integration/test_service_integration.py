"""Integration tests for the NWPConsumerService class.

WARNING: Requires environment variables to be set for the MetOffice and CEDA APIs.
Will download up to a GB of data. Costs may apply for usage of the APIs.
"""

import shutil
import unittest
import datetime as dt
import xarray as xr
import numpy as np

from nwp_consumer.internal import config, inputs, outputs, service


class TestNWPConsumerService_MetOffice(unittest.TestCase):

    def setUp(self) -> None:
        storageClient = outputs.localfs.LocalFSClient(
            rawDir='data/raw',
            zarrDir='data/zarr',
            createDirs=True,
        )
        mc = config.MetOfficeConfig()
        metOfficeClient = inputs.metoffice.MetOfficeClient(
            orderID=mc.METOFFICE_ORDER_ID,
            clientID=mc.METOFFICE_CLIENT_ID,
            clientSecret=mc.METOFFICE_CLIENT_SECRET,
        )

        self.testService = service.NWPConsumerService(
            fetcher=metOfficeClient,
            storer=storageClient
        )

    def test_downloadAndConvertDataset(self):
        initTime: dt.date = dt.datetime.now().date()

        paths = self.testService.DownloadRawDataset(startDate=initTime, endDate=initTime)
        self.assertGreater(len(paths), 0)

        paths = self.testService.ConvertRawDatasetToZarr(startDate=initTime, endDate=initTime)

        for path in paths:
            ds = xr.open_zarr(path)
            self.assertEqual(["UKV"], list(ds.data_vars))
            self.assertEqual(({"init_time": 1, "step": 13, "variable": 3, "y": 639, "x": 455}), ds.dims)
            self.assertEqual(np.datetime64(initTime), ds.coords["init_time"].values[0])

    def tearDown(self) -> None:
        shutil.rmtree('data/raw', ignore_errors=True)
        shutil.rmtree('data/zarr', ignore_errors=True)


class TestNWPConsumerService_CEDA(unittest.TestCase):

        def setUp(self) -> None:
            storageClient = outputs.localfs.LocalFSClient(
                rawDir='data/raw',
                zarrDir='data/zarr',
                createDirs=True,
            )
            cc = config.CEDAConfig()
            cedaClient = inputs.ceda.CEDAClient(
                ftpUsername=cc.CEDA_FTP_USER,
                ftpPassword=cc.CEDA_FTP_PASS,
            )

            self.testService = service.NWPConsumerService(
                fetcher=cedaClient,
                storer=storageClient
            )

        def test_downloadAndConvertDataset(self):
            initTime: dt.date = dt.date(year=2022, month=1, day=1)

            paths = self.testService.DownloadRawDataset(startDate=initTime, endDate=initTime)
            self.assertGreater(len(paths), 0)
            paths = self.testService.ConvertRawDatasetToZarr(startDate=initTime, endDate=initTime)

            for path in paths:
                ds = xr.open_zarr(path)
                self.assertEqual(["UKV"], list(ds.data_vars))
                self.assertEqual(({'init_time': 1, 'step': 37, 'variable': 12, 'y': 704, 'x': 548}), ds.dims)
                self.assertEqual(np.datetime64(initTime), ds.coords["init_time"].values[0])

        def tearDown(self) -> None:
            shutil.rmtree('data/raw', ignore_errors=True)
            shutil.rmtree('data/zarr', ignore_errors=True)
