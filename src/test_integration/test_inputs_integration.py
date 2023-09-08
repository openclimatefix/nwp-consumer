"""Integration tests for the `inputs` module.

WARNING: Requires environment variables to be set for the MetOffice and CEDA APIs.
Will download up to a GB of data. Costs may apply for usage of the APIs.
"""

import datetime as dt
import unittest

from nwp_consumer.internal import config, inputs, outputs
from nwp_consumer.internal.inputs.ceda._models import CEDAFileInfo
from nwp_consumer.internal.inputs.ecmwf._models import ECMWFMarsFileInfo
from nwp_consumer.internal.inputs.metoffice._models import MetOfficeFileInfo

storageClient = outputs.localfs.Client()


class TestClient_FetchRawFileBytes(unittest.TestCase):

    def test_downloadsRawGribFileFromCEDA(self):
        c = config.CEDAConfig()
        cedaClient = inputs.ceda.Client(
            ftpUsername=c.CEDA_FTP_USER,
            ftpPassword=c.CEDA_FTP_PASS,
        )

        fileInfo = CEDAFileInfo(name="202201010000_u1096_ng_umqv_Wholesale1.grib")
        _, tmpPath = cedaClient.downloadToTemp(fi=fileInfo)

        self.assertGreater(tmpPath.stat().st_size, 100000000)

    def test_downloadsRawGribFileFromMetOffice(self):
        metOfficeInitTime: dt.datetime = dt.datetime.now() \
            .replace(hour=0, minute=0, second=0, microsecond=0)

        c = config.MetOfficeConfig()
        metOfficeClient = inputs.metoffice.Client(
            orderID=c.METOFFICE_ORDER_ID,
            clientID=c.METOFFICE_CLIENT_ID,
            clientSecret=c.METOFFICE_CLIENT_SECRET,
        )
        fileInfo = MetOfficeFileInfo(
            fileId=f'agl_temperature_1.5_{dt.datetime.now().strftime("%Y%m%d")}00',
            runDateTime=metOfficeInitTime
        )
        _, tmpPath = metOfficeClient.downloadToTemp(fi=fileInfo)
        self.assertGreater(tmpPath.stat().st_size, 4000000)

    def test_downloadsRawGribFileFromECMWFMARS(self):
        ecmwfMarsInitTime: dt.datetime = dt.datetime(
            year=2022, month=1, day=1, hour=0, minute=0, tzinfo=None
        )

        c = config.ECMWFMARSConfig()
        ecmwfMarsClient = inputs.ecmwf.MARSClient(
            area=c.ECMWF_AREA,
        )
        fileInfo = ECMWFMarsFileInfo(
            inittime=ecmwfMarsInitTime,
            area=c.ECMWF_AREA,
        )
        _, tmpPath = ecmwfMarsClient.downloadToTemp(fi=fileInfo)
        self.assertGreater(tmpPath.stat().st_size, 4000000)


class TestListRawFilesForInitTime(unittest.TestCase):

    def test_getsFileInfosFromCEDA(self):
        cedaInitTime: dt.datetime = dt.datetime(
            year=2022, month=1, day=1, hour=0, minute=0, tzinfo=None
        )
        c = config.CEDAConfig()
        cedaClient = inputs.ceda.Client(
            ftpUsername=c.CEDA_FTP_USER,
            ftpPassword=c.CEDA_FTP_PASS,
        )
        fileInfos = cedaClient.listRawFilesForInitTime(it=cedaInitTime)
        self.assertTrue(len(fileInfos) > 0)

    def test_getsFileInfosFromMetOffice(self):
        metOfficeInitTime: dt.datetime = dt.datetime.now() \
            .replace(hour=0, minute=0, second=0, microsecond=0)
        c = config.MetOfficeConfig()
        metOfficeClient = inputs.metoffice.Client(
            orderID=c.METOFFICE_ORDER_ID,
            clientID=c.METOFFICE_CLIENT_ID,
            clientSecret=c.METOFFICE_CLIENT_SECRET,
        )
        fileInfos = metOfficeClient.listRawFilesForInitTime(it=metOfficeInitTime)
        self.assertTrue(len(fileInfos) > 0)

    def test_getsFileInfosFromECMWFMARS(self):
        ecmwfMarsInitTime: dt.datetime = dt.datetime(
            year=2022, month=1, day=1, hour=0, minute=0, tzinfo=None
        )
        c = config.ECMWFMARSConfig()
        ecmwfMarsClient = inputs.ecmwf.MARSClient(
            area=c.ECMWF_AREA,
        )
        fileInfos = ecmwfMarsClient.listRawFilesForInitTime(it=ecmwfMarsInitTime)
        self.assertTrue(len(fileInfos) > 0)


if __name__ == '__main__':
    unittest.main()
