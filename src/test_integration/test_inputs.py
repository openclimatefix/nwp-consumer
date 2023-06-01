"""Integration tests for the `inputs` module."""

import datetime as dt
import unittest

from nwp_consumer.internal import config
from nwp_consumer.internal.inputs import ceda, metoffice
from nwp_consumer.internal.inputs.ceda._models import CEDAFileInfo
from nwp_consumer.internal.inputs.metoffice._models import MetOfficeFileInfo

cedaInitTime: dt.datetime = dt.datetime(year=2022, month=1, day=1, hour=0, minute=0, tzinfo=dt.timezone.utc)
metOfficeInitTime: dt.datetime = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

cc = config.CEDAConfig()
lc = config.LocalFSConfig()
mc = config.MetOfficeConfig()
cedaClient = ceda.CEDAClient(
    ftpUsername=cc.CEDA_FTP_USER,
    ftpPassword=cc.CEDA_FTP_PASS,
)

metOfficeClient = metoffice.MetOfficeClient(
    orderID=mc.METOFFICE_ORDER_ID,
    clientID=mc.METOFFICE_CLIENT_ID,
    clientSecret=mc.METOFFICE_CLIENT_SECRET,
)


class TestFetchRawFileBytes(unittest.TestCase):

    def test_downloadsRawGribFileFromCEDA(self):
        fileInfo = CEDAFileInfo(name="202201010000_u1096_ng_umqv_Wholesale1.grib")
        outBytes = cedaClient.fetchRawFileBytes(fileInfo=fileInfo)
        self.assertTrue(len(outBytes) > 100000000)

    def test_downloadsRawGribFileFromMetOffice(self):
        fileInfo = MetOfficeFileInfo(
            fileId=f'agl_temperature_1.5_{dt.datetime.now().strftime("%Y%m%d")}00',
            runDateTime=metOfficeInitTime
        )
        outBytes = metOfficeClient.fetchRawFileBytes(fileInfo=fileInfo)
        self.assertTrue(len(outBytes) > 50000000)


class TestListRawFilesForInitTime(unittest.TestCase):

    def test_getsFileInfosFromCEDA(self):
        fileInfos = cedaClient.listRawFilesForInitTime(initTime=cedaInitTime)
        self.assertTrue(len(fileInfos) > 0)

    def test_getsFileInfosFromMetOffice(self):
        fileInfos = metOfficeClient.listRawFilesForInitTime(initTime=metOfficeInitTime)
        self.assertTrue(len(fileInfos) > 0)


if __name__ == '__main__':
    unittest.main()
