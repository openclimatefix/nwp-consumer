"""Integration tests for the `inputs` module.

WARNING: Requires environment variables to be set for the MetOffice and CEDA APIs.
Will download up to a GB of data. Costs may apply for usage of the APIs.
"""

import datetime as dt
import unittest

from nwp_consumer.internal import config
from nwp_consumer.internal import inputs, outputs
from nwp_consumer.internal.inputs.ceda._models import CEDAFileInfo
from nwp_consumer.internal.inputs.metoffice._models import MetOfficeFileInfo

cedaInitTime: dt.datetime = dt.datetime(year=2022, month=1, day=1, hour=0, minute=0, tzinfo=None)
metOfficeInitTime: dt.datetime = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

cc = config.CEDAConfig()
mc = config.MetOfficeConfig()
cedaClient = inputs.ceda.CEDAClient(
    ftpUsername=cc.CEDA_FTP_USER,
    ftpPassword=cc.CEDA_FTP_PASS,
)
metOfficeClient = inputs.metoffice.MetOfficeClient(
    orderID=mc.METOFFICE_ORDER_ID,
    clientID=mc.METOFFICE_CLIENT_ID,
    clientSecret=mc.METOFFICE_CLIENT_SECRET,
)
storageClient = outputs.localfs.LocalFSClient(
    rawDir='data/raw',
    zarrDir='data/zarr',
)


class TestClient_FetchRawFileBytes(unittest.TestCase):

    def test_downloadsRawGribFileFromCEDA(self):
        fileInfo = CEDAFileInfo(name="202201010000_u1096_ng_umqv_Wholesale1.grib")
        _, outBytes = cedaClient.fetchRawFileBytes(fileInfo=fileInfo)

        self.assertGreater(len(outBytes), 100000000)

    def test_downloadsRawGribFileFromMetOffice(self):
        fileInfo = MetOfficeFileInfo(
            fileId=f'agl_temperature_1.5_{dt.datetime.now().strftime("%Y%m%d")}00',
            runDateTime=metOfficeInitTime
        )
        _, outBytes = metOfficeClient.fetchRawFileBytes(fileInfo=fileInfo)
        self.assertGreater(len(outBytes), 5000000)


class TestListRawFilesForInitTime(unittest.TestCase):

    def test_getsFileInfosFromCEDA(self):
        fileInfos = cedaClient.listRawFilesForInitTime(initTime=cedaInitTime)
        self.assertTrue(len(fileInfos) > 0)

    def test_getsFileInfosFromMetOffice(self):
        fileInfos = metOfficeClient.listRawFilesForInitTime(initTime=metOfficeInitTime)
        self.assertTrue(len(fileInfos) > 0)


if __name__ == '__main__':
    unittest.main()
