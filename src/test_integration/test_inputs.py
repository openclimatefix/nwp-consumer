"""Integration tests for the `inputs` module."""

import datetime as dt
import unittest

from nwp_consumer.internal import config
from nwp_consumer.internal.inputs import ceda, metoffice
from nwp_consumer.internal.inputs.ceda._models import CEDAFileInfo
from nwp_consumer.internal.inputs.metoffice._models import MetOfficeFileInfo
from nwp_consumer.internal.outputs import localfs

cedaInitTime: dt.datetime = dt.datetime(year=2022, month=1, day=1, hour=0, minute=0, tzinfo=dt.timezone.utc)
metOfficeInitTime: dt.datetime = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

cc = config.CEDAConfig()
lc = config.LocalFSConfig()
mc = config.MetOfficeConfig()
cedaClient = ceda.CEDAClient(
    ftpUsername=cc.CEDA_FTP_USER,
    ftpPassword=cc.CEDA_FTP_PASS,
    storer=localfs.LocalFSClient(
        rawDir=lc.RAW_DIR,
        zarrDir=lc.ZARR_DIR,
    )
)

metOfficeClient = metoffice.MetOfficeClient(
    orderID=mc.METOFFICE_ORDER_ID,
    clientID=mc.METOFFICE_CLIENT_ID,
    clientSecret=mc.METOFFICE_CLIENT_SECRET,
    storer=localfs.LocalFSClient(
        rawDir=lc.RAW_DIR,
        zarrDir=lc.ZARR_DIR,
    )
)


class Test_DownloadRawGribFile(unittest.TestCase):

    def test_downloadsRawGribFileFromCEDA(self):
        fileInfo = CEDAFileInfo(name="202201010000_u1096_ng_umqv_Wholesale1.grib")
        outPath = cedaClient._downloadRawGRIBFile(fileInfo=fileInfo)
        self.assertTrue(cedaClient.storer.existsInRawDir(outPath))
        cedaClient.storer.removeFromRawDir(outPath)

    def test_downloadsRawGribFileFromMetOffice(self):
        fileInfo = MetOfficeFileInfo(
            fileId=f'agl_temperature_1.5_{dt.datetime.now().strftime("%Y%m%d")}00',
            runDateTime=metOfficeInitTime
        )
        outPath = metOfficeClient._downloadRawGRIBFile(fileInfo=fileInfo)
        self.assertTrue(metOfficeClient.storer.existsInRawDir(outPath))
        metOfficeClient.storer.removeFromRawDir(outPath)


class Test_GetFileInfosForInitTime(unittest.TestCase):

    def test_getsFileInfosFromCEDA(self):
        fileInfos = cedaClient._getFileInfosForInitTime(initTime=cedaInitTime)
        self.assertTrue(len(fileInfos) > 0)

    def test_getsFileInfosFromMetOffice(self):
        fileInfos = metOfficeClient._getFileInfosForInitTime(initTime=metOfficeInitTime)
        self.assertTrue(len(fileInfos) > 0)


if __name__ == '__main__':
    unittest.main()
