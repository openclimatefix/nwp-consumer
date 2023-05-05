"""Integration tests for the `inputs` module."""

import datetime as dt
import unittest

from src.nwp_consumer.internal.inputs import ceda, metoffice
from src.nwp_consumer.internal.inputs.ceda._models import CEDAFileInfo
from src.nwp_consumer.internal.inputs.metoffice._models import MetOfficeFileInfo
from src.nwp_consumer.internal import config
from src.nwp_consumer.internal.outputs import localfs

cedaInitTime: dt.datetime = dt.datetime(year=2022, month=1, day=1, hour=0, minute=0, tzinfo=dt.timezone.utc)
metOfficeInitTime: dt.datetime = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

cedaClient = ceda.CEDAClient(
    ftpUsername=config.CEDAConfig().CEDA_FTP_USER,
    ftpPassword=config.CEDAConfig().CEDA_FTP_PASS,
    storer=localfs.LocalFSClient(
        rawDir=config.LocalFSConfig().RAW_DIR,
        zarrDir=config.LocalFSConfig().ZARR_DIR,
    )
)

metOfficeClient = metoffice.MetOfficeClient(
    orderID=config.MetOfficeConfig().METOFFICE_ORDER_ID,
    clientID=config.MetOfficeConfig().METOFFICE_CLIENT_ID,
    clientSecret=config.MetOfficeConfig().METOFFICE_CLIENT_SECRET,
    storer=localfs.LocalFSClient(
        rawDir=config.LocalFSConfig().RAW_DIR,
        zarrDir=config.LocalFSConfig().ZARR_DIR,
    )
)


class TestGetDatasetForInitTime(unittest.TestCase):

    def testGetsDatasetSuccessfullyFromCEDA(self):
        dataset = cedaClient.getDatasetForInitTime(initTime=cedaInitTime)
        print(dataset)

        self.assertTrue({"x", "y", "init_time", "step_time"}.issubset(set(dataset.coords)))
        self.assertTrue(dataset.sizes["step_time"] == 37)
        self.assertTrue(len(dataset.data_vars) > 0)
        print(dataset["sde"].mean())

    def testGetsDatasetSuccessfullyFromMetOffice(self):
        dataset = metOfficeClient.getDatasetForInitTime(initTime=metOfficeInitTime)
        print(dataset)

        self.assertTrue({"x", "y", "init_time", "step_time"}.issubset(set(dataset.coords)))
        self.assertTrue(dataset.sizes["step_time"] > 1)
        self.assertTrue(len(dataset.data_vars) > 0)


class Test_DownloadRawGribFile(unittest.TestCase):

    def test_downloadsRawGribFileFromCEDA(self):
        fileInfo = CEDAFileInfo(name="202201010000_u1096_ng_umqv_Wholesale1.grib")
        outPath = cedaClient._downloadRawGRIBFile(fileInfo=fileInfo)
        self.assertTrue(outPath.exists())
        outPath.unlink()

    def test_downloadsRawGribFileFromMetOffice(self):
        fileInfo = MetOfficeFileInfo(
            fileId=f'agl_temperature_1.5_{dt.datetime.now().strftime("%Y%m%d")}00',
            runDateTime=metOfficeInitTime
        )
        outPath = metOfficeClient._downloadRawGRIBFile(fileInfo=fileInfo)
        self.assertTrue(outPath.exists())
        outPath.unlink()


class Test_GetFileInfosForInitTime(unittest.TestCase):

    def test_getsFileInfosFromCEDA(self):
        fileInfos = cedaClient._getFileInfosForInitTime(initTime=cedaInitTime)
        self.assertTrue(len(fileInfos) > 0)

    def test_getsFileInfosFromMetOffice(self):
        fileInfos = metOfficeClient._getFileInfosForInitTime(initTime=metOfficeInitTime)
        self.assertTrue(len(fileInfos) > 0)


if __name__ == '__main__':
    unittest.main()
