"""Integration tests for the `inputs` module."""

import unittest
import datetime as dt

from src.nwp_consumer.internal.inputs import ceda, metoffice
from src.nwp_consumer.internal.inputs.ceda._models import CEDAFileInfo
from src.nwp_consumer.internal.inputs.metoffice._models import MetOfficeFileInfo

cedaInitTime: dt.datetime = dt.datetime(year=2022, month=1, day=1, hour=0, minute=0, tzinfo=dt.timezone.utc)
metOfficeInitTime: dt.datetime = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


class TestGetDatasetForInitTime(unittest.TestCase):

    def testGetsDatasetSuccessfullyFromCEDA(self):
        client = ceda.CEDAClient()
        dataset = client.getDatasetForInitTime(initTime=cedaInitTime)
        print(dataset)

        self.assertTrue({"x", "y", "init_time", "step_time"}.issubset(set(dataset.coords)))
        self.assertTrue(dataset.sizes["step_time"] == 37)
        self.assertTrue(len(dataset.data_vars) > 0)
        print(dataset["sde"].mean())

    def testGetsDatasetSuccessfullyFromMetOffice(self):
        client = metoffice.MetOfficeClient()
        dataset = client.getDatasetForInitTime(initTime=metOfficeInitTime)
        print(dataset)

        self.assertTrue({"x", "y", "init_time", "step_time"}.issubset(set(dataset.coords)))
        self.assertTrue(dataset.sizes["step_time"] > 1)
        self.assertTrue(len(dataset.data_vars) > 0)


class Test_DownloadRawGribFile(unittest.TestCase):

    def test_downloadsRawGribFileFromCEDA(self):
        client = ceda.CEDAClient()
        fileInfo = CEDAFileInfo(name="202201010000_u1096_ng_umqv_Wholesale1.grib")
        outPath = client._downloadRawGRIBFile(fileInfo=fileInfo)
        self.assertTrue(outPath.exists())
        outPath.unlink()

    def test_downloadsRawGribFileFromMetOffice(self):
        client = metoffice.MetOfficeClient(orderID="uk-standard")
        fileInfo = MetOfficeFileInfo(
            fileId=f'agl_temperature_1.5_{dt.datetime.now().strftime("%Y%m%d")}00',
            runDateTime=metOfficeInitTime
        )
        outPath = client._downloadRawGRIBFile(fileInfo=fileInfo)
        self.assertTrue(outPath.exists())
        outPath.unlink()


class Test_GetFileInfosForInitTime(unittest.TestCase):

    def test_getsFileInfosFromCEDA(self):
        client = ceda.CEDAClient()
        fileInfos = client._getFileInfosForInitTime(initTime=cedaInitTime)
        self.assertTrue(len(fileInfos) > 0)

    def test_getsFileInfosFromMetOffice(self):
        client = metoffice.MetOfficeClient(orderID="uk-standard")
        fileInfos = client._getFileInfosForInitTime(initTime=metOfficeInitTime)
        self.assertTrue(len(fileInfos) > 0)


if __name__ == '__main__':
    unittest.main()
