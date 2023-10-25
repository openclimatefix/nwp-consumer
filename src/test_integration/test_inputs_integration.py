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
from nwp_consumer.internal.inputs.icon._models import IconFileInfo

storageClient = outputs.localfs.Client()


class TestClient_FetchRawFileBytes(unittest.TestCase):

    def test_downloadsRawGribFileFromCEDA(self):
        c = config.CEDAEnv()
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

        c = config.MetOfficeEnv()
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

        c = config.ECMWFMARSEnv()
        ecmwfMarsClient = inputs.ecmwf.mars.Client(
            area=c.ECMWF_AREA,
            hours=c.ECMWF_HOURS,
        )
        fileInfo = ECMWFMarsFileInfo(
            inittime=ecmwfMarsInitTime,
            area=c.ECMWF_AREA,
        )
        _, tmpPath = ecmwfMarsClient.downloadToTemp(fi=fileInfo)
        self.assertGreater(tmpPath.stat().st_size, 4000000)

    def test_downloadsRawGribFileFromICON(self):
        iconInitTime: dt.datetime = dt.datetime.now() \
            .replace(hour=0, minute=0, second=0, microsecond=0)

        iconClient = inputs.icon.Client(
            model="global",
        )
        fileInfo = IconFileInfo(
            it=iconInitTime,
            filename=f"icon_global_icosahedral_single-level_{iconInitTime.strftime('%Y%m%d%H')}_001_CLCL.grib2.bz2",
            currentURL="https://opendata.dwd.de/weather/nwp/icon/grib/00/clcl"
        )
        _, tmpPath = iconClient.downloadToTemp(fi=fileInfo)
        self.assertGreater(tmpPath.stat().st_size, 40000)

        iconClient = inputs.icon.Client(
            model="europe"
        )
        fileInfo = IconFileInfo(
            it=iconInitTime,
            filename=f"icon-eu_europe_regular-lat-lon_single-level_{iconInitTime.strftime('%Y%m%d%H')}_001_CLCL.grib2.bz2",
            currentURL="https://opendata.dwd.de/weather/nwp/icon-eu/grib/00/clcl"
        )
        _, tmpPath = iconClient.downloadToTemp(fi=fileInfo)
        self.assertGreater(tmpPath.stat().st_size, 40000)

class TestListRawFilesForInitTime(unittest.TestCase):

    def test_getsFileInfosFromCEDA(self):
        cedaInitTime: dt.datetime = dt.datetime(
            year=2022, month=1, day=1, hour=0, minute=0, tzinfo=None
        )
        c = config.CEDAEnv()
        cedaClient = inputs.ceda.Client(
            ftpUsername=c.CEDA_FTP_USER,
            ftpPassword=c.CEDA_FTP_PASS,
        )
        fileInfos = cedaClient.listRawFilesForInitTime(it=cedaInitTime)
        self.assertTrue(len(fileInfos) > 0)

    def test_getsFileInfosFromMetOffice(self):
        metOfficeInitTime: dt.datetime = dt.datetime.now() \
            .replace(hour=0, minute=0, second=0, microsecond=0)
        c = config.MetOfficeEnv()
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
        c = config.ECMWFMARSEnv()
        ecmwfMarsClient = inputs.ecmwf.mars.Client(
            area=c.ECMWF_AREA,
            hours=c.ECMWF_HOURS,
        )
        fileInfos = ecmwfMarsClient.listRawFilesForInitTime(it=ecmwfMarsInitTime)
        self.assertTrue(len(fileInfos) > 0)

    def test_getsFileInfosFromICON(self):
        iconInitTime: dt.datetime = dt.datetime.now() \
            .replace(hour=0, minute=0, second=0, microsecond=0)
        iconClient = inputs.icon.Client(
            model="global",
        )
        fileInfos = iconClient.listRawFilesForInitTime(it=iconInitTime)
        self.assertTrue(len(fileInfos) > 0)

        iconClient = inputs.icon.Client(
            model="europe"
        )
        euFileInfos = iconClient.listRawFilesForInitTime(it=iconInitTime)
        self.assertTrue(len(euFileInfos) > 0)

        self.assertNotEqual(fileInfos, euFileInfos)


if __name__ == '__main__':
    unittest.main()
