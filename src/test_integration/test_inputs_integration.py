"""Integration tests for the `inputs` module.

WARNING: Requires environment variables to be set for the MetOffice and CEDA APIs.
Just tests connections to the APIs. Tests assume that attempts to download the
source files would raise an exception in the first TIMEOUT seconds of running,
and will be considered passed if no exception is raised within that time.
"""

import datetime as dt
import unittest

from nwp_consumer.internal import config, inputs, outputs

storageClient = outputs.localfs.Client()


TIMEOUT = 10


class TestListRawFilesForInitTime(unittest.TestCase):
    def test_getsFileInfosFromCEDA(self) -> None:
        cedaInitTime: dt.datetime = dt.datetime(
            year=2022,
            month=1,
            day=1,
            hour=0,
            minute=0,
            tzinfo=dt.UTC,
        )
        c = config.CEDAEnv()
        cedaClient = inputs.ceda.Client(
            ftpUsername=c.CEDA_FTP_USER,
            ftpPassword=c.CEDA_FTP_PASS,
        )
        fileInfos = cedaClient.listRawFilesForInitTime(it=cedaInitTime)
        self.assertTrue(len(fileInfos) > 0)

    def test_getsFileInfosFromMetOffice(self) -> None:
        metOfficeInitTime: dt.datetime = dt.datetime.now(tz=dt.UTC).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        c = config.MetOfficeEnv()
        metOfficeClient = inputs.metoffice.Client(
            orderID=c.METOFFICE_ORDER_ID,
            apiKey=c.METOFFICE_API_KEY,
        )
        fileInfos = metOfficeClient.listRawFilesForInitTime(it=metOfficeInitTime)
        self.assertTrue(len(fileInfos) > 0)

    def test_getsFileInfosFromECMWFMARS(self) -> None:
        ecmwfMarsInitTime: dt.datetime = dt.datetime(
            year=2022,
            month=1,
            day=1,
            hour=0,
            minute=0,
            tzinfo=dt.UTC,
        )
        c = config.ECMWFMARSEnv()
        ecmwfMarsClient = inputs.ecmwf.MARSClient(
            area=c.ECMWF_AREA,
            hours=4,
        )
        fileInfos = ecmwfMarsClient.listRawFilesForInitTime(it=ecmwfMarsInitTime)
        self.assertTrue(len(fileInfos) > 0)

    def test_getsFileInfosFromICON(self) -> None:
        iconInitTime: dt.datetime = dt.datetime.now(tz=dt.UTC).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        iconClient = inputs.icon.Client(
            model="global",
            hours=4,
            param_group="basic",
        )
        fileInfos = iconClient.listRawFilesForInitTime(it=iconInitTime)
        self.assertTrue(len(fileInfos) > 0)

        iconClient = inputs.icon.Client(
            model="europe",
            hours=4,
            param_group="basic",
        )
        euFileInfos = iconClient.listRawFilesForInitTime(it=iconInitTime)
        self.assertTrue(len(euFileInfos) > 0)
        self.assertNotEqual(fileInfos, euFileInfos)

    def test_getsFileInfosFromCMC(self) -> None:
        cmcInitTime: dt.datetime = dt.datetime.now(tz=dt.UTC).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        cmcClient = inputs.cmc.Client(
            model="gdps",
            hours=4,
            param_group="basic",
        )
        fileInfos = cmcClient.listRawFilesForInitTime(it=cmcInitTime)
        self.assertGreater(len(fileInfos), 0)

        cmcClient = inputs.cmc.Client(
            model="geps",
            hours=4,
            param_group="basic",
        )
        gepsFileInfos = cmcClient.listRawFilesForInitTime(it=cmcInitTime)
        self.assertGreater(len(gepsFileInfos), 0)
        self.assertNotEqual(fileInfos, gepsFileInfos)

    def test_getsFileInfosFromMeteoFrance(self) -> None:
        arpegeInitTime: dt.datetime = dt.datetime.now(tz=dt.UTC).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        arpegeClient = inputs.meteofrance.Client(
            model="global",
            hours=4,
            param_group="basic",
        )
        fileInfos = arpegeClient.listRawFilesForInitTime(it=arpegeInitTime)
        self.assertTrue(len(fileInfos) > 0)

        arpegeClient = inputs.meteofrance.Client(
            model="europe",
            hours=4,
            param_group="basic",
        )
        europeFileInfos = arpegeClient.listRawFilesForInitTime(it=arpegeInitTime)
        self.assertTrue(len(europeFileInfos) > 0)
        self.assertNotEqual(fileInfos, europeFileInfos)

    def test_getsFilesFromNOAANCAR(self) -> None:
        ncarInitTime: dt.datetime = dt.datetime(
            year=2023,
            month=12,
            day=19,
            tzinfo=dt.UTC,
        )
        ncarClient = inputs.noaa.NCARClient(
            model="global",
            param_group="full",
            hours=4,
        )
        fileInfos = ncarClient.listRawFilesForInitTime(it=ncarInitTime)
        self.assertTrue(len(fileInfos) > 0)


if __name__ == "__main__":
    unittest.main()
