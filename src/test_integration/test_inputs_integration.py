"""Integration tests for the `inputs` module.

WARNING: Requires environment variables to be set for the MetOffice and CEDA APIs.
Just tests connections to the APIs. Tests assume that attempts to download the
source files would raise an exception in the first TIMEOUT seconds of running,
and will be considered passed if no exception is raised within that time.
"""

import datetime as dt
import multiprocessing
import unittest
from collections.abc import Callable

from nwp_consumer.internal import config, inputs, outputs
from nwp_consumer.internal.inputs.ceda._models import CEDAFileInfo
from nwp_consumer.internal.inputs.cmc._models import CMCFileInfo
from nwp_consumer.internal.inputs.ecmwf._models import ECMWFMarsFileInfo
from nwp_consumer.internal.inputs.icon._models import IconFileInfo
from nwp_consumer.internal.inputs.meteofrance._models import ArpegeFileInfo
from nwp_consumer.internal.inputs.metoffice._models import MetOfficeFileInfo

storageClient = outputs.localfs.Client()


TIMEOUT = 10


class TestClient_FetchRawFileBytes(unittest.TestCase):
    def _stop_after_timeout(self, func: Callable, kwargs: dict) -> None:
        """Wrapper to stop a test after TIMEOUT seconds."""
        p = multiprocessing.Process(target=func, kwargs=kwargs)
        p.start()
        p.join(TIMEOUT)
        if p.is_alive():
            p.terminate()
        else:
            # Capture any excepotions raised by the process function
            p.join()
            if p.exitcode != 0:
                self.fail("Function terminated with error.")

    def test_downloadsRawGribFileFromCEDA(self) -> None:
        c = config.CEDAEnv()
        cedaClient = inputs.ceda.Client(
            ftpUsername=c.CEDA_FTP_USER,
            ftpPassword=c.CEDA_FTP_PASS,
)

        fileInfo = CEDAFileInfo(name="202201010000_u1096_ng_umqv_Wholesale1.grib")
        self._stop_after_timeout(cedaClient.downloadToTemp, kwargs={"fi": fileInfo})

    def test_downloadsRawGribFileFromMetOffice(self) -> None:
        metOfficeInitTime: dt.datetime = dt.datetime.now(dt.UTC).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

        c = config.MetOfficeEnv()
        metOfficeClient = inputs.metoffice.Client(
            orderID=c.METOFFICE_ORDER_ID,
            clientID=c.METOFFICE_CLIENT_ID,
            clientSecret=c.METOFFICE_CLIENT_SECRET,
        )
        fileInfo = MetOfficeFileInfo(
            fileId=f'agl_temperature_1.5_{dt.datetime.now(tz=dt.UTC).strftime("%Y%m%d")}00',
            runDateTime=metOfficeInitTime,
        )
        self._stop_after_timeout(metOfficeClient.downloadToTemp, kwargs={"fi": fileInfo})

    def test_downloadsRawGribFileFromECMWFMARS(self) -> None:
        ecmwfMarsInitTime: dt.datetime = dt.datetime(
            year=2022,
            month=1,
            day=1,
            hour=0,
            minute=0,
            tzinfo=dt.UTC,
        )

        ecmwfMarsClient = inputs.ecmwf.mars.Client(
            area="uk",
            hours=4,
        )
        fileInfo = ECMWFMarsFileInfo(
            inittime=ecmwfMarsInitTime,
            area="uk",
        )
        self._stop_after_timeout(ecmwfMarsClient.downloadToTemp, kwargs={"fi": fileInfo})

    def test_downloadsRawGribFileFromICON(self) -> None:
        iconInitTime: dt.datetime = dt.datetime.now(tz=dt.UTC).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

        iconClient = inputs.icon.Client(
            model="global",
        )
        fileInfo = IconFileInfo(
            it=iconInitTime,
            filename=f"icon_global_icosahedral_single-level_{iconInitTime.strftime('%Y%m%d%H')}_001_CLCL.grib2.bz2",
            currentURL="https://opendata.dwd.de/weather/nwp/icon/grib/00/clcl",
            step=1,
        )
        self._stop_after_timeout(iconClient.downloadToTemp, kwargs={"fi": fileInfo})

        iconClient = inputs.icon.Client(model="europe")
        fileInfo = IconFileInfo(
            it=iconInitTime,
            filename=f"icon-eu_europe_regular-lat-lon_single-level_{iconInitTime.strftime('%Y%m%d%H')}_001_CLCL.grib2.bz2",
            currentURL="https://opendata.dwd.de/weather/nwp/icon-eu/grib/00/clcl",
            step=1,
        )
        self._stop_after_timeout(iconClient.downloadToTemp, kwargs={"fi": fileInfo})

    def test_downloadsRawGribFileFromCMC(self) -> None:
        cmcInitTime: dt.datetime = dt.datetime.now(tz=dt.UTC).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

        cmcClient = inputs.cmc.Client(
            model="gdps",
        )
        fileInfo = CMCFileInfo(
            it=cmcInitTime,
            filename=f"CMC_glb_VGRD_ISBL_200_latlon.15x.15_{cmcInitTime.strftime('%Y%m%d%H')}_P120.grib2",
            currentURL="https://dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/120",
            step=1,
        )
        self._stop_after_timeout(cmcClient.downloadToTemp, kwargs={"fi": fileInfo})

    def test_downloadsRawGribFileFromMeteoFrance(self) -> None:
        arpegeInitTime: dt.datetime = dt.datetime.now(tz=dt.UTC).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

        arpegeClient = inputs.meteofrance.Client(
            model="global",
        )
        fileInfo = ArpegeFileInfo(
            it=arpegeInitTime,
            filename="00H24H.grib2",
            currentURL=f"s3://mf-nwp-models/arpege-world/v1/{arpegeInitTime.strftime('%Y-%m-%d')}/{arpegeInitTime.strftime('%H')}/SP1/",
            step=1,
        )
        self._stop_after_timeout(arpegeClient.downloadToTemp, kwargs={"fi": fileInfo})


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
            clientID=c.METOFFICE_CLIENT_ID,
            clientSecret=c.METOFFICE_CLIENT_SECRET,
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
        ecmwfMarsClient = inputs.ecmwf.mars.Client(
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


# TODO: NOAA

if __name__ == "__main__":
    unittest.main()
