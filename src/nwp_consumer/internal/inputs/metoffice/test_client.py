import unittest.mock
import datetime as dt
import pathlib
import numpy as np

from .client import _isWantedFile, _getParameterNameFromFileName, MetOfficeClient
from ._models import MetOfficeFileInfo

from src.nwp_consumer.internal.outputs import localfs
from src.nwp_consumer import internal

METOFFICE_ENV_VARS_DICT = {
    "METOFFICE_ORDER_ID": "test_order_id",
    "METOFFICE_CLIENT_ID": "test_client_id",
    "METOFFICE_CLIENT_SECRET": "test_client_secret",
}


# --------- Client methods --------- #

class TestClient_Init(unittest.TestCase):
    @unittest.mock.patch.dict("os.environ", {}, clear=True)
    def test_errorsWhenVariablesAreNotSet(self):
        with self.assertRaises(KeyError):
            _ = MetOfficeClient(storageClient=localfs.LocalFSClient())

    @unittest.mock.patch.dict("os.environ", METOFFICE_ENV_VARS_DICT)
    def test_errorsWhenStorageClientIsNotSet(self):
        with self.assertRaises(TypeError):
            _ = MetOfficeClient()


@unittest.mock.patch.dict("os.environ", METOFFICE_ENV_VARS_DICT, clear=True)
class TestClient_loadSingleParameterGRIBAsOCFDataArray(unittest.TestCase):

    def setUp(self) -> None:
        self.client = MetOfficeClient(storageClient=localfs.LocalFSClient())

    def test_loadsCorrectly(self):
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_downward-short-wave-radiation-flux.grib"
        testInitTime: dt.datetime = dt.datetime(year=2023, month=3, day=24, hour=0, minute=0, tzinfo=dt.timezone.utc)

        ocfDataArray = self.client.loadSingleParameterGRIBAsOCFDataArray(
            path=testFilePath,
            initTime=testInitTime,
        )

        self.assertEqual(internal.OCFShortName.DownwardShortWaveRadiationFlux, ocfDataArray.name)
        self.assertEqual(np.datetime64(testInitTime), ocfDataArray["init_time"])
        self.assertEqual((13, 639, 455), ocfDataArray.data.shape)


# --------- Static methods --------- #

class Test_IsWantedFile(unittest.TestCase):

    def test_correctlyFiltersMetOfficeFileInfos(self):
        initTime: dt.datetime = dt.datetime(year=2023, month=3, day=24, hour=0, minute=0, tzinfo=dt.timezone.utc)

        wantedFileInfos: list[MetOfficeFileInfo] = [
            MetOfficeFileInfo(
                fileId="agl_temperature_1.5_2023032400",
                runDateTime=dt.datetime(year=2023, month=3, day=24, hour=0, minute=0, tzinfo=dt.timezone.utc)
            ),
            MetOfficeFileInfo(
                fileId="ground_downward-short-wave-radiation-flux_2023032400",
                runDateTime=dt.datetime(year=2023, month=3, day=24, hour=0, minute=0, tzinfo=dt.timezone.utc)
            )
        ]

        unwantedFileInfos: list[MetOfficeFileInfo] = [
            MetOfficeFileInfo(
                fileId="agl_temperature_1.5+00",
                runDateTime=dt.datetime(year=2023, month=3, day=24, hour=0, minute=0, tzinfo=dt.timezone.utc)
            ),
            MetOfficeFileInfo(
                fileId="agl_temperature_1.5_2023032403",
                runDateTime=dt.datetime(year=2023, month=3, day=24, hour=3, minute=0, tzinfo=dt.timezone.utc)
            ),
        ]

        self.assertTrue(all([_isWantedFile(fileInfo=fo, desiredInitTime=initTime) for fo in wantedFileInfos]))
        self.assertFalse(all([_isWantedFile(fileInfo=fo, desiredInitTime=initTime) for fo in unwantedFileInfos]))


class Test_GetParameterFromFileName(unittest.TestCase):

    def test_getsCorrectParameterName(self):
        fileToParameterMap = {
            "agl_temperature_1.5_2023032400": "temperature",
            "ground_downward-short-wave-radiation-flux_2023032400": "downward-short-wave-radiation-flux",
            "ground_downward-long-wave-radiation-flux_2023032400": "downward-long-wave-radiation-flux",
        }

        for fileName, parameterName in fileToParameterMap.items():
            self.assertEqual(parameterName, _getParameterNameFromFileName(fileName=fileName))
