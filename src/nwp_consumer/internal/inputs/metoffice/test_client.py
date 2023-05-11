import datetime as dt
import pathlib
import unittest.mock

from nwp_consumer.internal import outputs

from ._models import MetOfficeFileInfo
from .client import MetOfficeClient, _getParameterNameFromFileName, _isWantedFile

testStorer = outputs.localfs.LocalFSClient(
    rawDir=(pathlib.Path(__file__).parent / "test").as_posix(),
    zarrDir=(pathlib.Path(__file__).parent / "test").as_posix(),
)

testClient = MetOfficeClient(orderID="tmp", clientID="tmp", clientSecret="tmp", storer=testStorer)


# --------- Client methods --------- #


class TestClient_Init(unittest.TestCase):
    def test_errorsWhenVariablesAreNotSet(self):
        with self.assertRaises(KeyError):
            _ = MetOfficeClient(
                orderID="unset",
                clientID="",
                clientSecret="test_client_secret",
                storer=testStorer)


class TestClient_loadSingleParameterGRIBAsOCFDataArray(unittest.TestCase):

    @unittest.skip("Not yet implemented")
    def test_loadsCorrectly(self):
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_downward-short-wave-radiation-flux.grib"
        testInitTime: dt.datetime = dt.datetime(year=2023, month=3, day=24, hour=0, minute=0, tzinfo=dt.timezone.utc)

        testClient._loadSingleParameterGRIBAsOCFDataArray(
            path=testFilePath,
            initTime=testInitTime,
        )

        # TODO: assert


class TestClient_LoadRawInitTimeDataAsOCFDataset(unittest.TestCase):

    @unittest.skip("Not yet implemented")
    def test_loadsRawInitTimeDataCorrectly(self):
        initTime: dt.datetime = dt.datetime(year=2021, month=1, day=1, hour=0, minute=0, tzinfo=dt.timezone.utc)

        files = [
            pathlib.Path(__file__).parent / file for file in
            ["test_downward-short-wave-radiation-flux.grib", "test_total-precipitation-rate.grib"]
        ]

        dataset = testClient.loadRawInitTimeDataAsOCFDataset(rawRelativePaths=files, initTime=initTime)
        print(dataset)

        self.assertEqual(2, len(dataset.data_vars))
        # TODO: fix shape


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
