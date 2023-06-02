import datetime as dt
import pathlib
import unittest.mock

from ._models import MetOfficeFileInfo
from .client import MetOfficeClient, _isWantedFile, _loadSingleParameterGRIBAsOCFDataset


# --------- Test setup --------- #

testClient = MetOfficeClient(orderID="tmp", clientID="tmp", clientSecret="tmp")


# --------- Client methods --------- #


class TestClient_Init(unittest.TestCase):
    def test_errorsWhenVariablesAreNotSet(self):
        with self.assertRaises(KeyError):
            _ = MetOfficeClient(
                orderID="unset",
                clientID="",
                clientSecret="test_client_secret")


# --------- Static methods --------- #


class TestClient_loadSingleParameterGRIBAsOCFDataset(unittest.TestCase):

    def test_loadsCorrectly(self):
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_knownparam.grib"

        out = _loadSingleParameterGRIBAsOCFDataset(
            data=testFilePath.read_bytes(),
        )

        self.assertEqual(out.dims, ({"step": 13, "y": 639, "x": 455}))

    def test_renamesVariables(self):
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_wrongnameparam.grib"

        out = _loadSingleParameterGRIBAsOCFDataset(
            data=testFilePath.read_bytes(),
        )

        self.assertEqual(list(out.data_vars)[0], "prate")

    def test_handlesUnknownsInMetOfficeData(self):
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_unknownparam1.grib"

        out = _loadSingleParameterGRIBAsOCFDataset(
            data=testFilePath.read_bytes(),
        )

        actual = list(out.data_vars)[0]

        self.assertNotEqual("unknown", actual)
        self.assertEqual("si10", actual)

        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_unknownparam2.grib"

        out = _loadSingleParameterGRIBAsOCFDataset(
            data=testFilePath.read_bytes(),
        )

        actual = list(out.data_vars)[0]

        self.assertNotEqual("unknown", actual)
        self.assertEqual("wdir10", actual)


class TestClient_LoadRawInitTimeDataAsOCFDataset(unittest.TestCase):

    def test_loadsRawInitTimeDataCorrectly(self):

        fileBytesList: list[bytes] = [
            (pathlib.Path(__file__).parent / file).read_bytes() for file in
            ["test_knownparam.grib", "test_wrongnameparam.grib"]
        ]

        dataset = testClient.loadRawInitTimeDataAsOCFDataset(fileBytesList=fileBytesList)

        self.assertEqual(2, len(dataset.data_vars))


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

