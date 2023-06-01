import datetime as dt
import pathlib
import unittest.mock

from ._models import MetOfficeFileInfo
from .client import MetOfficeClient, _isWantedFile


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


class TestClient_loadSingleParameterGRIBAsOCFDataset(unittest.TestCase):

    def test_loadsCorrectly(self):
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_downward-short-wave-radiation-flux.grib"

        out = testClient._loadSingleParameterGRIBAsOCFDataset(
            data=testFilePath.open("rb").read(),
        )

        self.assertEqual(out.dims, ({"step": 13, "y": 639, "x": 455}))

    @unittest.skip("Not yet implemented")
    def test_renamesVariables(self):
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_total-precipitation-rate.grib"

        out = testClient._loadSingleParameterGRIBAsOCFDataset(
            data=testFilePath.open("rb").read(),
        )

        self.assertEqual(out.data_vars, "prate")


class TestClient_LoadRawInitTimeDataAsOCFDataset(unittest.TestCase):

    def test_loadsRawInitTimeDataCorrectly(self):

        fileBytesList: list[bytes] = [
            (pathlib.Path(__file__).parent / file).open('rb').read() for file in
            ["test_downward-short-wave-radiation-flux.grib", "test_total-precipitation-rate.grib"]
        ]

        dataset = testClient.loadRawInitTimeDataAsOCFDataset(fileBytesList=fileBytesList)

        self.assertEqual(2, len(dataset.data_vars))


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

