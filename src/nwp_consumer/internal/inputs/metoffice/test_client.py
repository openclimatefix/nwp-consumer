import unittest
import datetime as dt

from .client import _isWantedFile, _getParameterNameFromFileName, MetOfficeClient
from ._models import MetOfficeFileInfo


class TestIsWantedFile(unittest.TestCase):

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


class TestGetParameterFromFileName(unittest.TestCase):

    def test_getsCorrectParameterName(self):

        fileToParameterMap = {
            "agl_temperature_1.5_2023032400": "temperature",
            "ground_downward-short-wave-radiation-flux_2023032400": "downward-short-wave-radiation-flux",
            "ground_downward-long-wave-radiation-flux_2023032400": "downward-long-wave-radiation-flux",
        }

        for fileName, parameterName in fileToParameterMap.items():
            self.assertEqual(parameterName, _getParameterNameFromFileName(fileName=fileName))


class TestClient_Init(unittest.TestCase):
    @unittest.mock.patch.dict("os.environ", {}, clear=True)
    def test_errorsWhenVariablesAreNotSet(self):
        with self.assertRaises(KeyError):
            _ = MetOfficeClient()
