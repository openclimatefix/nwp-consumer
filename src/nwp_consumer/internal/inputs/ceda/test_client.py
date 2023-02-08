import pathlib
import unittest.mock

import datetime as dt

from .client import _isWantedFile, _splitRawGribPerParameter, PARAMETER_IGNORE_LIST, CEDAClient
from ._models import CEDAFileInfo


class TestIsWantedFile(unittest.TestCase):

    def test_correctlyFiltersCEDAFileInfos(self):
        initTime: dt.datetime = dt.datetime(year=2021, month=1, day=1, hour=0, minute=0, tzinfo=dt.timezone.utc)

        wantedFileInfos: list[CEDAFileInfo] = [
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale1.grib"),
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale2.grib"),
        ]

        unwantedFileInfos: list[CEDAFileInfo] = [
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale1T54.grib"),
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale2T54.grib"),
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale3.grib"),
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale3T54.grib"),
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale4.grib"),
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale5.grib"),
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale5T54.grib"),
            CEDAFileInfo(name="202101010300_u1096_ng_umqv_Wholesale1T120.grib"),
            CEDAFileInfo(name="202101010300_u1096_ng_umqv_Wholesale1.grib"),
        ]

        self.assertTrue(
            all([_isWantedFile(fileInfo=fo, desiredInitTime=initTime) for fo in wantedFileInfos]))
        self.assertFalse(
            all([_isWantedFile(fileInfo=fo, desiredInitTime=initTime) for fo in unwantedFileInfos]))


class TestSplitRawGribPerParameter(unittest.TestCase):

    def test_splitsWholesale1Correctly(self):
        wholesalePath: pathlib.Path = pathlib.Path(__file__).parent / "test_truncated_Wholesale1.grib"

        splitPaths = _splitRawGribPerParameter(gribFilePath=wholesalePath)

        self.assertEqual(6, len(splitPaths))
        self.assertFalse(any([path.stem in PARAMETER_IGNORE_LIST for path in splitPaths]))

        for path in splitPaths:
            self.assertTrue(path.exists())

    def test_splitsWholesale2Correctly(self):
        wholesalePath: pathlib.Path = pathlib.Path(__file__).parent / "test_truncated_Wholesale2.grib"

        splitPaths = _splitRawGribPerParameter(gribFilePath=wholesalePath)

        self.assertEqual(6, len(splitPaths))
        self.assertFalse(any([path.stem in PARAMETER_IGNORE_LIST for path in splitPaths]))

        for path in splitPaths:
            self.assertTrue(path.exists())

    def tearDown(self) -> None:
        # Remove created files
        for folder in pathlib.Path(__file__).parent.glob("*test*"):
            if folder.is_dir():
                for path in folder.glob("*"):
                    path.unlink()
                folder.rmdir()


class TestClient_Init(unittest.TestCase):
    @unittest.mock.patch.dict("os.environ", {}, clear=True)
    def test_errorsWhenVariablesAreNotSet(self):
        with self.assertRaises(KeyError):
            _ = CEDAClient()
