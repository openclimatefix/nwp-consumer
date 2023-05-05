import pathlib
import unittest
import unittest.mock

import xarray as xr

from .client import LocalFSClient


class TestClient_Exists(unittest.TestCase):

    def setUp(self) -> None:
        self.client = LocalFSClient(
            rawDir=pathlib.Path(__file__).parent.as_posix(),
            zarrDir=pathlib.Path(__file__).parent.as_posix(),
        )

    def test_returnsTrueWhenGRIBPathExists(self):
        self.assertTrue(self.client.existsInRawDir(relativePath=pathlib.Path("test_fakefile.grib")))

    def test_returnsFalseWhenPathDoesNotExist(self):
        self.assertFalse(self.client.existsInRawDir(relativePath=pathlib.Path("test_doesnotexist.grib")))


class TestClient_ReadBytesFromRawDir(unittest.TestCase):

    def setUp(self) -> None:
        self.client = LocalFSClient(
            rawDir=pathlib.Path(__file__).parent.as_posix(),
            zarrDir=pathlib.Path(__file__).parent.as_posix(),
        )

    def test_RaisesErrorWhenFileDoesNotExist(self):
        with self.assertRaises(FileNotFoundError):
            self.client.readBytesFromRawDir(relativePath="doesnotexist.grib")

    def test_ReadsBytesCorrectly(self):
        bytes = self.client.readBytesFromRawDir(relativePath="test_fakefile.grib")
        self.assertEqual(3, len(bytes))


class TestClient_WriteBytesToRawDir(unittest.TestCase):

    def setUp(self) -> None:
        self.client = LocalFSClient(
            rawDir=pathlib.Path(__file__).parent.as_posix(),
            zarrDir=pathlib.Path(__file__).parent.as_posix(),
        )
        self.testNonExistingRelativePath = pathlib.Path("doesnotexist.grib")
        self.testNonExistingRelativeDir = pathlib.Path("testdir") / "doesnotexist.grib"

    def test_WritesBytesCorrectly(self):
        self.client.writeBytesToRawDir(relativePath=self.testNonExistingRelativePath, data=b"test")
        self.assertTrue((pathlib.Path(__file__).parent / self.testNonExistingRelativePath).exists())

    def test_createsDirWhenItDoesNotExist(self):
        self.client.writeBytesToRawDir(relativePath=self.testNonExistingRelativeDir, data=b"test")
        self.assertTrue((pathlib.Path(__file__).parent / self.testNonExistingRelativeDir).exists())
        self.assertTrue((pathlib.Path(__file__).parent / self.testNonExistingRelativeDir.parent).exists())

    def tearDown(self) -> None:
        (pathlib.Path(__file__).parent / self.testNonExistingRelativePath).unlink(missing_ok=True)
        if (pathlib.Path(__file__).parent / self.testNonExistingRelativeDir).exists():
            (pathlib.Path(__file__).parent / self.testNonExistingRelativeDir).unlink()
            (pathlib.Path(__file__).parent / self.testNonExistingRelativeDir.parent).rmdir()


class TestClient_SaveDataset(unittest.TestCase):
    def setUp(self) -> None:
        self.client = LocalFSClient(
            rawDir=pathlib.Path(__file__).parent.as_posix(),
            zarrDir=pathlib.Path(__file__).parent.as_posix(),
        )

    def test_savesDataset(self):
        testDataset: xr.Dataset = xr.Dataset()
        testDataset["test"] = xr.DataArray([1, 2, 3])

    def tearDown(self) -> None:
        pass
