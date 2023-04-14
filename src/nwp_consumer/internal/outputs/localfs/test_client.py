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


class TestClient_Open(unittest.TestCase):

    def setUp(self) -> None:
        self.client = LocalFSClient(
            rawDir=pathlib.Path(__file__).parent.as_posix(),
            zarrDir=pathlib.Path(__file__).parent.as_posix(),
        )
        self.testNonExistingRelativePath = pathlib.Path("doesnotexist.grib")
        self.testNonExistingRelativeDir = pathlib.Path("testdir") / "doesnotexist.grib"

    def test_opensFile(self):
        with self.client.openFromRawDir(relativePath=pathlib.Path("test_fakefile.grib")) as f:
            self.assertIsNotNone(f)

    def test_doesNotRaisesErrorWhenFileDoesNotExist(self):
        with self.client.openFromRawDir(relativePath=self.testNonExistingRelativePath) as f:
            self.assertIsNotNone(f)

    def test_createsParentDirectories(self):
        with self.client.openFromRawDir(relativePath=self.testNonExistingRelativeDir) as f:
            self.assertIsNotNone(f)

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
