import unittest
import pathlib
import xarray as xr
import unittest.mock

from .client import LocalFSClient


@unittest.mock.patch.dict("os.environ", {
        "RAW_GRIB_DIR_PATH": pathlib.Path(__file__).parent.as_posix()
    }, clear=True)
class TestClient_Exists(unittest.TestCase):

    def setUp(self) -> None:
        self.client = LocalFSClient()

    def test_returnsTrueWhenGRIBPathExists(self):
        self.assertTrue(self.client.exists(filepath=pathlib.Path("test_fakefile.grib")))

    def test_returnsFalseWhenPathDoesNotExist(self):
        self.assertFalse(self.client.exists(filepath=pathlib.Path("test_doesnotexist.grib")))


class TestClient_Open(unittest.TestCase):

    @unittest.mock.patch.dict("os.environ", {
        "RAW_GRIB_DIR_PATH": pathlib.Path(__file__).parent.as_posix()
    }, clear=True)
    def setUp(self) -> None:
        self.client = LocalFSClient()
        self.testNonExistingPath = pathlib.Path("doesnotexist.grib")
        self.testNonExistingDir = pathlib.Path("testdir") / "doesnotexist.grib"

    def test_opensFile(self):
        with self.client.open(path=pathlib.Path("test_fakefile.grib")) as f:
            self.assertIsNotNone(f)

    def test_doesNotRaisesErrorWhenFileDoesNotExist(self):
        with self.client.open(path=self.testNonExistingPath) as f:
            self.assertIsNotNone(f)

    def test_createsParentDirectories(self):
        with self.client.open(path=self.testNonExistingDir) as f:
            self.assertIsNotNone(f)

    def tearDown(self) -> None:
        (pathlib.Path(__file__).parent / self.testNonExistingPath).unlink(missing_ok=True)
        if (pathlib.Path(__file__).parent / self.testNonExistingDir).exists():
            (pathlib.Path(__file__).parent / self.testNonExistingDir).unlink()
            (pathlib.Path(__file__).parent / self.testNonExistingDir.parent).rmdir()


class TestClient_SaveDataset(unittest.TestCase):
    def setUp(self) -> None:
        self.client = LocalFSClient()

    def test_savesDataset(self):
        testDataset: xr.Dataset = xr.Dataset()
        testDataset["test"] = xr.DataArray([1, 2, 3])