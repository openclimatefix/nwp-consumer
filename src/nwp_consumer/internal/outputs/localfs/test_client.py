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

    # TODO
    pass


class TestClient_ReadBytesFromRawDir(unittest.TestCase):
    # TODO
    pass


class TestClient_WriteBytesToRawDir(unittest.TestCase):
    # TODO
    pass

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

class TestClient_ListFilesInRawDir(unittest.TestCase):

    def test_listsCorrectly(self):
        client = LocalFSClient(
            rawDir=pathlib.Path(__file__).parent.as_posix(),
            zarrDir=pathlib.Path(__file__).parent.as_posix(),
        )

        files = client.listFilesInRawDir()
        print(files)
        self.assertEqual(len(files), 0)
