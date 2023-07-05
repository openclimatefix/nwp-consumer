import datetime as dt
import shutil
import unittest
from pathlib import Path

import numpy as np
import xarray as xr

from nwp_consumer import internal

# Import the class to be tested
from .client import LocalFSClient


class TestExistsInRawDir(unittest.TestCase):
    def setUp(self) -> None:
        self.client = LocalFSClient("test_raw_dir", "test_zarr_dir", createDirs=True)
        self.fileName = "test_file"
        self.initTime = dt.datetime(2023, 1, 1)

        # Create a temporary file to simulate an existing file in the raw directory
        self.file_path = Path(
            f"test_raw_dir/{self.initTime.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING)}/{self.fileName}")
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.file_path.touch()

    def test_file_exists(self) -> None:
        # Check if the file exists using the function
        exists = self.client.rawFileExistsForInitTime(
            name=self.fileName,
            it=self.initTime
        )

        # Assert that the file exists
        self.assertTrue(exists)

    def test_file_does_not_exist(self) -> None:
        # Check if the file exists using the function
        exists = self.client.rawFileExistsForInitTime(
            name=self.fileName + "not-here",
            it=self.initTime
        )

        # Assert that the file does not exist
        self.assertFalse(exists)

    def tearDown(self) -> None:
        # Clean up the temporary directory
        shutil.rmtree("test_raw_dir")
        shutil.rmtree("test_zarr_dir")


class TestWriteBytesToRawDir(unittest.TestCase):
    def setUp(self) -> None:
        self.client = LocalFSClient("test_raw_dir", "test_zarr_dir", createDirs=True)
        self.fileName = "test_file"
        self.initTime = dt.datetime(2023, 1, 1)
        self.data = b"test_data"

    def test_write_bytes_to_raw_dir(self) -> None:
        # Write the bytes to the raw directory using the function
        path = self.client.writeBytesToRawFile(
            name=self.fileName,
            it=self.initTime,
            b=self.data
        )

        # Assert that the path exists
        self.assertTrue(self.client.rawFileExistsForInitTime(
            name=self.fileName,
            it=self.initTime
        ))

        # Assert that the file content is correct
        self.assertEqual(path.read_bytes(), self.data)

    def tearDown(self) -> None:
        # Clean up the temporary file
        shutil.rmtree("test_raw_dir")
        shutil.rmtree("test_zarr_dir")


class TestListInitTimesInRawDir(unittest.TestCase):
    def setUp(self) -> None:
        self.client = LocalFSClient("test_raw_dir", "test_zarr_dir", createDirs=True)
        # Create temporary directories and files to simulate the raw directory structure
        self.dir_paths = [
            Path(f"test_raw_dir/{dt.datetime(2023, 1, 1, 3).strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING)}"),
            Path(f"test_raw_dir/{dt.datetime(2023, 1, 2, 6).strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING)}"),
            Path(f"test_raw_dir/{dt.datetime(2023, 1, 3, 9).strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING)}")
        ]
        for path in self.dir_paths:
            path.mkdir(parents=True, exist_ok=True)

    def test_list_init_times(self) -> None:
        # Get the list of init times using the function
        initTimes = self.client.listInitTimesInRawDir()

        # Assert that the list of init times is correct
        expected_initTimes = [
            dt.datetime(2023, 1, 1, 3, tzinfo=None),
            dt.datetime(2023, 1, 2, 6, tzinfo=None),
            dt.datetime(2023, 1, 3, 9, tzinfo=None)
        ]
        self.assertEqual(initTimes, expected_initTimes)

    def tearDown(self) -> None:
        # Clean up the temporary directories
        shutil.rmtree("test_raw_dir")
        shutil.rmtree("test_zarr_dir")


class TestReadBytesForInitTime(unittest.TestCase):
    def setUp(self) -> None:
        self.client = LocalFSClient("test_raw_dir", "test_zarr_dir", createDirs=True)
        self.initTime = dt.datetime(2023, 1, 1, 3)
        # Create temporary directories and files to simulate the raw directory structure
        self.file_paths = [
            Path(f"test_raw_dir/{self.initTime.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING)}/1.grib"),
            Path(f"test_raw_dir/{self.initTime.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING)}/2.grib"),
            Path(f"test_raw_dir/{self.initTime.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING)}/3.grib")
        ]
        for path in self.file_paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"test_data")

    def test_read_bytes_for_init_time(self) -> None:
        # Read the bytes for the init time using the function
        initTime, fileByteList = self.client.readRawFilesForInitTime(it=self.initTime)

        # Assert that the returned init time is correct
        self.assertEqual(initTime, self.initTime)

        # Assert that the list of file bytes is correct
        self.assertEqual(fileByteList, [b"test_data"] * 3)

    def tearDown(self) -> None:
        shutil.rmtree("test_raw_dir")
        shutil.rmtree("test_zarr_dir")


class TestExistsInZarrDir(unittest.TestCase):
    def setUp(self) -> None:
        self.client = LocalFSClient("test_raw_dir", "test_zarr_dir", createDirs=True)
        self.fileName = "test_file.zarr"
        self.initTime = dt.datetime(2023, 1, 1)

    def test_file_exists(self) -> None:
        # Create a temporary file to simulate an existing file in the zarr directory
        file_path = Path(f"test_zarr_dir/{self.fileName}")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.touch()

        # Check if the file exists using the function
        exists = self.client.zarrExistsForInitTime(name=self.fileName, it=self.initTime)

        # Assert that the file exists
        self.assertTrue(exists)

    def test_file_does_not_exist(self) -> None:
        # Check if the file exists using the function
        exists = self.client.zarrExistsForInitTime(
            name='no_such_' + self.fileName,
            it=self.initTime
        )

        # Assert that the file does not exist
        self.assertFalse(exists)

    def tearDown(self) -> None:
        shutil.rmtree("test_raw_dir")
        shutil.rmtree("test_zarr_dir")


class TestWriteDatasetToZarrDir(unittest.TestCase):
    def setUp(self) -> None:
        self.client = LocalFSClient("test_raw_dir", "test_zarr_dir", createDirs=True)
        self.fileName = "test_file"
        self.initTime = dt.datetime(2023, 1, 1)
        self.data = xr.Dataset(
            data_vars={
                "t": (["init_time", "step", "x", "y"], np.random.rand(1, 46, 100, 100)),
                "r": (["init_time", "step", "x", "y"], np.random.rand(1, 46, 100, 100))
            },
            coords={
                "init_time": (["init_time"], [self.initTime]),
                "step": (["step"], np.arange(46)),
                "x": (["x"], np.arange(100)),
                "y": (["y"], np.arange(100))
            },
        )

    def test_write_dataset_to_zarr_dir(self) -> None:
        # Write the dataset to the zarr directory using the function
        self.client.writeDatasetAsZarr(name=self.fileName, it=self.initTime, ds=self.data)

        # Assert that the path exists
        self.assertTrue(self.client.zarrExistsForInitTime(name=self.fileName, it=self.initTime))

    def tearDown(self) -> None:
        shutil.rmtree("test_raw_dir")
        shutil.rmtree("test_zarr_dir")

class TestDeleteZarrForInitTime(unittest.TestCase):

    def setUp(self) -> None:
        self.client = LocalFSClient("test_raw_dir", "test_zarr_dir", createDirs=True)
        self.fileName = "test_file.zarr"
        self.initTime = dt.datetime(2023, 1, 1)
        self.data = xr.Dataset(
            data_vars={
                "t": (["init_time", "step", "x", "y"], np.random.rand(1, 46, 100, 100)),
                "r": (["init_time", "step", "x", "y"], np.random.rand(1, 46, 100, 100))
            },
            coords={
                "init_time": (["init_time"], [self.initTime]),
                "step": (["step"], np.arange(46)),
                "x": (["x"], np.arange(100)),
                "y": (["y"], np.arange(100))
            },
        )
        # Write the dataset to the zarr directory using the function
        self.client.writeDatasetAsZarr(name=self.fileName, it=self.initTime, ds=self.data)

    def test_delete_zarr_for_init_time(self) -> None:
        # Delete the zarr file for the init time using the function
        self.client.deleteZarrForInitTime(name=self.fileName, it=self.initTime)

        # Assert that the path does not exist
        self.assertFalse(self.client.zarrExistsForInitTime(self.fileName, self.initTime))

    def tearDown(self) -> None:
        shutil.rmtree("test_raw_dir")
        shutil.rmtree("test_zarr_dir")


if __name__ == "__main__":
    unittest.main()
