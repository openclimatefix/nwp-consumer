import datetime as dt
import shutil
import unittest
import uuid
from pathlib import Path

import numpy as np
import xarray as xr

from nwp_consumer import internal

from .client import Client

RAW = Path("test_raw_dir")
ZARR = Path("test_zarr_dir")


class TestLocalFSClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        # Make test directories
        RAW.mkdir(parents=True, exist_ok=True)
        ZARR.mkdir(parents=True, exist_ok=True)

        cls.testClient = Client()

    @classmethod
    def tearDownClass(cls) -> None:
        # Clean up the temporary directory
        shutil.rmtree(RAW.as_posix())
        shutil.rmtree(ZARR.as_posix())

    def test_exists(self):
        initTime = dt.datetime(2021, 1, 1, 0, 0, 0)

        # Create a file in the raw directory
        path = RAW / f"{initTime:{internal.IT_FOLDER_FMTSTR}}" / "test_file.grib"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()

        # Check if the file exists using the function
        exists = self.testClient.exists(dst=path)

        # Assert that the file exists
        self.assertTrue(exists)

        # Remove the init time folder
        shutil.rmtree(RAW / "2021")

        # Check that the function returns false when the file does not exist
        exists = self.testClient.exists(
            dst=RAW / f"{initTime:{internal.IT_FOLDER_FMTSTR}}" / "not_exists.grib"
        )

        # Assert that the file does not exist
        self.assertFalse(exists)

        # Create a zarr file in the zarr directory
        xr.Dataset(
            data_vars={
                'UKV': (
                    ('init_time', 'variable', 'step', 'x', 'y'),
                    np.random.rand(1, 2, 12, 100, 100)),
            },
            coords={
                'init_time': [dt.datetime(2023, 1, 1)],
                'variable': ['t', 'r'],
                'step': range(12),
                'x': range(100),
                'y': range(100),
            }
        ).to_zarr(store=ZARR / "test_file.zarr")

        # Check if the file exists using the function
        exists = self.testClient.exists(dst=ZARR / "test_file.zarr")

        # Assert that the file exists
        self.assertTrue(exists)

    def test_store(self):
        initTime = dt.datetime(2021, 1, 2, 0, 0, 0)
        dst = RAW / f"{initTime:{internal.IT_FOLDER_FMTSTR}}" / "test_store.grib"
        src = internal.TMP_DIR / f"nwpc-{uuid.uuid4()}"
        # Create a temporary file to simulate a file to be stored
        src.write_bytes(bytes("test_file_contents", 'utf-8'))

        # Store the file using the function
        size = self.testClient.store(src=src, dst=dst)

        # Assert that the file exists
        self.assertTrue(dst.exists())
        # Assert that the file has the correct size
        self.assertEqual(size, 18)
        # Assert that the temporary file has been deleted
        self.assertFalse(src.exists())

    def test_listInitTimes(self):
        expectedTimes = [
            dt.datetime(2023, 1, 1, 3, tzinfo=None),
            dt.datetime(2023, 1, 2, 6, tzinfo=None),
            dt.datetime(2023, 1, 3, 9, tzinfo=None)
        ]

        # Create some files in the raw directory
        dirs = [RAW / t.strftime(internal.IT_FOLDER_FMTSTR) for t in expectedTimes]

        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)

        # Get the list of init times
        initTimes = self.testClient.listInitTimes(prefix=Path(RAW))

        # Assert that the list of init times is correct
        self.assertEqual(initTimes, expectedTimes)

        # Remove the files
        for d in dirs:
            shutil.rmtree(d)

    def test_copyITFolderToTemp(self):
        # Make some files in the raw directory
        initTime = dt.datetime(2023, 1, 1, 3)
        files = [
            RAW / f"{initTime:{internal.IT_FOLDER_FMTSTR}}" / "test_copyITFolderToTemp1.grib",
            RAW / f"{initTime:{internal.IT_FOLDER_FMTSTR}}" / "test_copyITFolderToTemp2.grib",
            RAW / f"{initTime:{internal.IT_FOLDER_FMTSTR}}" / "test_copyITFolderToTemp3.grib"
        ]
        for f in files:
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_bytes(bytes("test_file_contents", 'utf-8'))

        # Test the function
        paths = self.testClient.copyITFolderToTemp(prefix=RAW, it=initTime)

        # Assert the contents of the temp files is correct
        for _i, path in enumerate(paths):
            self.assertEqual(path.read_bytes(), bytes("test_file_contents", 'utf-8'))

        # Remove the files
        shutil.rmtree(files[0].parent)

    def test_delete(self):
        # Create a file in the raw directory
        initTime = dt.datetime(2023, 1, 1, 3)
        path = RAW / f"{initTime:{internal.IT_FOLDER_FMTSTR}}" / "test_delete.grib"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()

        # Delete the file using the function
        self.testClient.delete(p=path)

        # Assert that the file no longer exists
        self.assertFalse(path.exists())

        # Create a zarr folder in the zarr directory
        path = ZARR / "test_delete.zarr"
        _ = xr.Dataset(
            data_vars={
                'UKV': (
                    ('init_time', 'variable', 'step', 'x', 'y'),
                    np.random.rand(1, 2, 12, 100, 100)
                ),
            },
            coords={
                'init_time': [dt.datetime(2023, 1, 1)],
                'variable': ['t', 'r'],
                'step': range(12),
                'x': range(100),
                'y': range(100),
            }
        ).to_zarr(store=path)

        # Delete the folder using the function
        self.testClient.delete(p=path)

        # Assert that the folder no longer exists
        self.assertFalse(path.exists())


if __name__ == "__main__":
    unittest.main()
