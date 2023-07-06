import datetime as dt
import inspect
import pathlib
import unittest

import xarray as xr
from botocore.session import Session
from moto.server import ThreadedMotoServer

from nwp_consumer import internal

from . import S3Client


class TestS3Client(unittest.TestCase):

    mockS3 = None
    bucket = None
    server = None

    @classmethod
    def setUpClass(cls):
        cls.server = ThreadedMotoServer()
        cls.server.start()

        session = Session()
        cls.mockS3 = session.create_client(
            service_name="s3",
            region_name="us-east-1",
            endpoint_url="http://localhost:5000",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
        )

        # Create a mock S3 bucket
        cls.bucket = "test-bucket"

        cls.mockS3.create_bucket(
            Bucket=cls.bucket
        )

        # Create an instance of the S3Client class
        cls.client = S3Client(
            key="test-key",
            secret="test-secret",
            region="us-east-1",
            rawDir="raw",
            zarrDir="zarr",
            bucket=cls.bucket,
            endpointURL="http://localhost:5000",
        )

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def test_existsInRawDir(self):
        # Create a mock file in the raw directory
        initTime = dt.datetime(2023, 1, 1)
        fileName = inspect.stack()[0][3] + ".grib"
        filePath = f"raw/{initTime:%Y/%m/%d/%H%M}/{fileName}"
        self.mockS3.put_object(
            Bucket=self.bucket,
            Key=filePath,
            Body=b"test_data"
        )

        # Call the existsInRawDir method
        exists = self.client.rawFileExistsForInitTime(
            name=fileName,
            it=initTime
        )

        # Remove the mock file in the raw directory
        self.mockS3.delete_object(
            Bucket=self.bucket,
            Key=filePath,
        )

        # Verify the existence of the file
        self.assertTrue(exists)

        # Call the existsInRawDir method on a non-existent file
        exists = self.client.rawFileExistsForInitTime(
            name="non_existent_file.grib",
            it=initTime
        )

        # Verify the non-existence of the file
        self.assertFalse(exists)

    def test_writeBytesToRawDir(self):
        # Call the writeBytesToRawDir method
        initTime = dt.datetime(2023, 1, 1)
        fileName = inspect.stack()[0][3] + ".grib"
        path = self.client.writeBytesToRawFile(fileName, initTime, bytes(fileName, 'utf-8'))

        # Verify the written file in the raw directory
        response = self.mockS3.get_object(
            Bucket=self.bucket,
            Key=path.relative_to(self.bucket).as_posix()
        )
        self.assertEqual(response["Body"].read(), bytes(fileName, 'utf-8'))

        # Remove the mock file in the raw directory
        self.mockS3.delete_object(
            Bucket=self.bucket,
            Key=path.relative_to(self.bucket).as_posix(),
        )

    def test_listInitTimesInRawDir(self):
        # Create mock folders/files in the raw directory
        self.mockS3.put_object(
            Bucket=self.bucket,
            Key="raw/2023/01/01/0000/test_raw_file1.grib",
            Body=b"test_data"
        )
        self.mockS3.put_object(
            Bucket=self.bucket,
            Key="raw/2023/01/02/0300/test_raw_file2.grib",
            Body=b"test_data"
        )

        # Call the listInitTimesInRawDir method
        init_times = self.client.listInitTimesInRawDir()

        # Remove the mock files in the raw directory
        self.mockS3.delete_object(
            Bucket=self.bucket,
            Key="raw/2023/01/01/0000/test_raw_file1.grib"
        )
        self.mockS3.delete_object(
            Bucket=self.bucket,
            Key="raw/2023/01/02/0300/test_raw_file2.grib"
        )

        # Verify the returned list of init times
        expected_init_times = [
            dt.datetime(2023, 1, 1, 0, 0),
            dt.datetime(2023, 1, 2, 3, 0),
        ]
        self.assertEqual(init_times, expected_init_times)

    def test_readBytesForInitTime(self):
        # Create a mock file in the raw directory for the given init time
        initTime = dt.datetime(2023, 1, 1)
        fileName = "test_raw_file3.grib"
        filePath = pathlib.Path("raw") \
                   / initTime.strftime(internal.IT_FOLDER_FMTSTR) \
                   / fileName
        self.mockS3.put_object(
            Bucket=self.bucket,
            Key=filePath.as_posix(),
            Body=b"test_raw_file3"
        )

        # Call the readBytesForInitTime method
        readInitTime, readBytes = self.client.readRawFilesForInitTime(
            it=initTime
        )

        # Remove the mock file in the raw directory
        self.mockS3.delete_object(
            Bucket=self.bucket,
            Key=filePath.as_posix()
        )

        # Verify the returned init time and bytes
        self.assertEqual(initTime, readInitTime)
        self.assertEqual([b"test_raw_file3"], readBytes)

    def test_writeDatasetToZarrDir(self):
        # Create a mock dataset
        mock_dataset = xr.Dataset(
            data_vars={
                'wdir10': (
                    ('init_time', 'step', 'x', 'y'), [[[[1, 2], [3, 4]], [[5, 6], [7, 8]]]]
                ),
                'prate': (
                    ('init_time', 'step', 'x', 'y'), [[[[1, 2], [3, 4]], [[5, 6], [7, 8]]]]
                )
            },
            coords={
                'init_time': [dt.datetime(2023, 1, 1)],
                'step': [0, 1],
                'x': [0, 1],
                'y': [0, 1],
            }
        )

        # Call the writeDatasetToZarrDir method
        path = self.client.writeDatasetAsZarr(
            name="test_zarr_file.zarr",
            it=dt.datetime(2023, 1, 1),
            ds=mock_dataset
        )

        # Verify the returned path
        expected_path = pathlib.Path("test-bucket/zarr/test_zarr_file.zarr")
        self.assertEqual(expected_path, path)

    def test_existsInZarrDir(self):
        # Create a mock file in the zarr directory
        fileName = "test_zarr_file2.zarr"
        filePath = pathlib.Path("zarr") / fileName
        self.mockS3.put_object(
            Bucket=self.bucket,
            Key=filePath.as_posix(),
            Body=b"test_zarr_data2"
        )

        # Call the existsInZarrDir method
        exists = self.client.zarrExistsForInitTime(
            name=fileName,
            it=dt.datetime(2023, 1, 1)
        )

        # Verify the existence of the file
        self.assertTrue(exists)

    def test_deleteZarrForInitTime(self):
        # Write mock dataset to zarr directory
        mock_dataset = xr.Dataset(
            data_vars={
                'wdir10': (
                    ('init_time', 'step', 'x', 'y'), [[[[1, 2], [3, 4]], [[5, 6], [7, 8]]]]
                ),
                'prate': (
                    ('init_time', 'step', 'x', 'y'), [[[[1, 2], [3, 4]], [[5, 6], [7, 8]]]]
                )
            },
            coords={
                'init_time': [dt.datetime(2023, 1, 1)],
                'step': [0, 1],
                'x': [0, 1],
                'y': [0, 1],
            }
        )

        # Call the writeDatasetToZarrDir method
        self.client.writeDatasetAsZarr(
            name="latest.zarr",
            it=dt.datetime(2023, 1, 1),
            ds=mock_dataset
        )

        # Call the deleteZarrForInitTime method
        self.client.deleteZarrForInitTime(
            name="latest.zarr",
            it=dt.datetime(2023, 1, 1)
        )

        # Verify the file is deleted
        self.assertFalse(self.client.zarrExistsForInitTime(
            name="latest",
            it=dt.datetime(2023, 1, 1)
        ))


if __name__ == "__main__":
    unittest.main()
