import datetime as dt
import pathlib
import unittest

import boto3
import moto
import xarray as xr

from nwp_consumer import internal

from . import S3Client


@moto.mock_s3
class TestS3Client(unittest.TestCase):

    def setUp(self):
        # Create a mock S3 bucket
        self.bucket = "test-bucket"

        self.mockS3 = boto3.client(
            's3',
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region_name='us-east-1',
        )
        self.mockS3.create_bucket(
            Bucket=self.bucket
        )

        # Create an instance of the S3Client class
        self.client = S3Client(
            key="test-key",
            secret="test-secret",
            region="us-east-1",
            rawDir="raw",
            zarrDir="zarr",
            bucket=self.bucket,
        )

    def test_existsInRawDir(self):
        # Create a mock file in the raw directory
        initTime = dt.datetime(2023, 1, 1)
        fileName = "test_file.grib"
        filePath = pathlib.Path("raw") \
            / initTime.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING) \
            / fileName
        self.mockS3.put_object(
            Bucket=self.bucket,
            Key=filePath.as_posix(),
            Body=b"test_data"
        )

        # Call the existsInRawDir method
        exists = self.client.existsInRawDir(
            fileName=fileName,
            initTime=initTime
        )

        # Verify the existence of the file
        self.assertTrue(exists)

    def test_writeBytesToRawDir(self):
        # Call the writeBytesToRawDir method
        initTime = dt.datetime(2023, 1, 1)
        fileName = "test_file"
        self.client.writeBytesToRawDir(fileName, initTime, b"test_data")

        # Verify the written file in the raw directory
        self.assertTrue(self.client.existsInRawDir(
            fileName=fileName,
            initTime=initTime
        ))

    def test_listInitTimesInRawDir(self):
        # Create mock folders/files in the raw directory
        self.mockS3.put_object(
            Bucket=self.bucket,
            Key="raw/2023/01/01/0000/test_file",
            Body=b"test_data"
        )
        self.mockS3.put_object(
            Bucket=self.bucket,
            Key="raw/2023/01/02/0300/test_file",
            Body=b"test_data"
        )

        # Call the listInitTimesInRawDir method
        init_times = self.client.listInitTimesInRawDir()

        # Verify the returned list of init times
        expected_init_times = [
            dt.datetime(2023, 1, 1, 0, 0),
            dt.datetime(2023, 1, 2, 3, 0),
        ]
        self.assertEqual(init_times, expected_init_times)

    def test_readBytesForInitTime(self):
        # Create a mock file in the raw directory for the given init time
        initTime = dt.datetime(2023, 1, 1)
        fileName = "test_file"
        filePath = pathlib.Path("raw") \
            / initTime.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING) \
            / fileName
        self.mockS3.put_object(
            Bucket=self.bucket,
            Key=filePath.as_posix(),
            Body=b"test_data"
        )

        # Call the readBytesForInitTime method
        readInitTime, readBytes = self.client.readBytesForInitTime(
            initTime=initTime
        )

        # Verify the returned init time and bytes
        self.assertEqual(readInitTime, initTime)
        self.assertEqual(readBytes, [b"test_data"])

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
        path = self.client.writeDatasetToZarrDir(
            fileName="test_file",
            initTime=dt.datetime(2023, 1, 1),
            data=mock_dataset
        )

        # Verify the returned path
        expected_path = pathlib.Path("s3://test-bucket/zarr/test_file")
        self.assertEqual(expected_path, path)

    def test_existsInZarrDir(self):
        # Create a mock file in the zarr directory
        fileName = "test_file"
        filePath = pathlib.Path("zarr") / fileName
        self.mockS3.put_object(
            Bucket=self.bucket,
            Key=filePath.as_posix(),
            Body=b"test_data"
        )

        # Call the existsInZarrDir method
        exists = self.client.existsInZarrDir(
            fileName=fileName,
            initTime=dt.datetime(2023, 1, 1)
        )

        # Verify the existence of the file
        self.assertTrue(exists)


if __name__ == "__main__":
    unittest.main()
