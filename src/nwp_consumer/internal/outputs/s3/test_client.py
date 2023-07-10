import datetime as dt
import inspect
import pathlib
import unittest

import xarray as xr
from botocore.session import Session
from moto.server import ThreadedMotoServer
import numpy as np

from nwp_consumer import internal

from . import S3Client

ENDPOINT_URL = "http://localhost:5000"
BUCKET = "test-bucket"
KEY = "test-key"
SECRET = "test-secret"
REGION = "us-east-1"


class TestS3Client(unittest.TestCase):

    testS3 = None
    bucket = None
    server = None

    @classmethod
    def setUpClass(cls):
        # Start a local S3 server
        cls.server = ThreadedMotoServer()
        cls.server.start()

        session = Session()
        cls.testS3 = session.create_client(
            service_name="s3",
            region_name=REGION,
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET,
        )

        # Create a mock S3 bucket
        cls.testS3.create_bucket(
            Bucket=BUCKET,
        )

        # Create an instance of the S3Client class
        cls.client = S3Client(
            key=KEY,
            secret=SECRET,
            region=REGION,
            rawDir="raw",
            zarrDir="zarr",
            bucket=BUCKET,
            endpointURL=ENDPOINT_URL,
        )

    @classmethod
    def tearDownClass(cls):
        # Delete all objects in bucket
        print("Tearing down bucket")
        response = cls.testS3.list_objects_v2(
            Bucket=BUCKET,
        )
        if "Contents" in response:
            for obj in response["Contents"]:
                cls.testS3.delete_object(
                    Bucket=BUCKET,
                    Key=obj["Key"],
                )
        cls.server.stop()

    def setUp(self) -> None:
        print("Setting up test")

    def tearDown(self) -> None:
        # Delete all objects in bucket
        print("Tearing down test")

    def test_existsInRawDir(self):
        # Create a mock file in the raw directory
        initTime = dt.datetime(2023, 1, 1)
        fileName = inspect.stack()[0][3] + ".grib"
        filePath = f"raw/{initTime:%Y/%m/%d/%H%M}/{fileName}"
        self.testS3.put_object(
            Bucket=BUCKET,
            Key=filePath,
            Body=bytes(fileName, 'utf-8')
        )

        # Call the existsInRawDir method
        exists = self.client.rawFileExistsForInitTime(
            name=fileName,
            it=initTime
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

        # Delete the created files
        self.testS3.delete_object(
            Bucket=BUCKET,
            Key=filePath,
        )

    def test_writeBytesToRawDir(self):
        # Call the writeBytesToRawDir method
        initTime = dt.datetime(2023, 1, 2)
        fileName = inspect.stack()[0][3] + ".grib"
        path = self.client.writeBytesToRawFile(fileName, initTime, bytes(fileName, 'utf-8'))

        # Verify the written file in the raw directory
        response = self.testS3.get_object(
            Bucket=BUCKET,
            Key=path.relative_to(BUCKET).as_posix()
        )
        self.assertEqual(response["Body"].read(), bytes(fileName, 'utf-8'))

        # Delete the created file
        self.testS3.delete_object(
            Bucket=BUCKET,
            Key=path.relative_to(BUCKET).as_posix()
        )

    def test_listInitTimesInRawDir(self):
        # Create mock folders/files in the raw directory
        self.testS3.put_object(
            Bucket=BUCKET,
            Key="raw/2023/01/03/0000/test_raw_file1.grib",
            Body=b"test_data"
        )
        self.testS3.put_object(
            Bucket=BUCKET,
            Key="raw/2023/01/04/0300/test_raw_file2.grib",
            Body=b"test_data"
        )

        # Call the listInitTimesInRawDir method
        init_times = self.client.listInitTimesInRawDir()

        # Verify the returned list of init times
        expected_init_times = [
            dt.datetime(2023, 1, 3, 0, 0),
            dt.datetime(2023, 1, 4, 3, 0),
        ]
        self.assertEqual(init_times, expected_init_times)

        # Delete the created files
        self.testS3.delete_object(
            Bucket=BUCKET,
            Key="raw/2023/01/03/0000/test_raw_file1.grib",
        )
        self.testS3.delete_object(
            Bucket=BUCKET,
            Key="raw/2023/01/04/0300/test_raw_file2.grib",
        )

    def test_readBytesForInitTime(self):
        # Create a mock file in the raw directory for the given init time
        initTime = dt.datetime(2023, 1, 5)
        fileName = inspect.stack()[0][3] + ".grib"
        filePath = pathlib.Path("raw") \
                   / initTime.strftime(internal.IT_FOLDER_FMTSTR) \
                   / fileName
        self.testS3.put_object(
            Bucket=BUCKET,
            Key=filePath.as_posix(),
            Body=bytes(fileName, 'utf-8')
        )

        # Call the readBytesForInitTime method
        readInitTime, readBytes = self.client.readRawFilesForInitTime(
            it=initTime
        )

        # Verify the returned init time and bytes
        self.assertEqual(initTime, readInitTime)
        self.assertEqual([bytes(fileName, 'utf-8')], readBytes)

        # Delete the created file
        self.testS3.delete_object(
            Bucket=BUCKET,
            Key=filePath.as_posix(),
        )

    def test_writeDatasetToZarrDir(self):
        # Create a mock dataset
        mock_dataset = xr.Dataset(
            data_vars={
                'UKV': (('init_time', 'variable', 'step', 'x', 'y'), np.random.rand(1, 2, 12, 100, 100)),
            },
            coords={
                'init_time': [dt.datetime(2023, 1, 1)],
                'variable': ['wdir10', 'prate'],
                'step': range(12),
                'x': range(100),
                'y': range(100),
            }
        )

        filename = inspect.stack()[0][3] + ".zarr"

        # Call the writeDatasetToZarrDir method
        path = self.client.writeDatasetAsZarr(
            name=filename,
            it=dt.datetime(2023, 1, 6),
            ds=mock_dataset
        )

        # Verify the returned path
        expected_path = pathlib.Path(f"test-bucket/zarr/{filename}")
        self.assertEqual(expected_path, path)

        # Delete the created files
        response = self.testS3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=f"zarr/{filename}"
        )
        if "Contents" in response:
            for obj in response["Contents"]:
                self.testS3.delete_object(
                    Bucket=BUCKET,
                    Key=obj["Key"],
                )


    def test_existsInZarrDir(self):
        # Create a mock file in the zarr directory
        fileName = inspect.stack()[0][3] + ".zarr"
        self.testS3.put_object(
            Bucket=BUCKET,
            Key=f"zarr/{fileName}",
            Body=bytes(fileName, 'utf-8')
        )

        # Call the existsInZarrDir method
        exists = self.client.zarrExistsForInitTime(
            name=fileName,
            it=dt.datetime(2023, 1, 6)
        )

        # Verify the existence of the file
        self.assertTrue(exists)

        # Delete the created file
        self.testS3.delete_object(
            Bucket=BUCKET,
            Key=f"zarr/{fileName}",
        )

    def test_deleteZarrForInitTime(self):
        # Write mock dataset to zarr directory
        mock_dataset = xr.Dataset(
            data_vars={
                'UKV': (('init_time', 'variable', 'step', 'x', 'y'), np.random.rand(1, 2, 12, 100, 100)),
            },
            coords={
                'init_time': [dt.datetime(2023, 1, 1)],
                'variable': ['wdir10', 'prate'],
                'step': range(12),
                'x': range(100),
                'y': range(100),
            }
        )

        # Call the writeDatasetToZarrDir method
        self.client.writeDatasetAsZarr(
            name="latest.zarr",
            it=dt.datetime(2023, 1, 7),
            ds=mock_dataset
        )

        # Call the deleteZarrForInitTime method
        self.client.deleteZarrForInitTime(
            name="latest.zarr",
            it=dt.datetime(2023, 1, 7)
        )

        # Verify the file is deleted
        self.assertFalse(self.client.zarrExistsForInitTime(
            name="latest",
            it=dt.datetime(2023, 1, 7)
        ))


if __name__ == "__main__":
    unittest.main()
