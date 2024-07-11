"""Unit tests for the S3Client class."""

import datetime as dt
import unittest
from pathlib import Path

import xarray as xr
from botocore.client import BaseClient as BotocoreClient
from botocore.session import Session
from moto.server import ThreadedMotoServer
import numpy as np

from ._models import ECMWFLiveFileInfo
from .s3 import S3Client

ENDPOINT_URL = "http://localhost:5000"
BUCKET = "test-bucket"
KEY = "test-key"
SECRET = "test-secret"  # noqa: S105
REGION = "us-east-1"

RAW = Path("ecmwf")


class TestS3Client(unittest.TestCase):
    testS3: BotocoreClient
    client: S3Client
    server: ThreadedMotoServer

    @classmethod
    def setUpClass(cls) -> None:
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
            area="uk",
            key=KEY,
            secret=SECRET,
            region=REGION,
            bucket=BUCKET,
            endpointURL=ENDPOINT_URL,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        # Delete all objects in bucket
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

    def test_listFilesForInitTime(self) -> None:
        files = [
            "A1D01010000010100001",
            "A1D01010000010101001",
            "A1D01010000010102011",
            "A1D01010000010103001",
            "A1D01011200010112001",  # Different init time
            "A1D02191200010112001",  # Leap year on 2024-02-29
        ]
        for file in files:
            # Create files in the mock bucket
            self.testS3.put_object(
                Bucket=BUCKET,
                Key=(RAW / file).as_posix(),
                Body=b"test",
            )

        # Test the listFilesForInitTime method
        initTime = dt.datetime(2021, 1, 1, 0, 0, 0, tzinfo=dt.UTC)
        out = self.client.listRawFilesForInitTime(it=initTime)
        self.assertEqual(len(out), 4)

    def test_downloadRawFile(self) -> None:
        # Create a file in the mock bucket
        self.testS3.put_object(
            Bucket=BUCKET,
            Key=(RAW / "A1D01010000010100001").as_posix(),
            Body=b"test",
        )

        # Test the downloadRawFile method
        out = self.client.downloadToCache(fi=ECMWFLiveFileInfo(fname="A1D01010000010100001"))
        self.assertEqual(out.read_bytes(), b"test")

        out.unlink()

    def test_mapCached(self) -> None:
        testfile: Path = Path(__file__).parent / "test_multiarea.grib"
        out: xr.Dataset = self.client.mapCachedRaw(p=testfile)

        self.assertEqual(
            out[next(iter(out.data_vars.keys()))].dims,
            ("init_time", "step", "latitude", "longitude"),
        )
        self.assertEqual(len(out.data_vars.keys()), 18)
        self.assertEqual(out.coords["latitude"].to_numpy().max(), 60)
        self.assertIn("t2m", list(out.data_vars.keys()))
        self.assertTrue(np.all(out.data_vars["t2m"].values))

        print(out)

        # Check that setting the area maps only the relevant data
        indiaClient = S3Client(
            area="nw-india",
            key=KEY,
            secret=SECRET,
            region=REGION,
            bucket=BUCKET,
            endpointURL=ENDPOINT_URL,
        )
        out = indiaClient.mapCachedRaw(p=testfile)
        self.assertEqual(out.coords["latitude"].to_numpy().max(), 31)
        self.assertIn("t2m", list(out.data_vars.keys()))
        self.assertTrue(np.all(out.data_vars["t2m"].values))

