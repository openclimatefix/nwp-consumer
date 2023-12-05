import datetime as dt
import inspect
import unittest
import uuid
from pathlib import Path

from botocore.session import Session
from moto.server import ThreadedMotoServer

from nwp_consumer import internal

from .client import Client

ENDPOINT_URL = "http://localhost:5000"
BUCKET = "test-bucket"
KEY = "test-key"
SECRET = "test-secret"  # noqa: S105
REGION = "us-east-1"

RAW = Path("raw")
ZARR = Path("zarr")


class TestS3Client(unittest.TestCase):
    testS3 = None
    bucket = None
    server = None

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
        cls.client = Client(
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

    def test_exists(self) -> None:
        # Create a mock file in the raw directory
        initTime = dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc)
        fileName = inspect.stack()[0][3] + ".grib"
        filePath = RAW / f"{initTime:%Y/%m/%d/%H%M}" / fileName
        self.testS3.put_object(
            Bucket=BUCKET, Key=filePath.as_posix(), Body=bytes(fileName, "utf-8"),
        )

        # Call the existsInRawDir method
        exists = self.client.exists(dst=filePath)

        # Verify the existence of the file
        self.assertTrue(exists)

        # Call the existsInRawDir method on a non-existent file
        exists = self.client.exists(dst=Path("non_existent_file.grib"))

        # Verify the non-existence of the file
        self.assertFalse(exists)

        # Delete the created files
        self.testS3.delete_object(
            Bucket=BUCKET,
            Key=filePath.as_posix(),
        )

    def test_store(self) -> None:
        initTime = dt.datetime(2023, 1, 2, tzinfo=dt.timezone.utc)
        fileName = inspect.stack()[0][3] + ".grib"
        dst = RAW / f"{initTime:{internal.IT_FOLDER_FMTSTR}}" / fileName
        src = internal.TMP_DIR / f"nwpc-{uuid.uuid4()}"

        # Write the data to the temporary file
        src.write_bytes(bytes(fileName, "utf-8"))

        name = self.client.store(src=src, dst=dst)

        # Verify the written file in the raw directory
        response = self.testS3.get_object(Bucket=BUCKET, Key=dst.as_posix())
        self.assertEqual(response["Body"].read(), bytes(fileName, "utf-8"))

        # Verify the correct number of bytes was written
        self.assertEqual(name, dst)

        # Delete the created file and the temp file
        self.testS3.delete_object(Bucket=BUCKET, Key=dst.as_posix())
        src.unlink(missing_ok=True)

    def test_listInitTimes(self) -> None:
        # Create mock folders/files in the raw directory
        self.testS3.put_object(
            Bucket=BUCKET, Key=f"{RAW}/2023/01/03/0000/test_raw_file1.grib", Body=b"test_data",
        )
        self.testS3.put_object(
            Bucket=BUCKET, Key=f"{RAW}/2023/01/04/0300/test_raw_file2.grib", Body=b"test_data",
        )

        # Call the listInitTimesInRawDir method
        init_times = self.client.listInitTimes(prefix=RAW)

        # Verify the returned list of init times
        expected_init_times = [
            dt.datetime(2023, 1, 3, 0, 0, tzinfo=dt.timezone.utc),
            dt.datetime(2023, 1, 4, 3, 0, tzinfo=dt.timezone.utc),
        ]
        self.assertEqual(init_times, expected_init_times)

        # Delete the created files
        self.testS3.delete_object(
            Bucket=BUCKET,
            Key=f"{RAW}/2023/01/03/0000/test_raw_file1.grib",
        )
        self.testS3.delete_object(
            Bucket=BUCKET,
            Key=f"{RAW}/2023/01/04/0300/test_raw_file2.grib",
        )

    def test_copyITFolderToTemp(self) -> None:
        # Make some files in the raw directory
        initTime = dt.datetime(2023, 1, 1, 3, tzinfo=dt.timezone.utc)
        files = [
            RAW / f"{initTime:{internal.IT_FOLDER_FMTSTR}}" / "test_copyITFolderToTemp1.grib",
            RAW / f"{initTime:{internal.IT_FOLDER_FMTSTR}}" / "test_copyITFolderToTemp2.grib",
            RAW / f"{initTime:{internal.IT_FOLDER_FMTSTR}}" / "test_copyITFolderToTemp3.grib",
        ]
        for f in files:
            self.testS3.put_object(
                Bucket=BUCKET, Key=f.as_posix(), Body=bytes("test_file_contents", "utf-8"),
            )

        # Call the copyItFolderToTemp method
        paths = self.client.copyITFolderToTemp(prefix=RAW, it=initTime)

        # Assert the contents of the temp files is correct
        for _i, path in enumerate(paths):
            self.assertEqual(path.read_bytes(), bytes("test_file_contents", "utf-8"))

            # Delete the temp files
            path.unlink()

        # Delete the files in S3
        for f in files:
            self.testS3.delete_object(Bucket=BUCKET, Key=f.as_posix())

        # Make some more RAW files in the raw directory AND in the temp directory
        initTime2 = dt.datetime(2023, 1, 1, 6, tzinfo=dt.timezone.utc)
        files2 = [
            RAW / f"{initTime2:{internal.IT_FOLDER_FMTSTR}}" / "test_copyITFolderToTemp1.grib",
            RAW / f"{initTime2:{internal.IT_FOLDER_FMTSTR}}" / "test_copyITFolderToTemp2.grib",
            RAW / f"{initTime2:{internal.IT_FOLDER_FMTSTR}}" / "test_copyITFolderToTemp3.grib",
        ]
        for f in files2:
            self.testS3.put_object(
                Bucket=BUCKET, Key=f.as_posix(), Body=bytes("test_file_contents", "utf-8"),
            )
            with open(internal.TMP_DIR / f.name, "w") as f:
                f.write("test_file_contents")

        # Call the copyITFolderToTemp method again
        paths = self.client.copyITFolderToTemp(prefix=RAW, it=initTime2)
        self.assertEqual(len(paths), 3)

        # Delete the files in S3
        for f in files2:
            self.testS3.delete_object(Bucket=BUCKET, Key=f.as_posix())

    @unittest.skip("Broken on github ci")
    def test_delete(self) -> None:
        # Create a file in the raw directory
        initTime = dt.datetime(2023, 1, 1, 3, tzinfo=dt.timezone.utc)
        path = RAW / f"{initTime:{internal.IT_FOLDER_FMTSTR}}" / "test_delete.grib"
        self.testS3.put_object(
            Bucket=BUCKET, Key=path.as_posix(), Body=bytes("test_delete", "utf-8"),
        )

        # Delete the file using the function
        self.client.delete(p=path)

        # Assert that the file no longer exists
        with self.assertRaises(Exception):
            self.testS3.get_object(Bucket=BUCKET, Key=path.as_posix())


if __name__ == "__main__":
    unittest.main()
