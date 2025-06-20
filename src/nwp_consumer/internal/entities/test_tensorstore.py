import contextlib
import dataclasses
import datetime as dt
import logging
import os
import unittest
from collections.abc import Generator
from types import TracebackType
from unittest.mock import patch

import numpy as np
import xarray as xr
from botocore.client import BaseClient as BotocoreClient
from botocore.session import Session
from moto.server import ThreadedMotoServer
from returns.pipeline import is_successful
from returns.result import Success

from .coordinates import NWPDimensionCoordinateMap
from .parameters import Parameter
from .postprocess import PostProcessOptions
from .tensorstore import TensorStore

logging.getLogger("werkzeug").setLevel(logging.ERROR)


class MockS3Bucket:
    client: BotocoreClient
    server: ThreadedMotoServer
    bucket: str = "test-bucket"

    def __enter__(self) -> None:
        """Create a mock S3 server and bucket."""
        self.server = ThreadedMotoServer()
        self.server.start()

        session = Session()
        self.client = session.create_client(
            service_name="s3",
            region_name="us-east-1",
            endpoint_url="http://localhost:5000",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )

        self.client.create_bucket(
            Bucket=self.bucket,
        )

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        response = self.client.list_objects_v2(
            Bucket=self.bucket,
        )
        """Delete all objects in the bucket and stop the server."""
        if "Contents" in response:
            for obj in response["Contents"]:
                self.client.delete_object(
                    Bucket=self.bucket,
                    Key=obj["Key"],
                )
        self.server.stop()


class TestTensorStore(unittest.TestCase):
    """Test the business methods of the TensorStore class."""

    @contextlib.contextmanager
    def store(self, year: int) -> Generator[TensorStore, None, None]:
        """Create an instance of the TensorStore class."""

        test_coords: NWPDimensionCoordinateMap = NWPDimensionCoordinateMap(
            init_time=[dt.datetime(year, 1, 1, h, tzinfo=dt.UTC) for h in [0, 6, 12, 18]],
            step=[1, 2, 3, 4],
            variable=[Parameter.TEMPERATURE_SL],
            latitude=np.linspace(90, -90, 12).tolist(),
            longitude=np.linspace(0, 360, 18).tolist(),
        )

        init_result = TensorStore.initialize_empty_store(
            model="test_da",
            repository="dummy_repository",
            coords=test_coords,
            chunks=test_coords.chunking(chunk_count_overrides={}),
        )
        self.assertIsInstance(init_result, Success, msg=init_result)
        store = init_result.unwrap()
        yield store
        store.delete_store()

    @patch.dict(
        os.environ,
        {
            "AWS_ENDPOINT_URL": "http://localhost:5000",
            "AWS_ACCESS_KEY_ID": "test-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret",
            "ZARRDIR": "s3://test-bucket/data",
        },
        clear=True,
    )
    def test_initialize_and_delete_s3(self) -> None:
        """Test the initialize_empty_store method."""

        with MockS3Bucket(), self.store(year=2022) as ts:
            delete_result = ts.delete_store()
            self.assertIsInstance(delete_result, Success, msg=delete_result)

    def test_write_to_region(self) -> None:
        """Test the write_to_region method."""
        with self.store(year=2022) as ts:
            test_da: xr.DataArray = xr.DataArray(
                name="test_da",
                data=np.ones(
                    shape=list(ts.coordinate_map.shapemap.values()),
                ),
                coords=ts.coordinate_map.to_pandas(),
            )

            # Write each init time and step one at a time
            for it in test_da.coords["init_time"].values:
                for step in test_da.coords["step"].values:
                    write_result = ts.write_to_region(
                        da=test_da.where(
                            test_da["init_time"] == it,
                            drop=True,
                        ).where(test_da["step"] == step, drop=True),
                    )
                    self.assertIsInstance(write_result, Success, msg=write_result)

    def test_postprocess(self) -> None:
        """Test the postprocess method."""

        @dataclasses.dataclass
        class TestCase:
            name: str
            options: PostProcessOptions
            should_error: bool

        tests: list[TestCase] = [
            TestCase(
                name="empty_options",
                options=PostProcessOptions(),
                should_error=False,
            ),
        ]

        with self.store(year=1971) as ts:
            for t in tests:
                with self.subTest(name=t.name):
                    result = ts.postprocess(t.options)
                    if t.should_error:
                        self.assertTrue(
                            isinstance(result, Exception),
                            msg="Expected error to be returned.",
                        )
                    else:
                        self.assertTrue(is_successful(result))

    def test_missing_times(self) -> None:
        """Test the missing_times method."""

        @dataclasses.dataclass
        class TestCase:
            name: str
            times_to_write: list[dt.datetime]
            expected: list[dt.datetime]

        with self.store(year=2024) as ts:
            tests: list[TestCase] = [
                TestCase(
                    name="all_missing_times",
                    times_to_write=[],
                    expected=ts.coordinate_map.init_time,
                ),
                TestCase(
                    name="some_missing_times",
                    times_to_write=[ts.coordinate_map.init_time[0], ts.coordinate_map.init_time[2]],
                    expected=[ts.coordinate_map.init_time[1], ts.coordinate_map.init_time[3]],
                ),
                TestCase(
                    name="no_missing_times",
                    times_to_write=ts.coordinate_map.init_time,
                    expected=[],
                ),
            ]

            for t in tests:
                with self.subTest(name=t.name):
                    for i in t.times_to_write:
                        write_result = ts.write_to_region(
                            da=xr.DataArray(
                                name="test_da",
                                data=np.ones(
                                    shape=[
                                        1 if k == "init_time" else v
                                        for k, v in ts.coordinate_map.shapemap.items()
                                    ],
                                ),
                                coords=ts.coordinate_map.to_pandas()
                                | {
                                    "init_time": [np.datetime64(i.replace(tzinfo=None), "ns")],
                                },
                            ),
                        )
                        write_result.unwrap()
                    result = ts.missing_times()
                    missing_times = result.unwrap()
                    self.assertListEqual(missing_times, t.expected)


if __name__ == "__main__":
    unittest.main()
