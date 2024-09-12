import dataclasses
import datetime as dt
import os
import unittest
from typing import TYPE_CHECKING

from returns.pipeline import is_successful

from ...entities import NWPDimensionCoordinateMap
from .ecmwf_realtime import ECMWFRealTimeS3ModelRepository

if TYPE_CHECKING:
    import xarray as xr

    from nwp_consumer.internal import entities


class TestECMWFRealTimeS3ModelRepository(unittest.TestCase):
    """Test the business methods of the ECMWFRealTimeS3ModelRepository class."""

    @unittest.skipIf(
        condition="CI" in os.environ,
        reason="Skipping integration test that requires S3 access.",
    ) # TODO: Move into integration tests, or remove
    def test__download_and_convert(self) -> None:
        """Test the _download_and_convert method."""

        auth_result = ECMWFRealTimeS3ModelRepository.authenticate()
        self.assertTrue(is_successful(auth_result), msg=f"Error: {auth_result.failure}")
        c: ECMWFRealTimeS3ModelRepository = auth_result.unwrap()

        test_it: dt.datetime = dt.datetime(2024, 10, 25, 0, tzinfo=dt.UTC)
        test_coordinates: entities.NWPDimensionCoordinateMap = dataclasses.replace(
            c.model().expected_coordinates,
            init_time=[test_it],
        )

        urls: list[str] = [
            f"s3://{f}"
            for f in c._fs.ls(f"{c.bucket}/ecmwf")
            if c._wanted_file(
                filename=f.split("/")[-1],
                it=test_it,
                max_step=max(c.model().expected_coordinates.step),
            )
        ]

        for url in urls:
            with (self.subTest(url=url)):
                result = c._download_and_convert(url)

                self.assertTrue(is_successful(result), msg=f"Error: {result}")

                da: xr.DataArray = result.unwrap()[0]
                determine_region_result = NWPDimensionCoordinateMap.from_xarray(da).bind(
                    test_coordinates.determine_region,
                )
                self.assertTrue(
                    is_successful(determine_region_result),
                    msg=f"Error: {determine_region_result}",
                )

    def test__wanted_file(self) -> None:
        """Test the _wanted_file method."""

        @dataclasses.dataclass
        class TestCase:
            name: str
            filename: str
            expected: bool

        test_it: dt.datetime = dt.datetime(2024, 10, 25, 0, tzinfo=dt.UTC)

        tests: list[TestCase] = [
            TestCase(
                name="valid_filename",
                filename=f"A2D{test_it:%m%d%H%M}102516001",
                expected=True,
            ),
            TestCase(
                name="invalid_init_time",
                filename="A2D09250600102516002",
                expected=False,
            ),
            TestCase(
                name="invalid_prefix",
                filename=f"GGC{test_it:%m%d%H%M}102516002",
                expected=False,
            ),
            TestCase(
                name="unexpected_extension",
                filename="A2D10251200102516001.nc",
                expected=False,
            ),
            TestCase(
                name="step_too_large",
                filename="A2D10251200102916001",
                expected=False,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.name):
                result = ECMWFRealTimeS3ModelRepository._wanted_file(
                    filename=t.filename,
                    it=test_it,
                    max_step=max(ECMWFRealTimeS3ModelRepository.model().expected_coordinates.step))
                self.assertEqual(result, t.expected)
