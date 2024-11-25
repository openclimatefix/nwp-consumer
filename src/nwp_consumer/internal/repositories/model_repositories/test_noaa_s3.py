import dataclasses
import datetime as dt
import os
import unittest
from typing import TYPE_CHECKING

import s3fs
from returns.pipeline import is_successful

from ...entities import NWPDimensionCoordinateMap
from .noaa_s3 import NOAAS3ModelRepository

if TYPE_CHECKING:
    import xarray as xr

    from nwp_consumer.internal import entities


class TestECMWFRealTimeS3ModelRepository(unittest.TestCase):
    """Test the business methods of the ECMWFRealTimeS3ModelRepository class."""

    @unittest.skipIf(
        condition="CI" in os.environ,
        reason="Skipping integration test that requires S3 access.",
    )  # TODO: Move into integration tests, or remove
    def test__download_and_convert(self) -> None:
        """Test the _download_and_convert method."""

        c: NOAAS3ModelRepository = NOAAS3ModelRepository.authenticate().unwrap()

        test_it: dt.datetime = dt.datetime(2024, 10, 24, 12, tzinfo=dt.UTC)
        test_coordinates: entities.NWPDimensionCoordinateMap = dataclasses.replace(
            c.model().expected_coordinates,
            init_time=[test_it],
        )

        fs = s3fs.S3FileSystem(anon=True)
        bucket_path: str = f"noaa-gfs-bdp-pds/gfs.{test_it:%Y%m%d}/{test_it:%H}/atmos"
        urls: list[str] = [
            f"s3://{f}"
            for f in fs.ls(bucket_path)
            if c._wanted_file(
                filename=f.split("/")[-1],
                it=test_it,
                max_step=max(c.model().expected_coordinates.step),
            )
        ]

        for url in urls:
            with self.subTest(url=url):
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
                filename=f"gfs.t{test_it:%H}z.pgrb2.1p00.f000",
                expected=True,
            ),
            TestCase(
                name="invalid_init_time",
                filename="gfs.t02z.pgrb2.1p00.f000",
                expected=False,
            ),
            TestCase(
                name="invalid_prefix",
                filename=f"gfs.t{test_it:%H}z.pgrb2.0p20.f006",
                expected=False,
            ),
            TestCase(
                name="unexpected_extension",
                filename=f"gfs.t{test_it:%H}z.pgrb2.1p00.f030.nc",
                expected=False,
            ),
            TestCase(
                name="step_too_large",
                filename=f"gfs.t{test_it:%H}z.pgrb2.1p00.f049",
                expected=False,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.name):
                result = NOAAS3ModelRepository._wanted_file(
                    filename=t.filename,
                    it=test_it,
                    max_step=max(NOAAS3ModelRepository.model().expected_coordinates.step),
                )
                self.assertEqual(result, t.expected)

