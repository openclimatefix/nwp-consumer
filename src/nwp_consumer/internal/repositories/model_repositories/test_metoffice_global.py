import dataclasses
import datetime as dt
import os
import unittest

from returns.pipeline import flow, is_successful
from returns.pointfree import bind

from nwp_consumer.internal import entities

from .metoffice_global import CEDAFTPModelRepository


class TestCEDAFTPModelRepository(unittest.TestCase):
    """Test the business methods of the CEDAFTPModelRepository class."""

    @unittest.skipIf(
        condition="CI" in os.environ,
        reason="Skipping integration test that requires FTP access.",
    )
    def test__download_and_convert(self) -> None:
        """Test the _download_and_convert method."""

        auth_result = CEDAFTPModelRepository.authenticate()
        self.assertTrue(is_successful(auth_result), msg=f"Error: {auth_result.failure}")
        c = auth_result.unwrap()

        test_it: dt.datetime = dt.datetime(2021, 1, 1, 0, tzinfo=dt.UTC)
        test_coordinates: entities.NWPDimensionCoordinateMap = dataclasses.replace(
            c.model().expected_coordinates,
            init_time=[test_it],
        )

        @dataclasses.dataclass
        class TestCase:
            area: str
            crop: str | None = None

            @property
            def url(self) -> str:
                return "".join(
                    (
                        c.url_base,
                        f"/{test_it:%Y/%m/%d}",
                        f"/{test_it:%Y%m%d%H}_WSGlobal17km_Total_Downward_Surface_SW_Flux_{self.area}_000144.grib",
                    ),
                )

        tests = [
            TestCase(area="AreaC", crop="east"),
            TestCase(area="AreaG", crop="west"),
            TestCase(area="AreaE"),
        ]

        for test in tests:
            with (self.subTest(area=test.area)):
                result = c._download_and_convert(test.url)

                self.assertTrue(is_successful(result), msg=f"Error: {result}")

                for da in result.unwrap():
                    # Check resultant arrays are a subset of the expected coordinates
                    subset_result = flow(
                        da,
                        entities.NWPDimensionCoordinateMap.from_xarray,
                        bind(test_coordinates.determine_region),
                    )

                    self.assertTrue(
                        is_successful(subset_result),
                        msg=f"Error: {subset_result}",
                    )


if __name__ == "__main__":
    unittest.main()
