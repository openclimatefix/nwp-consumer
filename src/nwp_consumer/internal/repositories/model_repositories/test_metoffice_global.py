import dataclasses
import datetime as dt
import os
import unittest

from returns.pipeline import is_successful

from nwp_consumer.internal import entities

from .metoffice_global import CedaMetOfficeGlobalModelRepository
from ...entities import NWPDimensionCoordinateMap


class TestCedaMetOfficeGlobalModelRepository(unittest.TestCase):
    """Test the business methods of the CedaMetOfficeGlobalModelRepository class."""

    @unittest.skipIf(
        condition="CI" in os.environ,
        reason="Skipping integration test that requires FTP access.",
    )
    def test__download_and_convert(self) -> None:
        """Test the _download_and_convert method."""

        c = CedaMetOfficeGlobalModelRepository()
        _ = c.authenticate()

        test_it: dt.datetime = dt.datetime(2021, 1, 1, 0, tzinfo=dt.UTC)
        test_coordinates: entities.NWPDimensionCoordinateMap = dataclasses.replace(
            c.metadata.expected_coordinates,
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
            with self.subTest(area=test.area):
                result = c._download_and_convert(test.url, region=test.crop)

                self.assertTrue(is_successful(result), msg=f"Error: {result}")

                # Check resultant array is a subset of the expected coordinates
                region_result = result.bind(
                    NWPDimensionCoordinateMap.from_xarray,
                ).bind(
                    test_coordinates.determine_region,
                )
                self.assertTrue(is_successful(region_result), msg=f"Error: {region_result}")

