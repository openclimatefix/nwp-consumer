import dataclasses
import datetime as dt
import os
import pathlib
import unittest

from returns.pipeline import flow
from returns.pointfree import bind
from returns.result import Failure, ResultE, Success

from nwp_consumer.internal import entities

from ...entities import NWPDimensionCoordinateMap
from .ceda_ftp import CEDAFTPRawRepository


class TestCEDAFTPRawRepository(unittest.TestCase):
    """Test the business methods of the CEDAFTPRawRepository class."""

    @unittest.skipIf(
        condition="CI" in os.environ,
        reason="Skipping integration test that requires FTP access.",
    )
    def test__download_and_convert(self) -> None:
        """Test the _download_and_convert method."""

        auth_result = CEDAFTPRawRepository.authenticate()
        self.assertIsInstance(auth_result, Success, msg=f"{auth_result!s}")
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

                self.assertIsInstance(result, Success, msg=f"{result!s}")

                for da in result.unwrap():
                    # Check resultant arrays are a subset of the expected coordinates
                    subset_result = flow(
                        da,
                        entities.NWPDimensionCoordinateMap.from_xarray,
                        bind(test_coordinates.determine_region),
                    )

                    self.assertIsInstance(subset_result, Success, msg=f"{subset_result!s}")

    def test__convert(self) -> None:
        """Test the _convert method."""

        @dataclasses.dataclass
        class TestCase:
            filename: str
            should_error: bool
            expected_coords: entities.NWPDimensionCoordinateMap

        tests: list[TestCase] = [
            TestCase(
                filename="test_CEDAFTP_UM-Global_ssrd_20241105T00_S01-03.grib",
                expected_coords = dataclasses.replace(
                    CEDAFTPRawRepository.model().expected_coordinates,
                    init_time=[dt.datetime(2024, 11, 5, 0, tzinfo=dt.UTC)],
                    step=[1, 2, 3],
                    variable=[entities.parameters.Parameter.DOWNWARD_SHORTWAVE_RADIATION_FLUX_GL],
                ),
                should_error=False,
            ),
            TestCase(
                filename="test_CEDAFTP_UM-Global_u_20241105T00_S01-03_AreaC.grib",
                expected_coords = dataclasses.replace(
                    CEDAFTPRawRepository.model().expected_coordinates,
                    init_time=[dt.datetime(2024, 11, 5, 0, tzinfo=dt.UTC)],
                    step=[1, 2, 3],
                    variable=[entities.parameters.Parameter.WIND_U_COMPONENT_10m],
                ),
                should_error=False,
            ),
            TestCase(
                filename="test_MODatahub_UM-Global_t2m_20241120T00_S00.grib",
                expected_coords = CEDAFTPRawRepository.model().expected_coordinates,
                should_error=True,
            ),
        ]


        for t in tests:
            with self.subTest(name=t.filename):
                # Attempt to convert the file
                result = CEDAFTPRawRepository._convert(
                    path=pathlib.Path(__file__).parent.absolute() / "test_gribs" / t.filename,
                )
                region_result: ResultE[dict[str, slice]] = result.do(
                    region
                    for das in result
                    for da in das
                    for region in NWPDimensionCoordinateMap.from_xarray(da).bind(
                        t.expected_coords.determine_region,
                    )
                )
                if t.should_error:
                    self.assertIsInstance(region_result, Failure, msg=f"{region_result}")
                else:
                    self.assertIsInstance(region_result, Success, msg=f"{region_result}")

if __name__ == "__main__":
    unittest.main()
