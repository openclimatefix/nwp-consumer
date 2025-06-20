import dataclasses
import datetime as dt
import os
import pathlib
import unittest
from unittest.mock import patch

from returns.result import Failure, ResultE, Success

from ...entities import NWPDimensionCoordinateMap, Parameter
from .mo_datahub import MetOfficeDatahubRawRepository


class TestMetOfficeDatahubRawRepository(unittest.TestCase):
    """Test the business methods of the MetOfficeDatahubRawRepository class."""

    @unittest.skipIf(
        condition="CI" in os.environ,
        reason="Skipping integration test that requires MetOffice DataHub access.",
    )
    def test__download(self) -> None:
        """Test the _download method."""

        auth_result = MetOfficeDatahubRawRepository.authenticate()
        self.assertIsInstance(auth_result, Success, msg=f"{auth_result!s}")
        c = auth_result.unwrap()

        test_it = c.repository().determine_latest_it_from(
            dt.datetime.now(tz=dt.UTC),
            c.model().running_hours,
        )

        dl_result = c._download(
            f"{c.request_url}/agl_u-component-of-wind-surface-adjusted_10.0_{test_it:%Y%m%d%H}_1/data",
        )

        self.assertIsInstance(dl_result, Success, msg=f"{dl_result!s}")

    def test_convert(self) -> None:
        """Test the _convert method."""

        @dataclasses.dataclass
        class TestCase:
            filename: str
            expected_coords: NWPDimensionCoordinateMap
            should_error: bool

        tests: list[TestCase] = [
            TestCase(
                filename="test_MODatahub_UM-Global_t2m_20241120T00_S00.grib",
                expected_coords=dataclasses.replace(
                    MetOfficeDatahubRawRepository.model().expected_coordinates,
                    init_time=[dt.datetime(2024, 11, 20, 0, tzinfo=dt.UTC)],
                    variable=[Parameter.TEMPERATURE_SL],
                    step=[0],
                ),
                should_error=False,
            ),
            TestCase(
                filename="test_MODatahub_UM-Global_u10_20241120T00_S17.grib",
                expected_coords=dataclasses.replace(
                    MetOfficeDatahubRawRepository.model().expected_coordinates,
                    init_time=[dt.datetime(2024, 11, 20, 0, tzinfo=dt.UTC)],
                    variable=[Parameter.WIND_U_COMPONENT_10m],
                    step=[17],
                ),
                should_error=False,
            ),
            TestCase(
                filename="test_HRES-IFS_10u.grib",
                expected_coords=MetOfficeDatahubRawRepository.model().expected_coordinates,
                should_error=True,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.filename):
                # Attempt to convert the file
                result = MetOfficeDatahubRawRepository._convert_global(
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

    @patch.dict(os.environ, {"MODEL": "um-ukv-2km"}, clear=True)
    def test_convert_ukv(self) -> None:
        @dataclasses.dataclass
        class TestCase:
            filename: str
            expected_coords: NWPDimensionCoordinateMap
            should_error: bool

        tests: list[TestCase] = [
            TestCase(
                filename="test_MO_UKV_agl_relative-humidity_1.5_2025012112.grib",
                expected_coords=dataclasses.replace(
                    MetOfficeDatahubRawRepository.model().expected_coordinates,
                    init_time=[dt.datetime(2025, 1, 21, 12, tzinfo=dt.UTC)],
                    variable=[Parameter.TEMPERATURE_SL],
                    step=[0],
                ),
                should_error=False,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.filename):
                # Attempt to convert the file
                result = MetOfficeDatahubRawRepository._convert_ukv(
                    path=pathlib.Path(__file__).parent.absolute() / "test_gribs" / t.filename,
                )
                region_result: ResultE[NWPDimensionCoordinateMap] = result.do(
                    region
                    for das in result
                    for da in das
                    for region in NWPDimensionCoordinateMap.from_xarray(da)
                )
                if t.should_error:
                    self.assertIsInstance(region_result, Failure, msg=f"{region_result}")
                else:
                    self.assertIsInstance(region_result, Success, msg=f"{region_result}")
