import dataclasses
import datetime as dt
import os
import pathlib
import unittest
from unittest.mock import patch

from returns.result import Failure, ResultE, Success

from nwp_consumer.internal import entities

from ...entities import NWPDimensionCoordinateMap
from .ceda import CEDARawRepository


@dataclasses.dataclass
class TestCase:
    filename: str
    should_error: bool
    expected_coords: entities.NWPDimensionCoordinateMap


class TestCEDARawRepository(unittest.TestCase):
    """Test the business methods of the CEDAFTPRawRepository class."""

    def test__convert_global(self) -> None:
        """Test the _convert_global method."""

        tests: list[TestCase] = [
            TestCase(
                filename="test_CEDAFTP_UM-Global_ssrd_20241105T00_S01-03.grib",
                expected_coords=dataclasses.replace(
                    entities.Models.MO_UM_GLOBAL_17KM.expected_coordinates,
                    init_time=[dt.datetime(2024, 11, 5, 0, tzinfo=dt.UTC)],
                    step=[1, 2, 3],
                    variable=[entities.parameters.Parameter.DOWNWARD_SHORTWAVE_RADIATION_FLUX_GL],
                ),
                should_error=False,
            ),
            TestCase(
                filename="test_CEDAFTP_UM-Global_u_20241105T00_S01-03_AreaC.grib",
                expected_coords=dataclasses.replace(
                    entities.Models.MO_UM_GLOBAL_17KM.expected_coordinates,
                    init_time=[dt.datetime(2024, 11, 5, 0, tzinfo=dt.UTC)],
                    step=[1, 2, 3],
                    variable=[entities.parameters.Parameter.WIND_U_COMPONENT_10m],
                ),
                should_error=False,
            ),
            TestCase(
                filename="test_MODatahub_UM-Global_t2m_20241120T00_S00.grib",
                expected_coords=entities.Models.MO_UM_GLOBAL_17KM.expected_coordinates,
                should_error=True,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.filename):
                # Attempt to convert the file
                result = CEDARawRepository._convert_global(
                    pathlib.Path(__file__).parent.absolute() / "test_gribs" / t.filename,
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

    @patch.dict(os.environ, {"MODEL": "mo-um-ukv"}, clear=True)
    def test__convert_ukv(self) -> None:
        """Test the _convert_ukv method."""
        tests: list[TestCase] = [
            TestCase(
                filename="test_subset_Wholesale1.grib",
                expected_coords=dataclasses.replace(
                    entities.Models.MO_UM_UKV_2KM_OSGB.expected_coordinates,
                    init_time=[dt.datetime(2022, 1, 1, 3, tzinfo=dt.UTC)],
                    step=[1, 2],
                    variable=[entities.parameters.Parameter.TEMPERATURE_SL],
                ),
                should_error=False,
            ),
            TestCase(
                filename="test_subset_Wholesale2.grib",
                expected_coords=dataclasses.replace(
                    entities.Models.MO_UM_UKV_2KM_OSGB.expected_coordinates,
                    init_time=[dt.datetime(2022, 1, 1, 3, tzinfo=dt.UTC)],
                    step=[1, 2],
                    variable=[entities.parameters.Parameter.CLOUD_COVER_LOW],
                ),
                should_error=False,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.filename):
                # Attempt to convert the file
                result = CEDARawRepository._convert_ukv(
                    pathlib.Path(__file__).parent.absolute() / "test_gribs" / t.filename,
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
