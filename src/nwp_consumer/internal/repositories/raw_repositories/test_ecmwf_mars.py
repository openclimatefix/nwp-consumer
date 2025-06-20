import dataclasses
import datetime as dt
import os
import pathlib
import unittest
from unittest.mock import patch

from returns.result import Failure, ResultE, Success

from ...entities import NWPDimensionCoordinateMap
from .ecmwf_mars import ECMWFMARSRawRepository


class TestECMWFMARSEModelREpository(unittest.TestCase):
    """Test the business methods of the ECMWFRealTimeS3RawRepository class."""

    @patch.dict(os.environ, {"MODEL": "ens-stat-uk"}, clear=True)
    def test__convert(self) -> None:
        """Test the _convert method."""

        @dataclasses.dataclass
        class TestCase:
            filename: str
            expected_coords: NWPDimensionCoordinateMap
            should_error: bool

        tests: list[TestCase] = [
            TestCase(
                filename="test_ECMWFMARS_enfo-em_t2m-si10-si100-msp_20240101T00_S03-06.grib",
                expected_coords=dataclasses.replace(
                    ECMWFMARSRawRepository.model().expected_coordinates,
                    init_time=[dt.datetime(2024, 1, 1, 0, tzinfo=dt.UTC)],
                    ensemble_stat=["mean"],
                    step=[3, 6],
                ),
                should_error=False,
            ),
            TestCase(
                filename="test_ECMWFMARS_enfo-es_t2m-si10-si100-msp_20240101T00_S03-06.grib",
                expected_coords=dataclasses.replace(
                    ECMWFMARSRawRepository.model().expected_coordinates,
                    init_time=[dt.datetime(2024, 1, 1, 0, tzinfo=dt.UTC)],
                    step=[3, 6],
                ),
                should_error=False,
            ),
            TestCase(
                filename="test_NOAAS3_HRES-GFS_10u_20210509T06_S00.grib",
                expected_coords=ECMWFMARSRawRepository.model().expected_coordinates,
                should_error=True,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.filename):
                result = ECMWFMARSRawRepository._convert(
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
