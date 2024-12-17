"""Tests for the ModelMetadata class."""

import dataclasses
import unittest

from returns.result import Failure, Success

from .coordinates import NWPDimensionCoordinateMap
from .modelmetadata import ModelMetadata
from .parameters import Parameter


class TestModelMetadata(unittest.TestCase):
    """Tests for the ModelMetadata class."""

    def test_with_region(self) -> None:
        """Test the with_region method."""
        metadata: ModelMetadata = ModelMetadata(
            name="test",
            resolution="0.1 degrees",
            expected_coordinates=NWPDimensionCoordinateMap(
                init_time=[],
                step=list(range(0, 85, 1)),
                variable=[Parameter.WIND_U_COMPONENT_10m],
                latitude=[float(f"{lat / 10:.2f}") for lat in range(900, -900 - 1, -1)],
                longitude=[float(f"{lon / 10:.2f}") for lon in range(-1800, 1800 + 1, 1)],
            ),
        )

        @dataclasses.dataclass
        class TestCase:
            region: str
            expected_name: str
            expected_lat: list[float]
            expected_lon: list[float]
            should_error: bool


        test_cases: list[TestCase] = [
            TestCase(
                region="uk",
                expected_name="test_uk",
                expected_lat=[float(f"{lat / 10:.2f}") for lat in range(620, 480 - 1, -1)],
                expected_lon=[float(f"{lon / 10:.2f}") for lon in range(-120, 30 + 1, 1)],
                should_error=False,
            ),
            TestCase(
                region="india",
                expected_name="test_india",
                expected_lat=[float(f"{lat / 10:.2f}") for lat in range(350, 60 - 1, -1)],
                expected_lon=[float(f"{lon / 10:.2f}") for lon in range(670, 970 + 1, 1)],
                should_error=False,
            ),
            TestCase(
                region="invalid",
                expected_name="test",
                expected_lat=[float(f"{lat / 10:.2f}") for lat in range(900, -900 - 1, -1)],
                expected_lon=[float(f"{lon / 10:.2f}") for lon in range(-1800, 1800 + 1, 1)],
                should_error=False,
            ),
        ]

        for test_case in test_cases:
            new_metadata_result = metadata.with_region(test_case.region)
            if test_case.should_error:
                self.assertIsInstance(new_metadata_result, Failure)
            else:
                self.assertIsInstance(new_metadata_result, Success)
                new_metadata = new_metadata_result.unwrap()
                self.assertEqual(new_metadata.name, test_case.expected_name)
                self.assertListEqual(
                        new_metadata.expected_coordinates.latitude, # type: ignore
                    test_case.expected_lat,
                )
                self.assertListEqual(
                        new_metadata.expected_coordinates.longitude, # type: ignore
                    test_case.expected_lon,
                )

