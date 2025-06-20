"""Tests for the ModelMetadata class."""

import dataclasses
import unittest

from hypothesis import given
from hypothesis import strategies as st

from .coordinates import NWPDimensionCoordinateMap
from .modelmetadata import ModelMetadata, Models
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
            running_hours=[0, 6, 12, 18],
        )

        @dataclasses.dataclass
        class TestCase:
            region: str
            expected_name: str
            expected_lat: list[float]
            expected_lon: list[float]

        test_cases: list[TestCase] = [
            TestCase(
                region="uk",
                expected_name="test_uk",
                expected_lat=[float(f"{lat / 10:.2f}") for lat in range(620, 480 - 1, -1)],
                expected_lon=[float(f"{lon / 10:.2f}") for lon in range(-120, 30 + 1, 1)],
            ),
            TestCase(
                region="india",
                expected_name="test_india",
                expected_lat=[float(f"{lat / 10:.2f}") for lat in range(350, 60 - 1, -1)],
                expected_lon=[float(f"{lon / 10:.2f}") for lon in range(670, 970 + 1, 1)],
            ),
            TestCase(
                region="invalid",
                expected_name="test",
                expected_lat=[float(f"{lat / 10:.2f}") for lat in range(900, -900 - 1, -1)],
                expected_lon=[float(f"{lon / 10:.2f}") for lon in range(-1800, 1800 + 1, 1)],
            ),
        ]

        for test_case in test_cases:
            new_metadata = metadata.with_region(test_case.region)
            self.assertEqual(new_metadata.name, test_case.expected_name)
            self.assertListEqual(
                new_metadata.expected_coordinates.latitude,  # type: ignore
                test_case.expected_lat,
            )
            self.assertListEqual(
                new_metadata.expected_coordinates.longitude,  # type: ignore
                test_case.expected_lon,
            )

    @given(
        st.sampled_from([attr for attr in dir(Models) if not attr.startswith("__")]),
        st.sampled_from(["uk", "india", "invalid"]),
    )
    def hypothesis_test_with_region(self, model: ModelMetadata, region: str) -> None:
        """Assert that with_region does not raise an exception."""
        model.with_region(region)
