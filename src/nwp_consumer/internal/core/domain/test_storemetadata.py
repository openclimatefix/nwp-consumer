import dataclasses
import pathlib
import unittest

import numpy as np
from returns.result import Failure, Success

from .repometadata import (
    LabelCoordinateDict,
    StoreMetadata,
)


class TestTensorDimensionMap(unittest.TestCase):
    def test_determine_region(self) -> None:
        """Test the as_slices_of method of the TensorDimensionMap class."""

        @dataclasses.dataclass
        class TestContainer:
            name: str
            inner: LabelCoordinateDict
            expected_slices: dict[str, slice]
            should_error: bool

        smd: StoreMetadata = StoreMetadata(
            coordinate_map={
                "init_time": [np.datetime64(f"2021-01-01T0{i}:00:00") for i in range(0, 9, 3)],
                "step": [np.timedelta64(i, "h") for i in range(12)],
                "latitude": [60.0, 61.0, 62.0],
                "longitude": [10.0, 11.0, 12.0],
            },
            path=pathlib.Path(""),
        )

        tests = [
            TestContainer(
                name="basic_subset",
                inner={
                    "init_time": [np.datetime64("2021-01-01T00:00:00")],
                    "step": [np.timedelta64(i, "h") for i in range(6)],
                    "latitude": [60.0, 61.0, 62.0],
                    "longitude": [10.0, 11.0, 12.0],
                },
                expected_slices={
                    "init_time": slice(0, 1),
                    "step": slice(0, 6),
                    "latitude": slice(0, 3),
                    "longitude": slice(0, 3),
                },
                should_error=False,
            ),
            TestContainer(
                name="subset_with_multiple_span",
                inner={
                    "init_time": [
                        np.datetime64("2021-01-01T03:00:00"),
                        np.datetime64("2021-01-01T06:00:00"),
                    ],
                    "step": [np.timedelta64(i, "h") for i in range(12)],
                    "latitude": [60.0, 61.0, 62.0],
                    "longitude": [10.0, 11.0, 12.0],
                },
                expected_slices={
                    "init_time": slice(1, 3),
                    "step": slice(0, 12),
                    "latitude": slice(0, 3),
                    "longitude": slice(0, 3),
                },
                should_error=False,
            ),
            TestContainer(
                name="subset_with_non_contiguous_values",
                inner={
                    "init_time": [np.datetime64("2021-01-01T00:00:00")],
                    "step": [np.timedelta64(i, "h") for i in range(1, 6, 2)],
                    "latitude": [60.0, 63.0],
                    "longitude": [10.0, 11.0, 12.0],
                },
                expected_slices={},
                should_error=True,
            ),
            TestContainer(
                name="not_a_subset",
                inner={
                    "init_time": [np.datetime64("2021-01-01T00:00:00")],
                    "step": [np.timedelta64(15, "h")],
                    "latitude": [60.0, 61.0, 62.0, 64.0],
                    "longitude": [10.0, 11.0, 12.0],
                },
                expected_slices={},
                should_error=True,
            ),
            TestContainer(
                name="different_dimensions",
                inner={
                    "init_time": [np.datetime64("2021-01-01T00:00:00")],
                    "step": [np.timedelta64(15, "h")],
                    "x": [60.0, 61.0, 62.0, 64.0],
                    "y": [10.0, 11.0, 12.0],
                },
                expected_slices={},
                should_error=True,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.name):
                result = smd.determine_region(inner=t.inner)
                if t.should_error:
                    self.assertTrue(isinstance(result, Failure), msg="Expected error to be returned.")
                else:
                    self.assertEqual(result, Success(t.expected_slices))


if __name__ == "__main__":
    unittest.main()
