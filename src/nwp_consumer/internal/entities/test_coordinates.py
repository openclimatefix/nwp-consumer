import dataclasses
import datetime as dt
import unittest

from returns.result import Failure, Success

from .coordinates import NWPDimensionCoordinateMap, determine_region, params, to_pandas


class TestCoordinates(unittest.TestCase):

    def test_determine_region(self) -> None:

        @dataclasses.dataclass
        class TestContainer:
            name: str
            inner: NWPDimensionCoordinateMap
            expected_slices: dict[str, slice]
            should_error: bool

        outer: NWPDimensionCoordinateMap = {
            "init_time": [dt.datetime(2021, 1, 1, i, tzinfo=dt.UTC) for i in range(0, 9, 3)],
            "step": list(range(12)),
            "variable": [
                params.temperature_sl,
                params.cloud_cover_high,
                params.total_precipitation_rate_gl,
            ],
            "latitude": [60.0, 61.0, 62.0],
            "longitude": [10.0, 11.0, 12.0],
        }

        tests = [
            TestContainer(
                name="basic_subset",
                inner={
                    "init_time": outer["init_time"][:1],
                    "step": outer["step"][:6],
                    "variable": outer["variable"],
                    "latitude": outer["latitude"],
                    "longitude": outer["longitude"],
                },
                expected_slices={
                    "init_time": slice(0, 1),
                    "step": slice(0, 6),
                    "variable": slice(0, 3),
                    "latitude": slice(0, 3),
                    "longitude": slice(0, 3),
                },
                should_error=False,
            ),
            TestContainer(
                name="subset_with_multiple_span",
                inner={
                    "init_time": [
                        dt.datetime(2021, 1, 1, i, tzinfo=dt.UTC)
                        for i in [3, 6]
                    ],
                    "step": outer["step"],
                    "variable": outer["variable"],
                    "latitude": outer["latitude"],
                    "longitude": outer["longitude"],
                },
                expected_slices={
                    "init_time": slice(1, 3),
                    "step": slice(0, 12),
                    "variable": slice(0, 3),
                    "latitude": slice(0, 3),
                    "longitude": slice(0, 3),
                },
                should_error=False,
            ),
            TestContainer(
                name="subset_with_non_contiguous_values",
                inner={
                    "init_time": outer["init_time"][:1],
                    "step": list(range(1, 6, 2)),
                    "variable": outer["variable"],
                    "latitude": [60.0, 63.0],
                    "longitude": outer["longitude"],
                },
                expected_slices={},
                should_error=True,
            ),
            TestContainer(
                name="not_a_subset",
                inner={
                    "init_time": outer["init_time"][:1],
                    "step": [15],
                    "variable": outer["variable"],
                    "latitude": outer["latitude"] + [64.0],
                    "longitude": outer["longitude"],
                },
                expected_slices={},
                should_error=True,
            ),
            TestContainer(
                name="different_dimensions",
                inner={
                    "init_time": outer["init_time"][:1],
                    "step": [15],
                    "variable": outer["variable"],
                    "x": [60.0, 61.0, 62.0, 64.0],
                    "y": [10.0, 11.0, 12.0],
                },
                expected_slices={},
                should_error=True,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.name):
                result = determine_region(inner=t.inner, outer=outer)
                if t.should_error:
                    self.assertTrue(isinstance(result, Failure), msg="Expected error to be returned.")
                else:
                    self.assertEqual(result, Success(t.expected_slices))

    def test_to_pandas(self) -> None:
        coords: NWPDimensionCoordinateMap = {
            "init_time": [dt.datetime(2021, 1, 1, i, tzinfo=dt.UTC) for i in range(0, 9, 3)],
            "step": list(range(12)),
            "variable": [
                params.temperature_sl,
                params.cloud_cover_high,
                params.total_precipitation_rate_gl,
            ],
            "latitude": [60.0, 61.0, 62.0],
            "longitude": [10.0, 11.0, 12.0],
        }

        out = to_pandas(coords)

        self.assertEqual(out["init_time"].dtype, "datetime64[ns]")
        self.assertEqual(out["step"].dtype, "timedelta64[ns]")
        self.assertEqual(out["variable"].dtype, "object")
        self.assertEqual(out["latitude"].dtype, "float64")
        self.assertEqual(out["longitude"].dtype, "float64")

    def test_from_pandas(self) -> None:
        # TODO: Implement this test
        pass



if __name__ == "__main__":
    unittest.main()
