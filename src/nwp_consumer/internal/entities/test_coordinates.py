import dataclasses
import datetime as dt
import unittest

import pandas as pd
from returns.result import Failure, Success

from .coordinates import NWPDimensionCoordinateMap
from .parameters import Parameter


class TestCoordinates(unittest.TestCase):
    """Test the business methods of the NWPDimensionCoordinateMap class."""

    def test_determine_region(self) -> None:
        @dataclasses.dataclass
        class TestCase:
            name: str
            inner: NWPDimensionCoordinateMap
            expected_slices: dict[str, slice]
            should_error: bool

        outer: NWPDimensionCoordinateMap = NWPDimensionCoordinateMap(
            init_time=[dt.datetime(2021, 1, 1, i, tzinfo=dt.UTC) for i in range(0, 9, 3)],
            step=list(range(12)),
            variable=[
                Parameter.TEMPERATURE_SL,
                Parameter.CLOUD_COVER_HIGH,
                Parameter.TOTAL_PRECIPITATION_RATE_GL,
            ],
            latitude=[60.0, 61.0, 62.0],
            longitude=[10.0, 11.0, 12.0],
        )

        tests = [
            TestCase(
                name="basic_subset",
                inner=NWPDimensionCoordinateMap(
                    init_time=outer.init_time[:1],
                    step=outer.step[:6],
                    variable=outer.variable,
                    latitude=outer.latitude,
                    longitude=outer.longitude,
                ),
                expected_slices={
                    "init_time": slice(0, 1),
                    "step": slice(0, 6),
                    "variable": slice(0, 3),
                    "latitude": slice(0, 3),
                    "longitude": slice(0, 3),
                },
                should_error=False,
            ),
            TestCase(
                name="subset_with_multiple_span",
                inner=NWPDimensionCoordinateMap(
                    init_time=[dt.datetime(2021, 1, 1, i, tzinfo=dt.UTC) for i in [3, 6]],
                    step=outer.step,
                    variable=outer.variable,
                    latitude=outer.latitude,
                    longitude=outer.longitude,
                ),
                expected_slices={
                    "init_time": slice(1, 3),
                    "step": slice(0, 12),
                    "variable": slice(0, 3),
                    "latitude": slice(0, 3),
                    "longitude": slice(0, 3),
                },
                should_error=False,
            ),
            TestCase(
                name="subset_with_non_contiguous_values",
                inner=NWPDimensionCoordinateMap(
                    init_time=outer.init_time[:1],
                    step=list(range(1, 6, 2)),
                    variable=outer.variable,
                    latitude=[60.0, 63.0],
                    longitude=outer.longitude,
                ),
                expected_slices={},
                should_error=True,
            ),
            TestCase(
                name="not_a_subset",
                inner=NWPDimensionCoordinateMap(
                    init_time=outer.init_time[:1],
                    step=[15],
                    variable=outer.variable,
                    latitude=[12, 13, 14, 15],
                    longitude=outer.longitude,
                ),
                expected_slices={},
                should_error=True,
            ),
            TestCase(
                name="different_dimensions",
                inner=NWPDimensionCoordinateMap(
                    init_time=outer.init_time[:1],
                    step=[15],
                    variable=outer.variable,
                ),
                expected_slices={},
                should_error=True,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.name):
                result = outer.determine_region(inner=t.inner)
                if t.should_error:
                    self.assertTrue(
                        isinstance(result, Failure),
                        msg=f"{t.name}: Expected error to be returned.",
                    )
                else:
                    self.assertEqual(result, Success(t.expected_slices))

    def test_to_pandas(self) -> None:
        @dataclasses.dataclass
        class TestCase:
            name: str
            coords: NWPDimensionCoordinateMap
            expected_indexes: dict[str, pd.Index]

        tests = [
            TestCase(
                name="valid_data",
                coords=NWPDimensionCoordinateMap(
                    init_time=[dt.datetime(2021, 1, 1, i, tzinfo=dt.UTC) for i in range(0, 9, 3)],
                    step=list(range(12)),
                    variable=[
                        Parameter.CLOUD_COVER_HIGH,
                        Parameter.TEMPERATURE_SL,
                        Parameter.TOTAL_PRECIPITATION_RATE_GL,
                    ],
                    latitude=[60.0, 61.0, 62.0],
                    longitude=[10.0, 11.0, 12.0],
                ),
                expected_indexes={
                    "init_time": pd.to_datetime(
                        [
                            "2021-01-01T00:00:00Z",
                            "2021-01-01T03:00:00Z",
                            "2021-01-01T06:00:00Z",
                        ],
                    ),
                    "step": pd.Index([hour * 60 * 60 * 1000000000 for hour in range(12)]),
                    "variable": pd.Index(
                        [
                            Parameter.CLOUD_COVER_HIGH.value,
                            Parameter.TEMPERATURE_SL.value,
                            Parameter.TOTAL_PRECIPITATION_RATE_GL.value,
                        ],
                    ),
                    "latitude": pd.Index([62.0, 61.0, 60.0]),
                    "longitude": pd.Index([10.0, 11.0, 12.0]),
                },
            ),
        ]

        for t in tests:
            with self.subTest(name=t.name):
                result = t.coords.to_pandas()
                self.assertEqual(result["init_time"].dtype, "datetime64[ns]")
                self.assertListEqual(list(result.keys()), list(t.expected_indexes.keys()))
                for key in result:
                    self.assertListEqual(
                        result[key].values.tolist(), t.expected_indexes[key].values.tolist(),
                    )

    def test_from_pandas(self) -> None:
        @dataclasses.dataclass
        class TestCase:
            name: str
            data: dict[str, pd.Index]
            expected_coordinates: NWPDimensionCoordinateMap | None
            should_error: bool

        tests = [
            TestCase(
                name="valid_data",
                data={
                    "init_time": pd.to_datetime(["2021-01-01T00:00:00Z", "2021-01-01T03:00:00Z"]),
                    "step": pd.to_timedelta(["0 days", "3 days"]),
                    "variable": pd.Index(["temperature_sl", "cloud_cover_high"]),
                    "latitude": pd.Index([61.0, 60.0]),
                    "longitude": pd.Index([10.0, 11.0]),
                },
                expected_coordinates=NWPDimensionCoordinateMap(
                    init_time=[
                        dt.datetime(2021, 1, 1, 0, tzinfo=dt.UTC),
                        dt.datetime(2021, 1, 1, 3, tzinfo=dt.UTC),
                    ],
                    step=[0, 72],
                    variable=[Parameter.TEMPERATURE_SL, Parameter.CLOUD_COVER_HIGH],
                    latitude=[60.0, 61.0],
                    longitude=[10.0, 11.0],
                ),
                should_error=False,
            ),
            TestCase(
                name="missing_required_keys",
                data={
                    "init_time": pd.to_datetime(["2021-01-01T00:00:00Z", "2021-01-01T03:00:00Z"]),
                    "step": pd.to_timedelta(["0 days", "3 days"]),
                    "latitude": pd.Index([61.0, 60.0]),
                    "longitude": pd.Index([10.0, 11.0]),
                },
                expected_coordinates=None,
                should_error=True,
            ),
            TestCase(
                name="unknown_parameter",
                data={
                    "init_time": pd.to_datetime(["2021-01-01T00:00:00Z", "2021-01-01T03:00:00Z"]),
                    "step": pd.to_timedelta(["0 hours", "1 hours", "2 hours", "3 hours"]),
                    "variable": pd.Index(["temperature_sl", "not_a_variable"]),
                    "latitude": pd.Index([61.0, 60.0]),
                    "longitude": pd.Index([10.0, 11.0], dtype="int64"),
                },
                expected_coordinates=None,
                should_error=True,
            ),
            TestCase(
                name="unknown_keys",
                data={
                    "init_time": pd.to_datetime(["2021-01-01T00:00:00Z", "2021-01-01T03:00:00Z"]),
                    "step": pd.to_timedelta(["0 days", "3 days"]),
                    "variable": pd.Index(["temperature_sl", "cloud_cover_high"]),
                    "latitude": pd.Index([61.0, 60.0]),
                    "longitude": pd.Index([10.0, 11.0]),
                    "unknown": pd.Index(["unknown"]),
                },
                expected_coordinates=None,
                should_error=True,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.name):
                result = NWPDimensionCoordinateMap.from_pandas(t.data)
                if t.should_error:
                    self.assertTrue(
                        isinstance(result, Failure),
                        msg=f"{t.name}: Expected error to be returned.",
                    )
                else:
                    self.assertEqual(result, Success(t.expected_coordinates))

    def test_crop(self) -> None:
        """Test the crop method of the NWPDimensionCoordinateMap class."""

        @dataclasses.dataclass
        class Crop:
            n: float
            w: float
            s: float
            e: float

        @dataclasses.dataclass
        class TestCase:
            name: str
            coords: NWPDimensionCoordinateMap
            crop: Crop
            expected_latitude: list[float]
            expected_longitude: list[float]
            should_error: bool

        test_coords = NWPDimensionCoordinateMap(
            init_time=[dt.datetime(2021, 1, 1, i, tzinfo=dt.UTC) for i in range(0, 9, 3)],
            step=list(range(12)),
            variable=[
                Parameter.TEMPERATURE_SL,
                Parameter.CLOUD_COVER_HIGH,
                Parameter.TOTAL_PRECIPITATION_RATE_GL,
            ],
            latitude=[float(f"{lat / 10:.2f}") for lat in range(800, -800, -1)],
            longitude=[float(f"{lon / 10:.2f}") for lon in range(150, -150, -1)],
        )

        test_cases = [
            TestCase(
                name="basic_crop",
                coords=test_coords,
                crop=Crop(n=60.5, w=10.0, s=60, e=10.5),
                expected_latitude=[60.5, 60.4, 60.3, 60.2, 60.1, 60.0],
                expected_longitude=[10.0, 10.1, 10.2, 10.3, 10.4, 10.5],
                should_error=False,
            ),
            TestCase(
                name="crop_with_invalid_values",
                coords=test_coords,
                crop=Crop(n=60.0, w=10.0, s=61.0, e=12.0),
                expected_latitude=[60.0],
                expected_longitude=[10.0, 11.0],
                should_error=True,
            ),
            TestCase(
                name="crop_with_larger_values",
                coords=test_coords,
                crop=Crop(n=90.0, w=180, s=-90, e=-180),
                expected_latitude=test_coords.latitude if test_coords.latitude else [90.0],
                expected_longitude=test_coords.longitude if test_coords.longitude else [180.0],
                should_error=True,
            ),
        ]

        for t in test_cases:
            with self.subTest(name=t.name):
                result = t.coords.crop(
                    north=t.crop.n,
                    west=t.crop.w,
                    south=t.crop.s,
                    east=t.crop.e,
                )
                if t.should_error:
                    self.assertIsInstance(result, Failure)
                else:
                    self.assertIsInstance(result, Success)
                    coords = result.unwrap()
                    self.assertListEqual(coords.latitude, t.expected_latitude)  # type: ignore
                    self.assertListEqual(coords.longitude, t.expected_longitude)  # type: ignore


if __name__ == "__main__":
    unittest.main()
