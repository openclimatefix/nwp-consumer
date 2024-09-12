import dataclasses
import datetime as dt
import unittest

import numpy as np
from returns.pipeline import is_successful
from returns.result import Failure, Success
import xarray as xr

from . import NWPDimensionCoordinateMap
from .parameters import params
from .postprocess import PostProcessOptions
from .tensorstore import TensorStore


class TestTensorStore(unittest.TestCase):
    """Test the business methods of the TensorStore class."""

    test_coords: NWPDimensionCoordinateMap
    test_store: TensorStore

    def setUp(self) -> None:
        self.test_coords = NWPDimensionCoordinateMap(
            init_time=[dt.datetime(2021, 1, 1, h, tzinfo=dt.UTC) for h in [0, 6, 12, 18]],
            step=[1, 2, 3, 4],
            variable=[params.temperature_sl],
            latitude=np.linspace(90, -90, 12).tolist(),
            longitude=np.linspace(0, 360, 18).tolist(),
        )

        init_result = TensorStore.initialize_empty_store(
            name="test_da",
            coords=self.test_coords,
        )
        match init_result:
            case Success(store):
                self.test_store = store
            case Failure(e):
                raise ValueError(f"Failed to initialize test store: {e}.")


    def test_initialize_empty_store(self) -> None:
        """Test the initialize_empty_store method."""
        # TODO
        pass

    def test_write_to_region(self) -> None:
        """Test the write_to_region method."""
        # TODO
        pass

    def test_postprocess(self) -> None:
        """Test the postprocess method."""

        @dataclasses.dataclass
        class TestCase:
            name: str
            options: PostProcessOptions
            should_error: bool

        tests: list[TestCase] = [
            TestCase(
                name="empty_options",
                options=PostProcessOptions(),
                should_error=False,
            ),
            TestCase(
                name="standardize_coordinates",
                options=PostProcessOptions(
                    standardize_coordinates=True,
                ),
                should_error=False,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.name):
                result = self.test_store.postprocess(t.options)
                if t.should_error:
                    self.assertTrue(
                        isinstance(result, Exception),
                        msg="Expected error to be returned.",
                    )
                else:
                    self.assertTrue(is_successful(result))

    def test_missing_times(self) -> None:
        """Test the missing_times method."""

        @dataclasses.dataclass
        class TestCase:
            name: str
            times_to_write: list[dt.datetime]
            expected: list[dt.datetime]

        tests: list[TestCase] = [
            TestCase(
                name="all_missing_times",
                times_to_write=[],
                expected=self.test_coords.init_time,
            ),
            TestCase(
                name="some_missing_times",
                times_to_write=[self.test_coords.init_time[0], self.test_coords.init_time[2]],
                expected=[self.test_coords.init_time[1], self.test_coords.init_time[3]],
            ),
            TestCase(
                name="no_missing_times",
                times_to_write=self.test_coords.init_time,
                expected=[],
            ),
        ]

        for t in tests:
            with self.subTest(name=t.name):
                for i in t.times_to_write:
                    write_result = self.test_store.write_to_region(
                        da=xr.DataArray(
                            name="test_da",
                            data=np.ones(
                                shape=[
                                    1 if k == "init_time" else v
                                    for k, v in self.test_coords.shapemap.items()
                                ],
                            ),
                            coords=self.test_coords.to_pandas() | {
                                "init_time": [np.datetime64(i.replace(tzinfo=None), "ns")],
                            },
                        ),
                    )
                    write_result.unwrap()
                result = self.test_store.missing_times()
                missing_times = result.unwrap()
                self.assertListEqual(missing_times, t.expected)

