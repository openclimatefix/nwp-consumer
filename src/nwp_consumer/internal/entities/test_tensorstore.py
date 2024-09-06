import dataclasses
import datetime as dt
import unittest

import numpy as np
from returns.pipeline import is_successful

from . import NWPDimensionCoordinateMap, Parameter
from .parameters import params
from .postprocess import PostProcessOptions
from .tensorstore import TensorStore


class TestTensorStore(unittest.TestCase):
    """Test the business methods of the TensorStore class."""

    test_coords: NWPDimensionCoordinateMap
    test_class: TensorStore

    @classmethod
    def setUpClass(cls) -> None:
        test_coords = NWPDimensionCoordinateMap(
            init_time=[dt.datetime(2021, 1, 1, tzinfo=dt.UTC)],
            step=[1, 2, 3, 4],
            variable=[params.temperature_sl],
            latitude=np.linspace(90, -90, 12).tolist(),
            longitude=np.linspace(0, 360, 18).tolist(),
        )

        init_result = TensorStore.initialize_empty_store(
            name="test_da",
            coords=test_coords,
        )
        if is_successful(init_result):
            cls.test_class = init_result.unwrap()
            cls.test_coords = test_coords
        else:
            raise ValueError("Failed to initialize test store.")

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
                result = self.test_class.postprocess(t.options)
                if t.should_error:
                    self.assertTrue(
                        isinstance(result, Exception),
                        msg="Expected error to be returned.",
                    )
                else:
                    self.assertTrue(is_successful(result))

