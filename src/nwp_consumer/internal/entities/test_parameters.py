import unittest

import numpy as np
import xarray as xr
from hypothesis import given
from hypothesis import strategies as st
from returns.pipeline import is_successful

from .parameters import Parameter


class TestParameters(unittest.TestCase):
    """Test the business methods of the Parameters class."""

    @given(st.sampled_from(Parameter))
    def test_metadata(self, p: Parameter) -> None:
        """Test the metadata method."""
        metadata = p.metadata()
        self.assertEqual(metadata.name, p.value)

    @given(st.sampled_from([s for p in Parameter for s in p.metadata().alternative_shortnames]))
    def test_try_from_shortname(self, shortname: str) -> None:
        """Test the try_from_shortname method."""
        p = Parameter.try_from_alternate(shortname)
        self.assertTrue(is_successful(p))

        p = Parameter.try_from_alternate("invalid")
        self.assertFalse(is_successful(p))

    @given(
        st.sampled_from([s for p in Parameter for s in p.metadata().alternative_shortnames]),
        st.sampled_from(Parameter),
    )
    def test_rename_else_drop_ds_vars(self, shortname: str, parameter: Parameter) -> None:
        """Test the rename_else_drop_ds_vars method."""
        allowed_parameters: list[Parameter] = [parameter]

        ds = xr.Dataset(
            data_vars={
                shortname: (
                    ("init_time", "step", "latitude", "longitude"),
                    np.random.rand(1, 12, 15, 15),
                ),
                "unknown-parameter": (
                    ("init_time", "step", "latitude", "longitude"),
                    np.random.rand(1, 12, 15, 15),
                ),
            },
            coords={
                "init_time": np.array([0]),
                "step": np.array(range(12)),
                "latitude": np.array(range(15)),
                "longitude": np.array(range(15)),
            },
        )

        ds = Parameter.rename_else_drop_ds_vars(
            ds,
            allowed_parameters=allowed_parameters,
        )

        if shortname in parameter.metadata().alternative_shortnames:
            self.assertTrue(len(list(ds.data_vars)) == 1)
            self.assertEqual(next(iter(ds.data_vars)), str(parameter))
        else:
            self.assertTrue(len(list(ds.data_vars)) == 0)


if __name__ == "__main__":
    unittest.main()
