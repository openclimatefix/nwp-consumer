"""Tests for the ecmwf module."""

import pathlib
import unittest.mock

from .client import MARSClient

# --------- Test setup --------- #

testMARSClient = MARSClient(
    area="uk"
)


# --------- Client methods --------- #


class TestECMWFMARSClient(unittest.TestCase):
    """Tests for the ECMWFMARSClient method."""

    def test_init(self):
        with self.assertRaises(KeyError):
            _ = MARSClient(area="not a valid area")

    def test_mapTemp(self):
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_2params.grib"

        out = testMARSClient.mapTemp(p=testFilePath)

        # Ensure the dimensions have the right sizes
        self.assertDictEqual(
            {"init_time": 1, "variable": 2, "step": 49, "latitude": 241, "longitude": 301},
            dict(out.dims.items())
        )
        # Ensure the dimensions of the variables are in the correct order
        self.assertEqual(
            ("variable", "init_time", "step", "latitude", "longitude"),
            out["UKV"].dims
        )
        # Ensure the correct variables are in the variable dimension
        self.assertListEqual(['prate', 'sde'], sorted(out.coords["variable"].values))


# --------- Static methods --------- #

