"""Tests for the ecmwf module."""

import datetime as dt
import pathlib
import unittest.mock

from .mars import Client, PARAMETER_ECMWFCODE_MAP

# --------- Test setup --------- #

testMARSClient = Client(
    area="uk",
    hours=48,
)


# --------- Client methods --------- #


class TestECMWFMARSClient(unittest.TestCase):
    """Tests for the ECMWFMARSClient method."""

    def test_init(self) -> None:
        with self.assertRaises(KeyError):
            _ = Client(area="not a valid area", hours=48)
        with self.assertRaises(KeyError):
            _ = Client(area="uk", hours=100)

    def test_mapTemp(self) -> None:
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_2params.grib"

        out = testMARSClient.mapTemp(p=testFilePath)

        # Ensure the dimensions have the right sizes
        self.assertDictEqual(
            {"init_time": 1, "variable": 2, "step": 49, "latitude": 241, "longitude": 301},
            dict(out.dims.items()),
        )
        # Ensure the dimensions of the variables are in the correct order
        self.assertEqual(
            ("variable", "init_time", "step", "latitude", "longitude"),
            out[next(iter(out.data_vars.keys()))].dims,
        )
        # Ensure the correct variables are in the variable dimension
        self.assertListEqual(["prate", "sde"], sorted(out.coords["variable"].values))

    def test_buildMarsRequest(self) -> None:
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_2params.grib"


        # Test that the request is build correctly for the default client
        testDefaultClient = Client()
        out = testDefaultClient._buildMarsRequest(
            list_only=True,
            target=testFilePath.as_posix(),
            it=dt.datetime(2020, 1, 1, tzinfo=dt.UTC),
        )

        out.replace(" ", "")
        lines = out.split("\n")
        self.assertEqual(lines[0], "list,")

        d: dict = {}
        for line in lines[1:]:
            key, value = line.split("=")
            d[key.strip()] = value.strip().replace(",", "")

        self.assertEqual(d["param"], "/".join(PARAMETER_ECMWFCODE_MAP.keys()))
        self.assertEqual(d["date"], "20200101")

        # Test that the request is build correctly with the basic parameters

        testBasicClient = Client(
            area="uk",
            hours=4,
            param_group="basic",
        )

        out = testBasicClient._buildMarsRequest(
            list_only=False,
            target=testFilePath.as_posix(),
            it=dt.datetime(2020, 1, 1, tzinfo=dt.UTC),
        )

        out.replace(" ", "")
        lines = out.split("\n")
        self.assertEqual(lines[0], "retrieve,")

        d2: dict = {}
        for line in lines[1:]:
            key, value = line.split("=")
            d2[key.strip()] = value.strip().replace(",", "")

        self.assertEqual(d2["param"], "167.128/169.128")
        self.assertEqual(d2["date"], "20200101")



# --------- Static methods --------- #
