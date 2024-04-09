"""Tests for the ecmwf module."""

import datetime as dt
import pathlib
import unittest.mock

from .mars import PARAMETER_ECMWFCODE_MAP, MARSClient, _parseListing

# --------- Test setup --------- #

testMARSClient = MARSClient(
    area="uk",
    hours=48,
)

test_list_response: str = """
class   = od
date    = 2017-09-11
expver  = 1
file[0] = hpss:/mars/prod/od/o/oper/fc/sfc/marsodoper/0001/fc/20170911/sfc/1200/879664.20170927.205633
id      = 879664
levtype = sfc
month   = 201709
stream  = oper
time    = 12:00:00
type    = fc
year    = 2017
file length   missing offset     param   step
0    13204588 .       1089967084 167.128 0
0    13204588 .       1116376260 169.128 0
0    13204588 .       2921064730 167.128 1
0    13204588 .       2947473906 169.128 1
0    13204588 .       4699268722 167.128 2
0    13204588 .       4725677898 169.128 2
0    13204588 .       6516961654 167.128 3
0    13204588 .       6543370830 169.128 3

Grand Total:
============

Entries       : 8
Total         : 105,636,704 (100.743 Mbytes)
"""


# --------- Client methods --------- #


class TestECMWFMARSClient(unittest.TestCase):
    """Tests for the ECMWFMARSClient method."""

    def test_init(self) -> None:
        with self.assertRaises(KeyError):
            _ = MARSClient(area="not a valid area", hours=48)

    def test_mapCachedRaw(self) -> None:
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_2params.grib"

        out = testMARSClient.mapCachedRaw(p=testFilePath)

        # Ensure the dimensions have the right sizes
        self.assertDictEqual(
            {"init_time": 1, "step": 49, "latitude": 241, "longitude": 301},
            dict(out.sizes.items()),
        )
        # Ensure the dimensions of the variables are in the correct order
        self.assertEqual(
            ("init_time", "step", "latitude", "longitude"),
            out[next(iter(out.data_vars.keys()))].dims,
        )
        # Ensure the correct datavars are in the dataset
        self.assertCountEqual(["tprate", "sd"], list(out.data_vars.keys()))

    def test_buildMarsRequest(self) -> None:
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_2params.grib"

        # Test that the request is build correctly for the default client
        testDefaultClient = MARSClient()
        out = testDefaultClient._buildMarsRequest(
            list_only=True,
            target=testFilePath.as_posix(),
            it=dt.datetime(2020, 1, 1, tzinfo=dt.UTC),
            params=testDefaultClient.desired_params,
            steps=range(4),
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

        testBasicClient = MARSClient(
            area="uk",
            hours=4,
            param_group="basic",
        )

        out = testBasicClient._buildMarsRequest(
            list_only=False,
            target=testFilePath.as_posix(),
            it=dt.datetime(2020, 1, 1, tzinfo=dt.UTC),
            params=testBasicClient.desired_params,
            steps=range(4),
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


class TestParseAvailableParams(unittest.TestCase):
    def test_parsesSmallFileCorrectly(self) -> None:
        out = _parseListing(fileData=test_list_response)

        self.assertDictEqual(
            {
                "params": ["167.128", "169.128"],
                "steps": [0, 1, 2, 3],
            },
            out,
        )

    def test_parsesParamsCorrectly(self) -> None:
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_list_response.txt"

        filedata: str = testFilePath.read_text()

        out = _parseListing(fileData=filedata)

        self.maxDiff = None
        self.assertDictEqual(
            {
                "params": ["141.128","164.128","165.128","166.128","167.128","169.128","175.128","186.128","187.128","188.128","20.3","246.228","247.228","47.128","57.128"],
                "steps": list(range(0, 49)),
            },
            out,
        )
