import datetime as dt
import pathlib
import unittest
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._models import ArpegeFileInfo

from .client import Client, _parseArpegeFilename

testClient = Client(model="global")


class TestClient(unittest.TestCase):
    def test_mapTemp(self) -> None:
        # Test with global file
        testFilePath: pathlib.Path = (
            pathlib.Path(__file__).parent / "SP1_00H24H_t.grib2"
        )
        out = testClient.mapTemp(p=testFilePath)

        # Check latitude and longitude are injected
        self.assertTrue("latitude" in out.coords)
        self.assertTrue("longitude" in out.coords)
        # Check that the dimensions are correctly ordered and renamed
        self.assertEqual(
            out[next(iter(out.data_vars.keys()))].dims,
            ("variable", "init_time", "step", "latitude", "longitude"),
        )
        # Check that the parameter is renamed
        self.assertEqual(out["variable"].values[0], "t")
        self.assertEqual(len(out["latitude"].values), 361)
        self.assertEqual(len(out["longitude"].values), 720)
        self.assertEqual(len(out["init_time"].values), 1)
        self.assertEqual(len(out["step"].values), 9)

        # Test with height level file
        testFilePath: pathlib.Path = (
                pathlib.Path(__file__).parent / "HP1_00H24H_t.grib2"
        )
        out = testClient.mapTemp(p=testFilePath)

        # Check latitude and longitude are injected
        self.assertTrue("latitude" in out.coords)
        self.assertTrue("longitude" in out.coords)
        # Check that the dimensions are correctly ordered and renamed
        self.assertEqual(
            out[next(iter(out.data_vars.keys()))].dims,
            ("variable", "init_time", "step", "heightAboveGround", "latitude", "longitude"),
        )
        # Check that the parameter is renamed
        self.assertEqual(out["variable"].values[0], "t")
        self.assertEqual(len(out["latitude"].values), 361)
        self.assertEqual(len(out["longitude"].values), 720)
        self.assertEqual(len(out["init_time"].values), 1)
        self.assertEqual(len(out["step"].values), 9)
        self.assertEqual(len(out["heightAboveGround"].values), 24)

        # Test with pressure level file
        testFilePath: pathlib.Path = (
            pathlib.Path(__file__).parent / "IP1_00H24H_t.grib2"
        )
        out = testClient.mapTemp(p=testFilePath)

        # Check latitude and longitude are present
        self.assertTrue("latitude" in out.coords)
        self.assertTrue("longitude" in out.coords)
        # Check that the dimensions are correctly ordered and renamed
        self.assertEqual(
            out[next(iter(out.data_vars.keys()))].dims,
            ("variable", "init_time", "step", "isobaricInhPa", "latitude", "longitude"),
        )
        # Check that the parameter is renamed
        self.assertEqual(out["variable"].values[0], "t")
        self.assertEqual(len(out["latitude"].values), 361)
        self.assertEqual(len(out["longitude"].values), 720)
        self.assertEqual(len(out["init_time"].values), 1)
        self.assertEqual(len(out["step"].values), 9)
        self.assertEqual(len(out["isobaricInhPa"].values), 28)


class TestParseArpegeFilename(unittest.TestCase):
    baseurl = "s3://mf-nwp-models/arpege-world/v1/2023-12-03/12/"

    def test_parsesSingleLevel(self) -> None:
        filename: str = "00H24H.grib2"

        out: ArpegeFileInfo | None = _parseArpegeFilename(
            name=filename,
            baseurl=self.baseurl+"SP1/",
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename)
        self.assertEqual(out.it(), dt.datetime(2023, 12, 3, 12, tzinfo=dt.timezone.utc))

    def test_parsesHeightLevel(self) -> None:
        filename: str = "00H24H.grib2"

        out: ArpegeFileInfo | None = _parseArpegeFilename(
            name=filename,
            baseurl=self.baseurl+"HP2/",
            match_hl=True,
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename)
        self.assertEqual(out.it(), dt.datetime(2023, 12, 3, 12, tzinfo=dt.timezone.utc))

        out: ArpegeFileInfo | None = _parseArpegeFilename(
            name=filename,
            baseurl=self.baseurl,
            match_hl=False,
        )
        self.assertIsNone(out)

    def test_parsesPressureLevel(self) -> None:
        filename: str = "00H24H.grib2"

        out: ArpegeFileInfo | None = _parseArpegeFilename(
            name=filename,
            baseurl=self.baseurl+"IP4/",
            match_pl=True,
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename)
        self.assertEqual(out.it(), dt.datetime(2023, 12, 3, 12, tzinfo=dt.timezone.utc))

        out: ArpegeFileInfo | None = _parseArpegeFilename(
            name=filename,
            baseurl=self.baseurl,
            match_pl=False,
        )
        self.assertIsNone(out)
