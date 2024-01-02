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
            pathlib.Path(__file__).parent / "test_icon_global_001_CLCL.grib2"
        )
        out = testClient.mapTemp(p=testFilePath)

        # Check latitude and longitude are injected
        self.assertTrue("latitude" in out.coords)
        self.assertTrue("longitude" in out.coords)
        # Check that the dimensions are correctly ordered and renamed
        self.assertEqual(
            out[next(iter(out.data_vars.keys()))].dims,
            ("variable", "init_time", "step", "values"),
        )
        # Check that the parameter is renamed
        self.assertEqual(out["variable"].values[0], "ccl")

        # Test with europe file
        testFilePath: pathlib.Path = (
            pathlib.Path(__file__).parent / "test_icon_europe_001_CLCL.grib2"
        )
        out = testClient.mapTemp(p=testFilePath)

        # Check latitude and longitude are present
        self.assertTrue("latitude" in out.coords)
        self.assertTrue("longitude" in out.coords)
        # Check that the dimensions are correctly ordered and renamed
        self.assertEqual(
            out[next(iter(out.data_vars.keys()))].dims,
            ("variable", "init_time", "step", "latitude", "longitude"),
        )
        # Check that the parameter is renamed
        self.assertEqual(out["variable"].values[0], "ccl")


class TestParseIconFilename(unittest.TestCase):
    baseurl = "https://mf-nwp-models.s3.amazonaws.com/arpege-world/v1/2023-12-03/12/"

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
            baseurl=self.baseurl+"HP1/",
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
            baseurl=self.baseurl+"IP1/",
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
