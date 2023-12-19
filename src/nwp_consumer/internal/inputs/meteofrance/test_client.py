import datetime as dt
import pathlib
import unittest
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._models import IconFileInfo

from .client import Client, _parseIconFilename

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
    baseurl = "https://opendata.dwd.de/weather/nwp/icon/grib"

    def test_parsesSingleLevel(self) -> None:
        filename: str = "icon_global_icosahedral_single-level_2020090100_000_T_HUM.grib2.bz2"

        out: IconFileInfo | None = _parseIconFilename(
            name=filename,
            baseurl=self.baseurl,
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename.removesuffix(".bz2"))
        self.assertEqual(out.it(), dt.datetime(2020, 9, 1, 0, tzinfo=dt.timezone.utc))

    def test_parsesTimeInvariant(self) -> None:
        filename: str = "icon_global_icosahedral_time-invariant_2020090100_CLAT.grib2.bz2"

        out: IconFileInfo | None = _parseIconFilename(
            name=filename,
            baseurl=self.baseurl,
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename.removesuffix(".bz2"))
        self.assertEqual(out.it(), dt.datetime(2020, 9, 1, 0, tzinfo=dt.timezone.utc))

    def test_parsesModelLevel(self) -> None:
        filename: str = "icon_global_icosahedral_model-level_2020090100_048_32_CLCL.grib2.bz2"

        out: IconFileInfo | None = _parseIconFilename(
            name=filename,
            baseurl=self.baseurl,
            match_ml=True,
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename.removesuffix(".bz2"))
        self.assertEqual(out.it(), dt.datetime(2020, 9, 1, 0, tzinfo=dt.timezone.utc))

        out: IconFileInfo | None = _parseIconFilename(
            name=filename,
            baseurl=self.baseurl,
            match_ml=False,
        )
        self.assertIsNone(out)

    def test_parsesPressureLevel(self) -> None:
        filename: str = "icon_global_icosahedral_pressure-level_2020090100_048_1000_T.grib2.bz2"

        out: IconFileInfo | None = _parseIconFilename(
            name=filename,
            baseurl=self.baseurl,
            match_pl=True,
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename.removesuffix(".bz2"))
        self.assertEqual(out.it(), dt.datetime(2020, 9, 1, 0, tzinfo=dt.timezone.utc))

        out: IconFileInfo | None = _parseIconFilename(
            name=filename,
            baseurl=self.baseurl,
            match_pl=False,
        )
        self.assertIsNone(out)
