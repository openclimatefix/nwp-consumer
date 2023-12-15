import datetime as dt
import pathlib
import unittest
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._models import CMCFileInfo

from .client import Client, _parseCMCFilename

testClient = Client(model="gdps")


class TestClient(unittest.TestCase):
    def test_mapTemp(self) -> None:
        # Test with global file
        testFilePath: pathlib.Path = (
            pathlib.Path(__file__).parent / "CMC_glb_VGRD_ISBL_200_latlon.15x.15_2023080900_P027.grib2"
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
        self.assertEqual(out["variable"].values[0], "v")

        # Test with europe file
        testFilePath: pathlib.Path = (
            pathlib.Path(__file__).parent / "CMC_glb_CAPE_SFC_0_latlon.15x.15_2023080900_P027.grib2"
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
        self.assertEqual(out["variable"].values[0], "cape")


class TestParseCMCFilename(unittest.TestCase):
    baseurl = "https://dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/"

    def test_parsesSingleLevel(self) -> None:
        filename: str = "CMC_glb_CAPE_SFC_0_latlon.15x.15_2023080900_P027.grib2"

        out: CMCFileInfo | None = _parseCMCFilename(
            name=filename,
            baseurl=self.baseurl,
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename)
        self.assertEqual(out.it(), dt.datetime(2023, 8, 9, 0, tzinfo=dt.timezone.utc))

    def test_parsesHeightAboveGround(self) -> None:
        filename: str = "CMC_glb_TMP_TGL_2_latlon.15x.15_2023080900_P027.grib2"

        out: CMCFileInfo | None = _parseCMCFilename(
            name=filename,
            baseurl=self.baseurl,
            match_tgl=True,
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename)
        self.assertEqual(out.it(), dt.datetime(2023, 8, 9, 0, tzinfo=dt.timezone.utc))

        out: CMCFileInfo | None = _parseCMCFilename(
            name=filename,
            baseurl=self.baseurl,
            match_tgl=False,
        )
        self.assertIsNone(out)

    def test_parsesPressureLevel(self) -> None:
        filename: str = "CMC_glb_VGRD_ISBL_200_latlon.15x.15_2023080900_P027.grib2"

        out: CMCFileInfo | None = _parseCMCFilename(
            name=filename,
            baseurl=self.baseurl,
            match_pl=True,
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename)
        self.assertEqual(out.it(), dt.datetime(2023, 8, 9, 0, tzinfo=dt.timezone.utc))

        out: CMCFileInfo | None = _parseCMCFilename(
            name=filename,
            baseurl=self.baseurl,
            match_pl=False,
        )
        self.assertIsNone(out)
