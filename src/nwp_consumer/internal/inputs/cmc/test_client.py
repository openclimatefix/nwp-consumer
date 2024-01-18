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
        self.assertEqual(len(out["latitude"].values), 1201)
        self.assertEqual(len(out["longitude"].values), 2400)
        # Check that the dimensions are correctly ordered and renamed
        self.assertEqual(
            out[next(iter(out.data_vars.keys()))].dims,
            ("variable", "init_time", "step", "latitude", "longitude"),
        )
        # Check that the parameter is renamed
        self.assertEqual(out["variable"].values[0], "v10")

        # Test with europe file
        testFilePath: pathlib.Path = (
            pathlib.Path(__file__).parent / "CMC_glb_CAPE_SFC_0_latlon.15x.15_2023080900_P027.grib2"
        )
        out = testClient.mapTemp(p=testFilePath)

        # Check latitude and longitude are present
        self.assertTrue("latitude" in out.coords)
        self.assertTrue("longitude" in out.coords)
        self.assertEqual(len(out["latitude"].values), 1201)
        self.assertEqual(len(out["longitude"].values), 2400)
        # Check that the dimensions are correctly ordered and renamed
        self.assertEqual(
            out[next(iter(out.data_vars.keys()))].dims,
            ("variable", "init_time", "step", "latitude", "longitude"),
        )
        # Check that the parameter is renamed
        self.assertEqual(out["variable"].values[0], "cape")


class TestParseCMCFilename(unittest.TestCase):
    baseurl = "https://dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/"

    def test_parses(self) -> None:
        tests = {
            "gdps-sl": "CMC_glb_CIN_SFC_0_latlon.15x.15_2023080900_P027.grib2",
            "geps-sl": "CMC_geps-raw_CIN_SFC_0_latlon0p5x0p5_2023080900_P027_allmbrs.grib2",
            "gdps-hl": "CMC_glb_SPFH_TGL_40_latlon.15x.15_2023080900_P027.grib2",
            "geps-hl": "CMC_geps-raw_SPFH_TGL_80_latlon0p5x0p5_2023080900_P000_allmbrs.grib2",
            "gdps-pl": "CMC_glb_TMP_ISBL_300_latlon.15x.15_2023080900_P000.grib2",
            "geps-pl": "CMC_geps-raw_TMP_ISBL_0500_latlon0p5x0p5_2023080900_P000_allmbrs.grib2",
        }

        for k, v in tests.items():
            with self.subTest(msg=k):
                out: CMCFileInfo | None = _parseCMCFilename(
                    name=v,
                    baseurl=self.baseurl,
                    match_hl="hl" in k,
                    match_pl="pl" in k,
                )
                if out is None:
                    self.fail(f"Failed to parse filename {v}")
                self.assertEqual(out.filename(), v)
                self.assertEqual(out.it(), dt.datetime(2023, 8, 9, 0, tzinfo=dt.UTC))


