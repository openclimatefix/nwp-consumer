import datetime as dt
import pathlib
import unittest
from typing import TYPE_CHECKING

import xarray as xr

if TYPE_CHECKING:
    from ._models import IconFileInfo

from .client import Client, _parseIconFilename

testClientGlobal = Client(model="global")
testClientEurope = Client(model="europe")


class TestClient(unittest.TestCase):
    def test_mapCachedRawGlobal(self) -> None:
        tests = [
            {
                "filename": "test_icon_global_001_CLCL.grib2",
                "expected_dims": ["init_time", "step", "values"],
                "expected_var": "ccl",
            },
            {
                "filename": "test_icon_global_001_HTOP_CON.grib2",
                "expected_dims": ["init_time", "step", "values"],
                "expected_var": "hcct",
            },
            {
                "filename": "test_icon_global_001_CLCT_MOD.grib2",
                "expected_dims": ["init_time", "step", "values"],
                "expected_var": "CLCT_MOD",
            },
        ]

        for tst in tests:
            with self.subTest(f"test file {tst['filename']}"):
                out = testClientGlobal.mapCachedRaw(p=pathlib.Path(__file__).parent / tst["filename"])
                print(out)

                # Check latitude and longitude are injected
                self.assertTrue("latitude" in out.coords)
                self.assertTrue("longitude" in out.coords)
                # Check that the dimensions are correctly ordered and renamed
                self.assertEqual((list(out.dims.keys())), tst["expected_dims"])

    def test_mapCachedRawEurope(self) -> None:
        tests = [
            {
                "filename": "test_icon_europe_001_CLCL.grib2",
                "expected_dims": ["init_time", "step", "latitude", "longitude"],
                "expected_var": "ccl",
            },
        ]

        for tst in tests:
            with self.subTest(f"test file {tst['filename']}"):
                out = testClientEurope.mapCachedRaw(p=pathlib.Path(__file__).parent / tst["filename"])
                print(out)

                # Check latitude and longitude are injected
                self.assertTrue("latitude" in out.coords)
                self.assertTrue("longitude" in out.coords)
                # Check that the dimensions are correctly ordered and renamed
                for data_var in out.data_vars:
                    self.assertEqual(list(out[data_var].dims), tst["expected_dims"])

    def test_mergeRaw(self) -> None:
        ds1 = testClientGlobal.mapCachedRaw(
            p=pathlib.Path(__file__).parent / "test_icon_global_001_CLCT_MOD.grib2"
        )
        ds2 = testClientGlobal.mapCachedRaw(
            p=pathlib.Path(__file__).parent / "test_icon_global_001_HTOP_CON.grib2"
        )

        # This should merge without raising an error
        _ = xr.merge([ds1, ds2])


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
        self.assertEqual(out.it(), dt.datetime(2020, 9, 1, 0, tzinfo=dt.UTC))

    def test_parsesTimeInvariant(self) -> None:
        filename: str = "icon_global_icosahedral_time-invariant_2020090100_CLAT.grib2.bz2"

        out: IconFileInfo | None = _parseIconFilename(
            name=filename,
            baseurl=self.baseurl,
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename.removesuffix(".bz2"))
        self.assertEqual(out.it(), dt.datetime(2020, 9, 1, 0, tzinfo=dt.UTC))

    def test_parsesModelLevel(self) -> None:
        filename: str = "icon_global_icosahedral_model-level_2020090100_048_32_CLCL.grib2.bz2"

        out: IconFileInfo | None = _parseIconFilename(
            name=filename,
            baseurl=self.baseurl,
            match_ml=True,
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename.removesuffix(".bz2"))
        self.assertEqual(out.it(), dt.datetime(2020, 9, 1, 0, tzinfo=dt.UTC))

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
        self.assertEqual(out.it(), dt.datetime(2020, 9, 1, 0, tzinfo=dt.UTC))

        out: IconFileInfo | None = _parseIconFilename(
            name=filename,
            baseurl=self.baseurl,
            match_pl=False,
        )
        self.assertIsNone(out)
