import datetime as dt
import pathlib
import unittest
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._models import ArpegeFileInfo

from .client import Client, _parseArpegeFilename

testClient = Client(model="global")


class TestClient(unittest.TestCase):
    def test_mapCachedRaw(self) -> None:

        tests = [
            {
                "filename": "SP1_00H24H_t.grib2",
                "expected_dims": ("init_time", "step", "latitude", "longitude"),
                "expected_var": "t",
            },
            {
                "filename": "HP1_00H24H_t.grib2",
                "expected_dims": ("init_time", "step", "heightAboveGround", "latitude", "longitude"),
                "expected_var": "t",
            },
            {
                "filename": "IP1_00H24H_t.grib2",
                "expected_dims": ("init_time", "step", "isobaricInhPa", "latitude", "longitude"),
                "expected_var": "t",
            },
        ]

        for tst in tests:
            with self.subTest(f"test file {tst['filename']}"):
                out = testClient.mapCachedRaw(p=pathlib.Path(__file__).parent / tst["filename"])

                # Check latitude and longitude are injected
                self.assertTrue("latitude" in out.coords)
                self.assertTrue("longitude" in out.coords)
                # Check that the dimensions are correctly ordered and renamed
                self.assertEqual(
                    out[next(iter(out.data_vars.keys()))].dims,
                    tst["expected_dims"],
                )


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
