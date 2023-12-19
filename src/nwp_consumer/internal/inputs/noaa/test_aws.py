import datetime as dt
import pathlib
import unittest
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._models import NOAAFileInfo

from .aws import Client, _parseAWSFilename

testClient = Client(model="global", param_group="full")


class TestClient(unittest.TestCase):
    def test_mapTemp(self) -> None:
        # Test with global file
        testFilePath: pathlib.Path = (
            pathlib.Path(__file__).parent / "gfs.t06z.pgrb2.0p25.f005"
        )
        out = testClient.mapTemp(p=testFilePath)
        # Check latitude and longitude are injected
        self.assertTrue("latitude" in out.coords)
        self.assertTrue("longitude" in out.coords)
        # Check that the dimensions are correctly ordered and renamed
        self.assertEqual(
            out[next(iter(out.data_vars.keys()))].dims,
            ("init_time", "step", "latitude", "longitude"),
        )


class TestParseIconFilename(unittest.TestCase):
    baseurl = "https://opendata.dwd.de/weather/nwp/icon/grib"

    def test_parsesSingleLevel(self) -> None:
        filename: str = "icon_global_icosahedral_single-level_2020090100_000_T_HUM.grib2.bz2"

        out: NOAAFileInfo | None = _parseAWSFilename(
            name=filename,
            baseurl=self.baseurl,
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename.removesuffix(".bz2"))
        self.assertEqual(out.it(), dt.datetime(2020, 9, 1, 0, tzinfo=dt.timezone.utc))

    def test_parsesTimeInvariant(self) -> None:
        filename: str = "icon_global_icosahedral_time-invariant_2020090100_CLAT.grib2.bz2"

        out: NOAAFileInfo | None = _parseAWSFilename(
            name=filename,
            baseurl=self.baseurl,
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename.removesuffix(".bz2"))
        self.assertEqual(out.it(), dt.datetime(2020, 9, 1, 0, tzinfo=dt.timezone.utc))
