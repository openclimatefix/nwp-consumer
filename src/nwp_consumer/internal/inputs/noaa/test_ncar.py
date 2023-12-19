import datetime as dt
import pathlib
import unittest
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._models import NOAAFileInfo

from .ncar import Client, _parseNCARFilename

testClient = Client(model="global", param_group="full")


class TestClient(unittest.TestCase):
    def test_mapTemp(self) -> None:
        # Test with global file
        testFilePath: pathlib.Path = (
            pathlib.Path(__file__).parent / "gfs.0p25.2023121906.f001.grib2"
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
        self.assertEqual(len(out["latitude"].values), 721)
        self.assertEqual(len(out["longitude"].values), 1440)
        self.assertEqual(len(out["init_time"].values), 1)
        self.assertEqual(len(out["step"].values), 1)


class TestParseIconFilename(unittest.TestCase):
    baseurl = "https://data.rda.ucar.edu/ds084.1"

    def test_parsesSingleLevel(self) -> None:
        filename: str = "gfs.0p25.2016010300.f003.grib2"
        it = dt.datetime(2016, 1, 3, 0, tzinfo=dt.timezone.utc)
        out: NOAAFileInfo | None = _parseNCARFilename(
            name=filename,
            baseurl=f"{self.baseurl}/{it.strftime('%Y')}/{it.strftime('%Y%m%d')}",
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename)
        self.assertEqual(out.it(), dt.datetime(2016, 1, 3, 0, tzinfo=dt.timezone.utc))
