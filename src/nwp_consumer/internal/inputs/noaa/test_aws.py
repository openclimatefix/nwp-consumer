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
            pathlib.Path(__file__).parent / "gfs.t06z.pgrb2.0p25.f001"
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
    baseurl = "https://noaa-gfs-bdp-pds.s3.amazonaws.com"

    def test_parsesSingleLevel(self) -> None:
        filename: str = "gfs.t06z.pgrb2.0p25.f005"
        it = dt.datetime(2020, 9, 1, 6, tzinfo=dt.timezone.utc)
        out: NOAAFileInfo | None = _parseAWSFilename(
            name=filename,
            baseurl=f"{self.baseurl}/gfs.{it.strftime('%Y%m%d')}/{it.strftime('%H')}",
            it=it,
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.filename(), filename)
        self.assertEqual(out.it(), dt.datetime(2020, 9, 1, 6, tzinfo=dt.timezone.utc))
