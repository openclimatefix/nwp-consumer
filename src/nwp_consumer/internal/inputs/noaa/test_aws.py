import datetime as dt
import pathlib
import unittest
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._models import NOAAFileInfo

from .aws import Client

testClient = Client(model="global", param_group="basic")


class TestClient(unittest.TestCase):
    def test_mapCachedRaw(self) -> None:
        # Test with global file
        testFilePath: pathlib.Path = (
            pathlib.Path(__file__).parent / "test_surface_000.grib2"
        )
        out = testClient.mapCachedRaw(p=testFilePath)
        # Check latitude and longitude are injected
        self.assertTrue("latitude" in out.coords)
        self.assertTrue("longitude" in out.coords)
        print(out)
        # Check that the dimensions are correctly ordered and renamed
        self.assertEqual(
            out[next(iter(out.data_vars.keys()))].dims,
            ("init_time", "step", "latitude", "longitude"),
        )
        self.assertEqual(len(out["latitude"].values), 721)
        self.assertEqual(len(out["longitude"].values), 1440)
        self.assertEqual(len(out["init_time"].values), 1)
        self.assertEqual(len(out["step"].values), 1)
        self.assertListEqual(list(out.data_vars.keys()), ["t2m"])

