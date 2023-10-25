import datetime as dt
import pathlib
import unittest

from .client import PARAMETER_RENAME_MAP, Client

testClient = Client(model='global')

class TestClient(unittest.TestCase):


    def test_mapTemp(self):

        # Test with global file
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / 'test_icon_global_001_CLCL.grib2'
        out = testClient.mapTemp(p=testFilePath)

        # Check latitude and longitude are injected
        self.assertTrue("latitude" in out.coords)
        self.assertTrue("longitude" in out.coords)
        # Check that the dimensions are correctly ordered and renamed
        self.assertEqual(out[list(out.data_vars.keys())[0]].dims, ('variable', 'init_time', 'step', 'values'))
        # Check that the parameter is renamed
        self.assertEqual(out["variable"].values[0], "ccl")

        # Test with europe file
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / 'test_icon_europe_001_CLCL.grib2'
        out = testClient.mapTemp(p=testFilePath)

        # Check latitude and longitude are present
        self.assertTrue("latitude" in out.coords)
        self.assertTrue("longitude" in out.coords)
        # Check that the dimensions are correctly ordered and renamed
        self.assertEqual(out[list(out.data_vars.keys())[0]].dims, ('variable', 'init_time', 'step', 'latitude', 'longitude'))
        # Check that the parameter is renamed
        self.assertEqual(out["variable"].values[0], "ccl")

