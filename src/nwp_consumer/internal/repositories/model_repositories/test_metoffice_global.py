import datetime as dt
import os
import unittest

import numpy as np
import xarray as xr
from returns.pipeline import is_successful

from nwp_consumer.internal import entities

from .metoffice_global import CedaMetOfficeGlobalModelRepository


class TestCedaMetOfficeGlobalModelRepository(unittest.TestCase):
    """Test the business methods of the CedaMetOfficeGlobalModelRepository class."""

    c = CedaMetOfficeGlobalModelRepository()

    @unittest.skipIf(
        condition="CI" in os.environ,
        reason="Skipping integration test that requires FTP access.",
    )
    def test__download_and_convert(self) -> None:
        """Test the _download_and_convert method."""

        test_it: dt.datetime = dt.datetime(2021, 1, 1, 0, tzinfo=dt.UTC)


        test_url: str = "".join((
            self.c.url_base,
            f"/{test_it:%Y/%m/%d}",
            f"/{test_it:%Y%m%d%H}_WSGlobal17km_Total_Downward_Surface_SW_Flux_AreaA_000144.grib",
        ))

        result = self.c._download_and_convert(test_url)

        self.assertTrue(is_successful(result), msg=f"Error: {result}")

        # Check resultant array is a subset of the expected coordinates
        map_result = entities.NWPDimensionCoordinateMap.from_pandas(result.unwrap().coords.indexes)
        self.c.metadata.expected_coordinates.init_time = [test_it]
        region_result = map_result.bind(self.c.metadata.expected_coordinates.determine_region)

        self.assertTrue(is_successful(region_result), msg=f"Error: {region_result}")

