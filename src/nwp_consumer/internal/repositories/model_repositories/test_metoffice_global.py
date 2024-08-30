import os
import unittest

from returns.pipeline import is_successful

from ...entities import to_pandas
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

        test_url: str = "".join((
            self.c.url_base,
            "/2021/01/01/2021010100_WSGlobal17km_Total_Downward_Surface_SW_Flux_AreaA_000144.grib",
        ))

        result = self.c._download_and_convert(test_url)

        self.assertTrue(is_successful(result), msg=f"Error: {result.failure()}")
        self.assertDictEqual(
            result.unwrap().coords.indexes,
            to_pandas(self.c.metadata.expected_coordinates),
        )