import datetime as dt
import os
import unittest

from returns.pipeline import is_successful

from .mo_datahub import MetOfficeDatahubModelRepository


class TestMetOfficeDatahubModelRepository(unittest.TestCase):
    """Test the business methods of the MetOfficeDatahubModelRepository class."""

    @unittest.skipIf(
        condition="CI" in os.environ,
        reason="Skipping integration test that requires MetOffice DataHub access.",
    )
    def test__download(self) -> None:
        """Test the _download method."""

        auth_result = MetOfficeDatahubModelRepository.authenticate()
        self.assertTrue(is_successful(auth_result), msg=f"Error: {auth_result}")
        c = auth_result.unwrap()

        test_it = c.repository().determine_latest_it_from(dt.datetime.now(tz=dt.UTC))

        dl_result = c._download(
            f"{c.request_url}/agl_u-component-of-wind-surface-adjusted_10.0_{test_it:%Y%m%d%H}_1/data",
        )

        self.assertTrue(is_successful(dl_result), msg=f"Error: {dl_result}")


