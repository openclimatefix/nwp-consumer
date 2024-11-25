import shutil
import unittest

import xarray as xr
from returns.pipeline import is_successful

from nwp_consumer.internal.services.archiver_service import ArchiverService

from ._dummy_adaptors import DummyModelRepository, DummyNotificationRepository


class TestParallelConsumer(unittest.TestCase):

    @unittest.skip("Takes an age to run, need to figure out a better way.")
    def test_archive(self) -> None:
        """Test the consume method of the ParallelConsumer class."""

        test_consumer = ArchiverService(
            model_repository=DummyModelRepository,
            notification_repository=DummyNotificationRepository,
        )

        result = test_consumer.archive(year=2021, month=1)

        self.assertTrue(is_successful(result), msg=result)

        da: xr.DataArray = xr.open_dataarray(result.unwrap(), engine="zarr")

        self.assertEqual(
            list(da.sizes.keys()),
            ["init_time", "step", "variable", "latitude", "longitude"],
        )

        path = result.unwrap()
        shutil.rmtree(path)


if __name__ == "__main__":
    unittest.main()

