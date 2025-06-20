import datetime as dt
import shutil
import unittest

import xarray as xr
from returns.pipeline import is_successful

from nwp_consumer.internal.services.consumer_service import ConsumerService

from ._dummy_adaptors import DummyNotificationRepository, DummyRawRepository


class TestParallelConsumer(unittest.TestCase):
    def test_consume(self) -> None:
        """Test the consume method of the ParallelConsumer class."""

        test_consumer = ConsumerService.from_adaptors(
            model_adaptor=DummyRawRepository,
            notification_adaptor=DummyNotificationRepository,
        ).unwrap()

        result = test_consumer.consume(period=dt.datetime(2021, 1, 1, tzinfo=dt.UTC))

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
