import datetime as dt
import unittest

import xarray as xr
from returns.pipeline import is_successful

from nwp_consumer.internal import handlers, repositories, services


class TestIntegration(unittest.TestCase):
    def test_ceda_metoffice_global_model(self) -> None:
        c = handlers.CLIHandler(
            consumer_usecase=services.ConsumerService(
                model_repository=repositories.CedaMetOfficeGlobalModelRepository,
                notification_repository=repositories.StdoutNotificationRepository,
            ),
            archiver_usecase=services.ArchiverService(
                model_repository=repositories.CedaMetOfficeGlobalModelRepository,
                notification_repository=repositories.StdoutNotificationRepository,
            ),
        )
        result = c._consumer_usecase.consume(it=dt.datetime(2021, 1, 1, tzinfo=dt.UTC))

        self.assertTrue(is_successful(result), msg=f"{result}")

        da = xr.open_dataarray(result.unwrap(), engine="zarr")

        self.assertTrue(da.sizes["init_time"] > 0)
