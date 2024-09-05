import datetime as dt
import unittest
import xarray as xr

from returns.pipeline import is_successful

from nwp_consumer.internal import entities, handlers, repositories, services


class TestIntegration(unittest.TestCase):
    def test_ceda_metoffice_global_model(self):
        c = handlers.CLIHandler(
            consumer_usecase=services.ConsumerService(
                model_repository=repositories.CedaMetOfficeGlobalModelRepository(),
                notification_repository=repositories.StdoutNotificationRepository(),
                zarr_repository=None,
            ),
        )
        result = c._consumer_usecase.consume(it=dt.datetime(2021, 1, 1, tzinfo=dt.UTC))

        self.assertTrue(is_successful(result), msg=f"{result}")

        da = xr.open_dataarray(result.unwrap(), engine="zarr")
        print(da)

