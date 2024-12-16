import datetime as dt
import unittest

import xarray as xr
from returns.pipeline import is_successful
from returns.result import ResultE

from nwp_consumer.internal import repositories, services


class TestIntegration(unittest.TestCase):
    def test_ceda_metoffice_global_model(self) -> None:
        test_it =dt.datetime(2021, 1, 1, tzinfo=dt.UTC)
        service_result = services.ConsumerService.from_adaptors(
            model_adaptor=repositories.model_repositories.CEDAFTPModelRepository,
            notification_adaptor=repositories.notification_repositories.StdoutNotificationRepository,
        )
        result: ResultE[str] = service_result.do(
            consume_result
            for service in service_result
            for consume_result in service.consume(period=test_it)
        )

        self.assertTrue(is_successful(result), msg=f"{result}")

        da = xr.open_dataarray(result.unwrap(), engine="zarr")

        self.assertTrue(da.sizes["init_time"] > 0)

