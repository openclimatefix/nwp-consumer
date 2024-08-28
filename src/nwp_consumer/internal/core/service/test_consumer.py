import datetime as dt
import os
import pathlib
import time
import unittest
from collections.abc import Callable, Iterator

import numpy as np
import xarray as xr
import cProfile

from joblib import delayed
from returns.pipeline import is_successful
from returns.result import Result, ResultE

from nwp_consumer.internal.core import domain, ports
from nwp_consumer.internal.core.service.consumer import ParallelConsumer


class DummyModelRepository(ports.ModelRepository):

    @property
    def metadata(self) -> domain.ModelRepositoryMetadata:
        """Overrides the corresponding method in the parent class."""
        return domain.ModelRepositoryMetadata(
            name="dummy",
            is_archive=False,
            is_order_based=False,
            running_hours=[0, 6, 12, 18],
            delay_minutes=60,
            required_env=[],
            optional_env={},
            expected_coordinates={
                "init_time": [np.datetime64("1970-01-01T00:00", "ns")],
                "step": [
                    np.timedelta64(np.timedelta64(h, "h"), "ns")
                    for h in range(0, 48, 1)
                ],
                "variable": [
                    domain.params.temperature_sl.name,
                    domain.params.downward_shortwave_radiation_flux_gl.name,
                    domain.params.cloud_cover_high.name,
                ],
                "latitude": np.linspace(90, -90, 721).tolist(),
                "longitude": np.linspace(-180, 179.8, 1440).tolist(),
            },
        )

    def fetch_init_data(self, it: dt.datetime) -> Iterator[Callable[[],xr.Dataset]]:
        """Overrides the corresponding method in the parent class."""

        def gen_dataset(s: int, variable: str) -> xr.Dataset:
            """Define a generator that provides one variable at one step."""
            ds = xr.Dataset({
                self.metadata.name: (
                        ["init_time", "step", "variable", "latitude", "longitude"],
                        np.random.rand(1, 1, 1, 721, 1440),
                    ),
                },
                coords=self.metadata.expected_coordinates | {
                    "init_time": [np.datetime64(it.replace(tzinfo=None), "ns")],
                    "step": [s],
                    "variable": [variable],
            })
            return ds


        for s in self.metadata.expected_coordinates["step"]:
            for v in self.metadata.expected_coordinates["variable"]:
                yield delayed(gen_dataset)(s, v)


class DummyNotificationRepository(ports.NotificationRepository):

    def notify(
            self,
            message: domain.StoreAppendedNotification | domain.StoreCreatedNotification,
    ) -> ResultE[str]:
        """Overrides the corresponding method in the parent class."""
        print(message)
        return Result.from_value(str(message))


class DummyZarrRepository(ports.ZarrRepository):

    def save(self, src: pathlib.Path, dst: pathlib.Path) -> ResultE[str]:
        """Overrides the corresponding method in the parent class."""
        return Result.from_value(str(dst))


class TestParallelConsumer(unittest.TestCase):

    def test_consume(self) -> None:
        """Test the consume method of the ParallelConsumer class."""

        test_consumer = ParallelConsumer(
            model_repository=DummyModelRepository(),
            notification_repository=DummyNotificationRepository(),
            zarr_repository=DummyZarrRepository(),
        )

        result = test_consumer.consume(it=dt.datetime(2021, 1, 1, tzinfo=dt.UTC))

        self.assertTrue(is_successful(result), msg=f"Error: {result}")

        ds = xr.open_zarr(result.unwrap())
        print(ds)

