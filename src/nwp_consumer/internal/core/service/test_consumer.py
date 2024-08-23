import datetime as dt
import pathlib
import unittest

import numpy as np
import xarray as xr
from returns.pipeline import is_successful
from returns.result import Result

from nwp_consumer.internal.core import domain, ports

from .consumer import ParallelConsumer


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
                "latitude": np.linspace(90, -90, 721).tolist(),
                "longitude": np.linspace(-180, 179.8, 1440).tolist(),
            },
        )

    def fetch_init_data(self, it: dt.datetime) -> list[xr.Dataset]:
        """Overrides the corresponding method in the parent class."""

        # TODO: properly handle parameters
        return [
            xr.Dataset(
                {
                    "temperature_agl": (
                        ["init_time", "step", "latitude", "longitude"],
                        np.random.rand(1, 1, 721, 1440),
                    ),
                    "wind_u": (
                        ["init_time", "step", "latitude", "longitude"],
                        np.random.rand(1, 1, 721, 1440),
                    ),
                    "wind_v": (
                        ["init_time", "step", "latitude", "longitude"],
                        np.random.rand(1, 1, 721, 1440),
                    ),
                },
                coords=self.metadata.expected_coordinates | {
                    "init_time": [np.datetime64(it.replace(tzinfo=None), "ns")],
                    "step": [s],
                },
            )
            for s in self.metadata.expected_coordinates["step"]
        ]


class DummyNotificationRepository(ports.NotificationRepository):

    def notify(
            self,
            message: domain.StoreAppendedNotification | domain.StoreCreatedNotification,
    ) -> Result[str, Exception]:
        """Overrides the corresponding method in the parent class."""
        return Result.from_value(str(message))


class DummyZarrRepository(ports.ZarrRepository):

    def save(self, src: pathlib.Path, dst: pathlib.Path) -> Result[str, Exception]:
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

        self.assertTrue(is_successful(result), msg=result.failure())
        ds = result.map(xr.open_zarr)
