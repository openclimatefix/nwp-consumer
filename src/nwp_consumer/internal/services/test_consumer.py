import datetime as dt
import pathlib
import unittest
from collections.abc import Callable, Iterator
from typing import override

import numpy as np
import xarray as xr
from joblib import delayed
from returns.pipeline import is_successful
from returns.result import Result, ResultE

from nwp_consumer.internal import entities, ports
from nwp_consumer.internal.services.consumer_service import ConsumerService


class DummyModelRepository(ports.ModelRepository):

    @override
    @property
    def metadata(self) -> entities.ModelRepositoryMetadata:
        """See parent class."""
        return entities.ModelRepositoryMetadata(
            name="dummy",
            is_archive=False,
            is_order_based=False,
            running_hours=[0, 6, 12, 18],
            delay_minutes=60,
            max_connections=4,
            required_env=[],
            optional_env={},
            postprocess_options=entities.PostProcessOptions(),
            expected_coordinates=entities.NWPDimensionCoordinateMap(
                init_time=[dt.datetime(2021, 1, 1, 0, 0, tzinfo=dt.UTC)],
                step=list(range(0, 48, 1)),
                variable=[
                    entities.params.temperature_sl,
                    entities.params.downward_shortwave_radiation_flux_gl,
                    entities.params.cloud_cover_high,
                ],
                latitude=np.linspace(90, -90, 721).tolist(),
                longitude=np.linspace(-180, 179.8, 1440).tolist(),
            ),
        )

    @override
    def fetch_init_data(self, it: dt.datetime) -> Iterator[Callable[[], ResultE[xr.DataArray]]]:
        """See parent class."""

        def gen_dataset(s: int, variable: str) -> ResultE[xr.DataArray]:
            """Define a generator that provides one variable at one step."""
            da = xr.DataArray(
                name=self.metadata.name,
                dims=["init_time", "step", "variable", "latitude", "longitude"],
                data=np.random.rand(1, 1, 1, 721, 1440),
                coords=self.metadata.expected_coordinates.to_pandas() | {
                    "init_time": [np.datetime64(it.replace(tzinfo=None), "ns")],
                    "step": [s],
                    "variable": [variable],
                },
            )
            return Result.from_value(da)


        for s in self.metadata.expected_coordinates.step:
            for v in self.metadata.expected_coordinates.variable:
                yield delayed(gen_dataset)(s, v.name)


class DummyNotificationRepository(ports.NotificationRepository):

    @override
    def notify(
            self,
            message: entities.StoreAppendedNotification | entities.StoreCreatedNotification,
    ) -> ResultE[str]:
        """See parent class."""
        print(message)
        return Result.from_value(str(message))


class DummyZarrRepository(ports.ZarrRepository):

    @override
    def save(self, src: pathlib.Path, dst: pathlib.Path) -> ResultE[str]:
        """See parent class."""
        return Result.from_value(str(dst))


class TestParallelConsumer(unittest.TestCase):

    def test_consume(self) -> None:
        """Test the consume method of the ParallelConsumer class."""

        test_consumer = ConsumerService(
            model_repository=DummyModelRepository(),
            notification_repository=DummyNotificationRepository(),
            zarr_repository=DummyZarrRepository(),
        )

        result = test_consumer.consume(it=dt.datetime(2021, 1, 1, tzinfo=dt.UTC))

        self.assertTrue(is_successful(result), msg=f"Error: {result}")

        da: xr.DataArray = xr.open_dataarray(result.unwrap(), engine="zarr")

        self.assertEqual(
            list(da.sizes.keys()),
            ["init_time", "step", "variable", "latitude", "longitude"],
        )

