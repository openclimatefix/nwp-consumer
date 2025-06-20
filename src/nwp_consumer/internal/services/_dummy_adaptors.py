import datetime as dt
from collections.abc import Callable, Iterator
from typing import override

import numpy as np
import xarray as xr
from joblib import delayed
from returns.result import ResultE, Success

from nwp_consumer.internal import entities, ports


class DummyRawRepository(ports.RawRepository):
    @classmethod
    @override
    def authenticate(cls) -> ResultE["DummyRawRepository"]:
        return Success(cls())

    @staticmethod
    @override
    def repository() -> entities.RawRepositoryMetadata:
        return entities.RawRepositoryMetadata(
            name="ACME-Test-Models",
            is_archive=False,
            is_order_based=False,
            delay_minutes=60,
            max_connections=4,
            required_env=[],
            optional_env={},
            postprocess_options=entities.PostProcessOptions(),
            available_models={},
        )

    @staticmethod
    @override
    def model() -> entities.ModelMetadata:
        return entities.ModelMetadata(
            name="simple-random",
            resolution="17km",
            expected_coordinates=entities.NWPDimensionCoordinateMap(
                init_time=[dt.datetime(2021, 1, 1, 0, 0, tzinfo=dt.UTC)],
                step=list(range(0, 48, 1)),
                variable=[
                    entities.Parameter.TEMPERATURE_SL,
                    entities.Parameter.DOWNWARD_SHORTWAVE_RADIATION_FLUX_GL,
                    entities.Parameter.CLOUD_COVER_HIGH,
                ],
                latitude=np.linspace(90, -90, 721).tolist(),
                longitude=np.linspace(-180, 179.8, 1440).tolist(),
            ),
            running_hours=[0, 6, 12, 18],
        )

    @override
    def fetch_init_data(
        self, it: dt.datetime,
    ) -> Iterator[Callable[..., ResultE[list[xr.DataArray]]]]:
        def gen_dataset(step: int, variable: str) -> ResultE[list[xr.DataArray]]:
            """Define a generator that provides one variable at one step."""
            da = xr.DataArray(
                name=self.model().name,
                dims=["init_time", "step", "variable", "latitude", "longitude"],
                data=np.random.rand(1, 1, 1, 721, 1440),
                coords=self.model().expected_coordinates.to_pandas()
                | {
                    "init_time": [np.datetime64(it.replace(tzinfo=None), "ns")],
                    "step": [step],
                    "variable": [variable],
                },
            )
            return Success([da])

        for s in self.model().expected_coordinates.step:
            for v in self.model().expected_coordinates.variable:
                yield delayed(gen_dataset)(s, v.value)


class DummyNotificationRepository(ports.NotificationRepository):
    @override
    def notify(
        self,
        message: entities.StoreAppendedNotification | entities.StoreCreatedNotification,
    ) -> ResultE[str]:
        return Success(str(message))
