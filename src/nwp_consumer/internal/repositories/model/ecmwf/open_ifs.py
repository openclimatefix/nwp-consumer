import datetime as dt
import pathlib

from returns.io import IOResultE
from returns.result import ResultE
import numpy as np

from nwp_consumer.internal import entities, ports


class ECMWFOpenIFS(ports.ModelRepository):
    """Repository for ECMWF OpenIFS model data."""

    @property
    def metadata(self) -> entities.ModelRepositoryMetadata:
        return entities.ModelRepositoryMetadata(
            name="ecmwf_open_ifs",
            is_archive=False,
            is_order_based=False,
            running_hours=[0, 6, 12, 18],
            delay_minutes=420,
            required_env=[],
            optional_env={},
            steps=list(range(0, 90, 3)),
            spatial_coordinates={
                "lat": np.linspace(90, -90, 721).tolist(),
                "lon": np.linspace(-180, 179.8, 1440).tolist(),
            },
        )

    def map_file(self, cached_file: entities.ModelFileMetadata, store_metadata: entities.StoreMetadata) -> ResultE[
        entities.ModelFileMetadata]:
        pass

    def download_file(self, file: entities.ModelFileMetadata) -> IOResultE[entities.ModelFileMetadata]:
        pass

    def list_fileset(self, it: dt.datetime) -> IOResultE[list[entities.ModelFileMetadata]]:

        [
            entities.ModelFileMetadata(
                name=f"{it:%Y%m%d%H%M%s}-{s}h-oper-fc.grib2",
                path=pathlib.Path(
                    "https://storage.cloud.google.com/ecmwf-open-data"
                    f"{it:%Y%m%d}/{it:%H}z/ifs/0p25/oper/{it:%Y%m%d%H%M%s}-{s}h-oper-fc.grib2",
                ),
                scheme="https",
                extension=".grib2",
                size_bytes=1*pow(10, 6),
                parameters=["temperature_agl", "wind_u_component_10m", "wind_v_component_10m"],
                coordinates={}
            ) for s in self.metadata.steps
        ]





