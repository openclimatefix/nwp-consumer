"""Domain classes for Models and their metadata.

Sources of NWP data have attributes both pertaining to and apart from
the data they deliver. This module defines classes for metadata that
tracks relevant information about the model repository and the data
it provides. This might be helpful in determining the quality of the
data, defining pipelines for processing, or establishing the availability
for a live service.

In this instance, the `ModelMetadata` refers to information pertaining
to the model used to generate the data itself.
"""

import dataclasses
import logging

import numpy as np

from .coordinates import NWPDimensionCoordinateMap
from .parameters import Parameter

log = logging.getLogger("nwp-consumer")


@dataclasses.dataclass(slots=True)
class ModelMetadata:
    """Metadata for an NWP model."""

    name: str
    """The name of the model.

    Used to name the tensor in the zarr store.
    """

    resolution: str
    """The resolution of the model with units."""

    expected_coordinates: NWPDimensionCoordinateMap
    """The expected dimension coordinate mapping.

    This is a dictionary mapping dimension labels to their coordinate values,
    for a single init time dataset, e.g.

    >>> {
    >>>     "init_time": [dt.datetime(2021, 1, 1, 0, 0), ...],
    >>>     "step": [1, 2, ...],
    >>>     "latitude": [90, 89.75, 89.5, ...],
    >>>     "longitude": [180, 179, ...],
    >>> }

    To work this out, it can be useful to use the 'grib_ls' tool from eccodes:

    >>> grib_ls -n geography -wcount=13 raw_file.grib

    Which prints grid data from the grib file.
    """

    chunk_count_overrides: dict[str, int] = dataclasses.field(default_factory=dict)
    """Mapping of dimension names to the desired number of chunks in that dimension.

    Overrides the default chunking strategy.

    See Also:
        - `entities.coordinates.NWPDimensionCoordinateMap.chunking`
    """

    def __str__(self) -> str:
        """Return a pretty-printed string representation of the metadata."""
        pretty: str = "".join((
            "Model:",
            "\n\t{self.name} ({self.resolution} resolution)",
            "\tCoordinates:",
            "\n".join(
                f"\t\t{dim}: {vals}"
                if len(vals) < 5
                else f"\t\t{dim}: {vals[:3]} ... {vals[-3:]}"
                for dim, vals in self.expected_coordinates.__dict__.items()
            ),
        ))
        return pretty

    def with_region(self, region: str) -> "ModelMetadata":
        """Returns metadata for the given model cropped to the given region.

        If an unknown region is given, the metadata is returned unchanged,
        with a warning.
        """
        match region:
            case "uk":
                return self.expected_coordinates.crop(
                    north=62, west=-12, south=48, east=3,
                ).map(lambda coords: dataclasses.replace(
                    self, name=f"{self.name}_uk", expected_coordinates=coords,
                )).unwrap()
            case "india":
                return self.expected_coordinates.crop(
                    north=35, west=67, south=6, east=97,
                ).map(lambda coords: dataclasses.replace(
                    self, name=f"{self.name}_india", expected_coordinates=coords,
                )).unwrap()
            case _:
                log.warning(f"Unknown region '{region}', not cropping expected coordinates.")
                return self

    def with_chunk_count_overrides(self, overrides: dict[str, int]) -> "ModelMetadata":
        """Returns metadata for the given model with the given chunk count overrides."""
        if not set(overrides.keys()).issubset(self.expected_coordinates.dims):
            log.warning(
                "Chunk count overrides contain keys not in the expected coordinates. "
                "These will not modify the chunking strategy.",
            )
        return dataclasses.replace(self, chunk_count_overrides=overrides)

class Models:
    """Namespace containing known models."""

    ECMWF_HRES_IFS_0P1DEGREE: ModelMetadata = ModelMetadata(
        name="HRES-IFS",
        resolution="0.1 degrees",
        expected_coordinates=NWPDimensionCoordinateMap(
            init_time=[],
            step=list(range(0, 85, 1)),
            variable=[
                Parameter.WIND_U_COMPONENT_10m,
                Parameter.WIND_V_COMPONENT_10m,
                Parameter.WIND_U_COMPONENT_100m,
                Parameter.WIND_V_COMPONENT_100m,
                Parameter.WIND_U_COMPONENT_200m,
                Parameter.WIND_V_COMPONENT_200m,
                Parameter.TEMPERATURE_SL,
                Parameter.TOTAL_PRECIPITATION_RATE_GL,
                Parameter.DOWNWARD_SHORTWAVE_RADIATION_FLUX_GL,
                Parameter.DOWNWARD_LONGWAVE_RADIATION_FLUX_GL,
                Parameter.CLOUD_COVER_HIGH,
                Parameter.CLOUD_COVER_MEDIUM,
                Parameter.CLOUD_COVER_LOW,
                Parameter.CLOUD_COVER_TOTAL,
                Parameter.SNOW_DEPTH_GL,
                Parameter.VISIBILITY_SL,
                Parameter.DIRECT_SHORTWAVE_RADIATION_FLUX_GL,
                Parameter.DOWNWARD_ULTRAVIOLET_RADIATION_FLUX_GL,
            ],
            latitude=[float(f"{lat / 10:.2f}") for lat in range(900, -900 - 1, -1)],
            longitude=[float(f"{lon / 10:.2f}") for lon in range(-1800, 1800 + 1, 1)],
        ),
    )
    """ECMWF's High Resolution Integrated Forecast System."""

    ECMWF_ENS_STAT_0P1DEGREE: ModelMetadata = ModelMetadata(
        name="ENS-Stat",
        resolution="0.1 degrees",
        expected_coordinates=NWPDimensionCoordinateMap(
            init_time=[],
            step=list(range(0, 85, 3)),
            variable=[
                Parameter.PRESSURE_MSL,
                Parameter.WIND_SPEED_10m,
                Parameter.WIND_SPEED_100m,
                Parameter.TEMPERATURE_SL,
            ],
            ensemble_stat=["mean", "std", "P10", "P25", "P75", "P90"],
            latitude=[v/10 for v in range(900, -900, -1)],
            longitude=[v/10 for v in range(-1800, 1800, 1)],
        ),
    )
    """Summary statistics from ECMWF's Ensemble Forecast System."""

    NCEP_GFS_1DEGREE: ModelMetadata = ModelMetadata(
        name="NCEP-GFS",
        resolution="1 degree",
        expected_coordinates=NWPDimensionCoordinateMap(
            init_time=[],
            step=list(range(0, 49, 3)),
            variable=sorted(
                [
                    Parameter.TEMPERATURE_SL,
                    Parameter.CLOUD_COVER_TOTAL,
                    Parameter.CLOUD_COVER_HIGH,
                    Parameter.CLOUD_COVER_MEDIUM,
                    Parameter.CLOUD_COVER_LOW,
                    Parameter.DOWNWARD_SHORTWAVE_RADIATION_FLUX_GL,
                    Parameter.DOWNWARD_LONGWAVE_RADIATION_FLUX_GL,
                    Parameter.TOTAL_PRECIPITATION_RATE_GL,
                    Parameter.SNOW_DEPTH_GL,
                    Parameter.RELATIVE_HUMIDITY_SL,
                    Parameter.VISIBILITY_SL,
                    Parameter.WIND_U_COMPONENT_10m,
                    Parameter.WIND_V_COMPONENT_10m,
                    Parameter.WIND_U_COMPONENT_100m,
                    Parameter.WIND_V_COMPONENT_100m,
                ],
            ),
            latitude=[float(lat) for lat in range(90, -90 - 1, -1)],
            longitude=[float(lon) for lon in range(-180, 180 + 1, 1)],
        ),
    )
    """NCEP's Global Forecast System."""

    MO_UM_GLOBAL_17KM: ModelMetadata = ModelMetadata(
        name="UM-Global",
        resolution="17km",
        expected_coordinates = NWPDimensionCoordinateMap(
            init_time=[],
            step=list(range(0, 48, 1)),
            variable=[
                Parameter.DOWNWARD_SHORTWAVE_RADIATION_FLUX_GL,
                Parameter.CLOUD_COVER_TOTAL,
                Parameter.CLOUD_COVER_HIGH,
                Parameter.CLOUD_COVER_LOW,
                Parameter.CLOUD_COVER_MEDIUM,
                Parameter.RELATIVE_HUMIDITY_SL,
                Parameter.SNOW_DEPTH_GL,
                Parameter.TEMPERATURE_SL,
                Parameter.WIND_U_COMPONENT_10m,
                Parameter.WIND_V_COMPONENT_10m,
                Parameter.VISIBILITY_SL,
            ],
            latitude=[
                float(f"{lat:.4f}") for lat in np.arange(89.856, -89.856 - 0.156, -0.156)
            ],
            longitude=[
                float(f"{lon:.4f}") for lon in np.concatenate([
                    np.arange(-45, 45, 0.234),
                    np.arange(45, 135, 0.234),
                    np.arange(135, 225, 0.234),
                    np.arange(225, 315, 0.234),
                ])
            ],
            # TODO: Change to -180 -> 180
        ),
    )
    """MetOffice's Unified Model, in the Global configuration, at a resolution of 17km."""

    MO_UM_GLOBAL_10KM: ModelMetadata = ModelMetadata(
        name="UM-Global",
        resolution="10km",
        expected_coordinates=NWPDimensionCoordinateMap(
            init_time=[],
            step=list(range(0, 55)),
            variable=sorted(
                [
                    Parameter.CLOUD_COVER_TOTAL,
                    Parameter.CLOUD_COVER_HIGH,
                    Parameter.CLOUD_COVER_MEDIUM,
                    Parameter.CLOUD_COVER_LOW,
                    Parameter.VISIBILITY_SL,
                    Parameter.RELATIVE_HUMIDITY_SL,
                    Parameter.SNOW_DEPTH_GL,
                    Parameter.DOWNWARD_SHORTWAVE_RADIATION_FLUX_GL,
                    Parameter.TEMPERATURE_SL,
                    Parameter.WIND_U_COMPONENT_10m,
                    Parameter.WIND_V_COMPONENT_10m,
                ],
            ),
            latitude=[
                float(f"{lat:.4f}")
                for lat in np.arange(89.953125, -89.953125 - 0.09375, -0.09375)
            ],
            longitude=[
                float(f"{lon:.4f}")
                for lon in np.arange(-179.929687, 179.929688 + 0.140625, 0.140625)
            ],
        ),
    )
    """MetOffice's Unified Model, in the Global configuration, at a resolution of 10km."""

