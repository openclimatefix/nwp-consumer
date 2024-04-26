"""Struct definitions for core domain objects.

Not every struct is a domain model! Only those involved in the business logic.
"""

import datetime as dt
import pathlib
from typing import Any

import attrs
import dask.array
import numpy as np
import pint
import xarray as xr
from argparse import Namespace

# Custom units
ureg = pint.UnitRegistry()
ureg.define("unit_interval = [] = ui")
ureg.define("octas = unit_interval * 1/8")
ureg.define("percent = unit_interval * 0.01")


@attrs.frozen
class Parameter:
    """A parameter that can be requested from a source repository.

    The level-based parameters must be set if parameter is multi-level.

    In OCF's case, some seeminlgy multi-level parameters will be specified as single-level
    (e.g. 10, 100, 200 meter wind speed). This is because the single/multi-level distinction
    is technically determining how the parameter is stored in the dataset, and isn't a
    semantic specifier.

    Multi-level parameters are often read from datasets by loading all levels at once
    (as this is more efficient and desirable from an ML perspective), and as such are stored
    with all levels in the same chunk. Wind speed however is more useful as a single-level-
    stored parameter, with different chunks for each height, as that is how it is typically
    used in forecasting.

    Attributes:
        longname: The full name of the parameter.
        shortname: A short name for the parameter.
        units: The units of the parameter.
        level_type: The type of level the parameter is defined on (singlelevel or multilevel).
        level_value: The number preceding the unit definining the level of the parameter.
        level_units: The units of the level value.
    """

    longname: str = attrs.field(validator=attrs.validators.min_len(3))
    shortname: str = attrs.field(validator=attrs.validators.max_len(10))
    units: pint.Unit
    level_type: str = attrs.field(
        default="single",
        validator=attrs.validators.in_(["single", "multi"]),
    )
    level_value: int | None = attrs.field(default=None)
    level_units: pint.Unit | None = attrs.field(default=None)

    @level_value.validator
    @level_units.validator
    def _defined_if_multilevel(
            self,
            attribute: attrs.Attribute,
            value: Any | None, # noqa: ANN401
        ) -> None:
        if self.level_type == "multi" and value is None:
            raise ValueError(f"{attribute.name} must be defined if level_type is 'multi'.")

    def at_level(self, level_value: int, level_units: pint.Unit) -> "Parameter":
        """Return a new multi-level parameter with the given level specification."""
        return attrs.evolve(
            self,
            level_type="multi",
            level_value=level_value,
            level_units=level_units,
        )

    def __repr__(self) -> str:
        """Return a representation of the parameter."""
        level_repr = f"_{self.level_value}{self.level_units}" if self.level_type == "multi" else ""
        return f"{self.longname}{level_repr}:{self.units}"

# Default parameters and units
PARAMS = Namespace(
    lcc=Parameter("low_cloud_cover", "lcc", ureg.Unit("ui")),
    mcc=Parameter("medium_cloud_cover", "mcc", ureg.Unit("ui")),
    hcc=Parameter("high_cloud_cover", "hcc", ureg.Unit("ui")),
    tcc=Parameter("total_cloud_cover", "tcc", ureg.Unit("ui")),
    vis=Parameter("visibility", "vis", ureg.Unit("m")),
    relhum=Parameter("relative_humidity", "r", ureg.Unit("percent")),
    prate=Parameter("precipitation_rate", "prate", ureg.Unit("kg m ** -2 s ** -1")),
    sdepth=Parameter("snow_depth", "sdepth", ureg.Unit("m")),
    dswrf=Parameter("downward_shortwave_radiation_flux", "dswrf", ureg.Unit("W m ** -2")),
    dlwrf=Parameter("downward_longwave_radiation_flux", "dlwrf", ureg.Unit("W m ** -2")),
    t=Parameter("temperature", "t", ureg.Unit("kelvin")),
    si10=Parameter("10m_wind_speed", "si10", ureg.Unit("m s ** -1")),
    wdir10=Parameter("10m_wind_direction", "wdir10", ureg.Unit("degrees")),
    u10=Parameter("10m_wind_u_component", "u10", ureg.Unit("m s ** -1")),
    v10=Parameter("10m_wind_v_component", "v10", ureg.Unit("m s ** -1")),
    u100=Parameter("100m_wind_u_component", "u100", ureg.Unit("m s ** -1")),
    v100=Parameter("100m_wind_v_component", "v100", ureg.Unit("m s ** -1")),
    u200=Parameter("200m_wind_u_component", "u200", ureg.Unit("m s ** -1")),
    v200=Parameter("200m_wind_v_component", "v200", ureg.Unit("m s ** -1")),
)


@attrs.frozen
class Area:
    """A geographical area, with bounding box in NWSE format.

    Lat/Long coordinates are in decimal degrees and must be given as signed floats as follows:
    - Latitudes north of the equator are *positive*.
    - Latitudes south of the equator are *negative*.
    - Longitudes east of the prime meridian are *positive*.
    - Longitudes west of the prime meridian are *negative*.
    """

    name: str
    north: float = attrs.field(validator=[attrs.validators.ge(-90), attrs.validators.le(90)])
    west: float = attrs.field(validator=[attrs.validators.ge(-180), attrs.validators.le(180)])
    south: float = attrs.field(validator=[attrs.validators.ge(-90), attrs.validators.le(90)])
    east: float = attrs.field(validator=[attrs.validators.ge(-180), attrs.validators.le(180)])

    def __str__(self) -> str:
        """String representation of the area."""
        return self.name + " (" + self.nwse() + ")"

    def nwse(self) -> str:
        """Return the bounding box as north/west/south/east string."""
        return "/".join([str(x) for x in (self.north, self.west, self.south, self.east)])

    def lats(self, resolution_degrees: float) -> list[float]:
        """Return the latitudes of the area at a given resolution."""
        return [self.north - i * resolution_degrees for i in range(self.nlats(resolution_degrees))]

    def lons(self, resolution_degrees: float) -> list[float]:
        """Return the longitudes of the area at a given resolution."""
        return [self.west + i * resolution_degrees for i in range(self.nlons(resolution_degrees))]

    def nlats(self, resolution_degrees: float) -> int:
        """Return the number of latitudes in the area at a given resolution."""
        # Add one to include the last latitude
        return round((self.north - self.south) / resolution_degrees) + 1

    def nlons(self, resolution_degrees: float) -> int:
        """Return the number of longitudes in the area at a given resolution."""
        # Add one to include the last longitude
        return round((self.east - self.west) / resolution_degrees) + 1

# Predefined areas
AREAS = Namespace(
    gl=Area("global", 90, -180, -90, 180),
    eu=Area("eu", 73.5, -27, 33, 45),
    nw_india=Area("nw_india", 31, 68, 20, 79),
    uk=Area("uk", 62, -12, 48, 3),
    malta=Area("malta", 37, 68, 20, 79),
)

@attrs.frozen
class DatasetDimensionMap:
    """Mapping of dimension labels to coordinate values for an NWP dataset.

    Can reasonably be thought of as a map of axis labels to the labels of
    each tick along that axis of a graph of the dataset.
    """

    init_time: list[np.datetime64]
    step: list[np.timedelta64]
    latitude: list[float]
    longitude: list[float]

    def shape(self) -> dict[str, int]:
        """Return the shape specified by the dimension mapping.

        Returns:
            Dictionary with keys corresponding to the coordinate names
            and values corresponding to the number of ticks along the
            coordinate axis.
        """
        return {k: len(v) for k, v in attrs.asdict(self).items()}

@attrs.frozen
class DataRequest:
    """A request for data for an init time from a source repository."""

    area: Area
    steps: list[int] = attrs.field(validator=attrs.validators.min_len(1))
    parameters: list[str] = attrs.field(validator=attrs.validators.min_len(1))
    init_time: dt.datetime

    def as_dataset(self, resolution_degrees: float) -> xr.Dataset:
        """Return a dummy dataset according to the request.

        The request is used to define the dimension labels and tick values of the output
        dataset object, as well as the data variables tracked within the dataset and
        their dependence on the dimensions.

        No actual data is defined on the produced dataset. As such, storing it as a zarr via
        ```
        dataset.to_zarr('dummy.zarr', compute=False)
        ```
        will result in a zarr store containing the metadata alone. The utility of this is
        to enable region-based writing of new data to the store, which can be done using
        parallel processes.

        There is a gotcha: regional writes can never be done in parallel to the same chunk,
        so writes must always be done at the chunk level or higher (as a chunk is an
        individual file in the store). In this manner chunks are chosen to cover as small
        a unit of data as could reasonbaly be expected to be provided by an NWP source:
        - Raw data files will always contain the full grid of data, hence 1 chunk per
          grid dimension (lat/lon/x/y axes) is sufficient.
        - Raw data files may contain as little as one step for a single parameter, so equate
          the number of chunks to the number of steps along the step dimension.
        """
        coords = self.as_dataset_dimension_map(resolution_degrees)
        data_vars = {
            p: (
                ("init_time", "step", "latitude", "longitude"),
                dask.array.zeros(coords.shape(), chunks=(1, len(self.steps), 1, 1)),
            ) for p in self.parameters
        }

        return xr.Dataset(data_vars=data_vars, coords=attrs.asdict(coords))

    def as_dataset_dimension_map(self, resolution_degrees: float) -> DatasetDimensionMap:
        """Return the request as a mapping of dataset dimension labels to values."""
        return DatasetDimensionMap(
            # Convert to UTC and remove timezone info to prevent numpy complaints
            init_time=[
                np.datetime64(self.init_time.astimezone(tz=dt.UTC).replace(tzinfo=None), "ns"),
            ],
            # Manually specify as timedelta64[ns] to prevent xarray complaints
            step=[np.timedelta64(np.timedelta64(i, "h"), "ns") for i in self.steps],
            latitude=self.area.lats(resolution_degrees),
            longitude=self.area.lons(resolution_degrees),
        )

    def total_values(self, resolution_degrees: float) -> int:
        """Return the total number of data points specified by the request definition."""
        return (
            len(self.steps)
            * self.area.nlats(resolution_degrees)
            * self.area.nlons(resolution_degrees)
            * len(self.parameters)
        )

@attrs.frozen
class SourceRepositoryMetadata:
    """Metadata for a source repository.

    Attributes:
        name: The name of the repository.
        is_archive: Whether the repository is a complete archival set.
            Archival datasets are able to be used to backfill old data.
            Non archival datasets only provide a limited window of data.
        is_order_based: Whether the repository is order-based.
            This means parameters cannot be chosen freely,
            but rather are defined by pre-selected agreements
            with the provider.
        running_hours: The running hours or the source.
        available_steps: The available steps of the repository.
        available_areas: The available areas of the repository.
        required_env: Environmant variables required for usage.
    """

    name: str = attrs.field(validator=attrs.validators.min_len(3), type=str)
    is_archive: bool
    is_order_based: bool
    running_hours: list[int]
    available_steps: list[int]
    available_areas: list[Area]
    required_env: list[str]

@attrs.frozen
class SourceFileMetadata:
    """Metadata for a raw file."""

    name: str
    path: pathlib.Path
    extension: str
    size: int
    steps: list[int]
    parameters: list[str]
    init_time: dt.datetime

