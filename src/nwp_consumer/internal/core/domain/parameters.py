"""Domain model for NWP parameters.

NWP forecasts have to forecast something, and that something is the value
of one or more parameters. Often referred to as variables (or
channels once stored in a tensor), these parameters are physical, measurable
quantities that are forecasted by the model.

Variables are forecasted at different levels in the atmosphere. A common
level of interest is called "screen level (sl)". This corresponds to 1.5-2m
above the Earth's surface, which is the height of many measuring stations.

Variables also have units, which are the physical quantities that the
variable is measured in. For example, temperature is measured in degrees
Celsius (C), and wind speed is measured in meters per second (m/s).
Some variables just occupy the range [0, 1], such as cloud cover.
This unit is referred to as the Unit Interval (UI).

See Also:
    - https://datahub.metoffice.gov.uk/docs/glossary
    - https://codes.ecmwf.int/grib/param-db
"""

import dataclasses
from typing import TypedDict


@dataclasses.dataclass
class ParameterLimits:
    """Class containing information about the limits of a parameter."""

    upper: float
    """The upper limit on the parameter value.

    Not an absolute maximum, but rather the maximum value that
    the parameter can resonably be expected to take.

    As an example, the maximum distance that can be seen horizontally
    is 4.5km at sea level, so the upper limit for a visibility
    parameter should be ~4500m.
    """

    lower: float
    """The lower limit on the parameter value.

    Not an absolute minimum, but rather the minimum value that
    the parameter can resonably be expected to take.

    As an example, the minimum temperature on Earth is -89C,
    so the lower limit for a temperature parameter should be ~ -90C.
    """

    threshold: float = 0.05
    """How carefully to enforce the limits.

    The threshold defines the number of values that can be outside
    the limits in a given dataset before the data is considered invalid.
    Denoted as a ratio (#outside/#total).
    """


@dataclasses.dataclass
class Parameter:
    """Class containing information about a parameter."""

    name: str
    """The name of the parameter as appears in produced datasets."""

    description: str
    """A brief description of the parameter."""

    units: str
    """The units of the parameter."""

    limits: ParameterLimits
    """Reasonable physical limits for the parameter.

    Used in sanity and validity checking the database values.
    """


class Parameters(TypedDict):
    """A dictionary of parameters."""
    temperature_sl: Parameter
    """Temperature at screen level (C)."""
    downward_shortwave_radiation_flux_gl: Parameter
    """Downward shortwave radiation flux at ground level (W/m^2)."""
    downward_longwave_radiation_flux_gl: Parameter
    """Downward longwave radiation flux at ground level (W/m^2)."""
    relative_humidity_sl: Parameter
    """Relative humidity at screen level (%)."""
    visibility_sl: Parameter
    """Visibility at screen level (m)."""
    wind_u_component_10m: Parameter
    """U component of wind at 10m above ground level (m/s)."""
    wind_v_component_10m: Parameter
    """V component of wind at 10m above ground level (m/s)."""
    wind_u_component_100m: Parameter
    """U component of wind at 100m above ground level (m/s)."""
    wind_v_component_100m: Parameter
    """V component of wind at 100m above ground level (m/s)."""
    snow_depth_sfc: Parameter
    """Depth of snow on the ground (m)."""
    cloud_cover_high: Parameter
    """Fraction of grid square covered by high-level cloud (UI)."""
    cloud_cover_medium: Parameter
    """Fraction of grid square covered by medium-level cloud (UI)."""
    cloud_cover_low: Parameter
    """Fraction of grid square covered by low-level cloud (UI)."""
    total_precipitation_rate_gl: Parameter
    """Total precipitation rate at ground level (kg/m^2/s)."""


parameters = Parameters(
    temperature_sl=Parameter(
        name="temperature_sl",
        description="Temperature at screen level",
        units="C",
        limits=ParameterLimits(upper=60, lower=-90),
    ),
    downward_shortwave_radiation_flux_gl=Parameter(
        name="downward_shortwave_radiation_flux_gl",
        description="Downward shortwave radiation flux at ground level. "
            "Defined as the mean amount of solar radiation incident on the surface "
            "expected over the next hour.",
        units="W/m^2",
        limits=ParameterLimits(upper=1500, lower=0)
    ),
    downward_longwave_radiation_flux_gl=Parameter(
        name="downward_longwave_radiation_flux_gl",
        description="Downward longwave radiation flux at ground level. "
            "Defined as the mean amount of thermal radiation incident on the surface "
            "expected over the next hour.",
        units="W/m^2",
        limits=ParameterLimits(upper=500, lower=0)
    ),
    relative_humidity_sl=Parameter(
        name="relative_humidity_sl",
        description="Relative humidity at screen level. "
                    "Defined as the ratio of partial pressure of water vapour "
                    "to the equilibrium vapour pressure of water",
        units="%",
        limits=ParameterLimits(upper=100, lower=0),
    ),
    visibility_sl=Parameter(
        name="visibility_sl",
        description="Visibility at screen level. "
                    "Defined as the distance at which an object can be seen "
                    "horizontally in daylight conditions.",
        units="m",
        limits=ParameterLimits(upper=4500, lower=0),
    ),
    wind_u_component_10m=Parameter(
        name="wind_u_component_10m",
        description="U component of wind at 10m above ground level. "
                    "Defined as the horizontal speed of the wind in the eastward direction.",
        units="m/s",
        limits=ParameterLimits(upper=100, lower=-100)
    ),
    wind_v_component_10m=Parameter(
        name="wind_v_component_10m",
        description="V component of wind at 10m above ground level. "
                    "Defined as the horizontal speed of the wind in the northward direction.",
        units="m/s",
        limits=ParameterLimits(upper=100, lower=-100)  # Non-tornadic winds are usually < 100m/s
    ),
    wind_u_component_100m=Parameter(
        name="wind_u_component_100m",
        description="U component of wind at 100m above ground level. "
                    "Defined as the horizontal speed of the wind in the eastward direction.",
        units="m/s",
        limits=ParameterLimits(upper=100, lower=-100)
    ),
    wind_v_component_100m=Parameter(
        name="wind_v_component_100m",
        description="V component of wind at 100m above ground level. "
                    "Defined as the horizontal speed of the wind in the northward direction.",
        units="m/s",
        limits=ParameterLimits(upper=100, lower=-100)
    ),
    snow_depth_gl=Parameter(
        name="snow_depth_gl",
        description="Depth of snow on the ground.",
        units="m",
        limits=ParameterLimits(upper=12, lower=0)
    ),
    cloud_cover_high=Parameter(
        name="cloud_cover_high",
        description="Fraction of grid square covered by high-level cloud. "
            "Defined as the ratio of the area of the grid square covered by high-level "
            "(>6km) cloud to the square's total area.",
        units="UI",
        limits=ParameterLimits(upper=1, lower=0)
    ),
    cloud_cover_medium=Parameter(
        name="cloud_cover_medium",
        description="Fraction of grid square covered by medium-level cloud. "
            "Defined as the ratio of the area of the grid square covered by medium-level "
            "(2-6km) cloud to the square's total area.",
        units="UI",
        limits=ParameterLimits(upper=1, lower=0)
    ),
    cloud_cover_low=Parameter(
        name="cloud_cover_low",
        description="Fraction of grid square covered by low-level cloud. "
            "Defined as the ratio of the area of the grid square covered by low-level "
            "(<2km) cloud to the square's total area.",
        units="UI",
        limits=ParameterLimits(upper=1, lower=0)
    ),
    total_precipitation_rate_gl=Parameter(
        name="total_precipitation_rate_gl",
        description="Total precipitation rate at ground level. "
            "Defined as the rate at which liquid is deposited on the ground "
            "including rain, snow, and hail.",
        units="kg/m^2/s",
        limits=ParameterLimits(upper=0.2, lower=0)
    ),
}
