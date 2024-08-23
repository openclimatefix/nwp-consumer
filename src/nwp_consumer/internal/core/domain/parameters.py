"""Domain model for NWP parameters.

NWP forecasts have to forecast something, and that something is the value
of one or more parameters. Often referred to as variables (or
channels once stored in a tensor), these parameters are physical, measurable
quantities that are forecasted by the model.

Variables are forecasted at different levels in the atmosphere. A common
level of interest is called "screen level (sl)". This corresponds to 1.5-2m
above the Earth's surface, which is the height of many measuring stations.

See Also:
    - https://datahub.metoffice.gov.uk/docs/glossary
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
    """Class containing information about a paremeter."""

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
    snow_depth: Parameter
    """Depth of snow on the ground (m)."""

parameters: Parameters = {
    "temperature_sl": Parameter(
        name="temperature_sl",
        description="Temperature at screen level",
        units="C",
        limits=ParameterLimits(upper=60, lower=-90),
    ),
    "relative_humidity_sl": Parameter(
        name="relative_humidity_sl",
        description="Relative humidity at screen level. "
            "Defined as the ratio of partial pressure of water vapour "
            "to the equilibrium vapour pressure of water",
        units="%",
        limits=ParameterLimits(upper=100, lower=0),
    ),
    "visibility_sl": Parameter(
        name="visibility_sl",
        description="Visibility at screen level. "
            "Defined as the distance at which an object can be seen "
            "horizontally in daylight conditions.",
        units="m",
        limits=ParameterLimits(upper=4500, lower=0),
    ),
    "wind_u_component_10m": Parameter(
        name="wind_u_component_10m",
        description="U component of wind at 10m above ground level. "
            "Defined as the horizontal speed of the wind in the eastward direction.",
        units="m/s",
        limits=ParameterLimits(upper=100, lower=-100)
    ),
    "wind_v_component_10m": Parameter(
        name="wind_v_component_10m",
        description="V component of wind at 10m above ground level. "
            "Defined as the horizontal speed of the wind in the northward direction.",
        units="m/s",
        limits=ParameterLimits(upper=100, lower=-100) # Non-tornadic winds are usually < 100m/s
    ),
    "wind_u_component_100m": Parameter(
        name="wind_u_component_100m",
        description="U component of wind at 100m above ground level. "
            "Defined as the horizontal speed of the wind in the eastward direction.",
        units="m/s",
        limits=ParameterLimits(upper=100, lower=-100)
    ),
    "wind_v_component_100m": Parameter(
        name="wind_v_component_100m",
        description="V component of wind at 100m above ground level. "
            "Defined as the horizontal speed of the wind in the northward direction.",
        units="m/s",
        limits=ParameterLimits(upper=100, lower=-100)
    ),
    "snow_depth": Parameter(
        name="snow_depth",
        description="Depth of snow on the ground.",
        units="m",
        limits=ParameterLimits(upper=12, lower=0)
    ),
}


