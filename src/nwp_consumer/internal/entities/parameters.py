"""Domain entities for NWP parameters.

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
from enum import StrEnum, auto


@dataclasses.dataclass(slots=True)
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
    the parameter can reasonably be expected to take.

    As an example, the minimum temperature on Earth is -89C,
    so the lower limit for a temperature parameter should be ~ -90C.
    """

    threshold: float = 0.05
    """How carefully to enforce the limits.

    The threshold defines the number of values that can be outside
    the limits in a given dataset before the data is considered invalid.
    Denoted as a ratio (#outside/#total).
    """


@dataclasses.dataclass(slots=True, frozen=True)
class ParameterData:
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

    def __str__(self) -> str:
        """String representation of the parameter."""
        return self.name


class Parameter(StrEnum):
    """Parameters of interest to OCF.

    Inheriting from StrEnum and using ``auto()`` makes the values
    of the enums equal to the lowercased enum name.

    See Also:
        - https://docs.python.org/3/library/enum.html#enum.StrEnum
    """

    TEMPERATURE_SL = auto()
    DOWNWARD_SHORTWAVE_RADIATION_FLUX_GL = auto()
    DOWNWARD_LONGWAVE_RADIATION_FLUX_GL = auto()
    RELATIVE_HUMIDITY_SL = auto()
    VISIBILITY_SL = auto()
    WIND_U_COMPONENT_10m = auto()
    WIND_V_COMPONENT_10m = auto()
    WIND_U_COMPONENT_100m = auto()
    WIND_V_COMPONENT_100m = auto()
    WIND_U_COMPONENT_200m = auto()
    WIND_V_COMPONENT_200m = auto()
    SNOW_DEPTH_GL = auto()
    CLOUD_COVER_HIGH = auto()
    CLOUD_COVER_MEDIUM = auto()
    CLOUD_COVER_LOW = auto()
    CLOUD_COVER_TOTAL = auto()
    TOTAL_PRECIPITATION_RATE_GL = auto()
    DOWNWARD_ULTRAVIOLET_RADIATION_FLUX_GL = auto()
    DIRECT_SHORTWAVE_RADIATION_FLUX_GL = auto()

    def metadata(self) -> ParameterData:
        """Get the metadata for the parameter."""
        match self.name:
            case self.TEMPERATURE_SL.name:
                return ParameterData(
                    name=str(self),
                    description="Temperature at screen level",
                    units="C",
                    limits=ParameterLimits(upper=60, lower=-90),
                )
            case self.DOWNWARD_SHORTWAVE_RADIATION_FLUX_GL.name:
                return ParameterData(
                    name=str(self),
                    description="Downward shortwave radiation flux at ground level. "
                                "Defined as the mean amount of solar radiation "
                                "incident on the surface expected over the next hour.",
                    units="W/m^2",
                    limits=ParameterLimits(upper=1500, lower=0),
                )
            case self.DOWNWARD_LONGWAVE_RADIATION_FLUX_GL.name:
                return ParameterData(
                    name=str(self),
                    description="Downward longwave radiation flux at ground level. "
                                "Defined as the mean amount of thermal radiation "
                                "incident on the surface expected over the next hour.",
                    units="W/m^2",
                    limits=ParameterLimits(upper=500, lower=0),
                )
            case self.RELATIVE_HUMIDITY_SL.name:
                return ParameterData(
                    name=str(self),
                    description="Relative humidity at screen level. "
                                "Defined as the ratio of partial pressure of water vapour "
                                "to the equilibrium vapour pressure of water",
                    units="%",
                    limits=ParameterLimits(upper=100, lower=0),
                )
            case self.VISIBILITY_SL.name:
                return ParameterData(
                    name=str(self),
                    description="Visibility at screen level. "
                                "Defined as the distance at which an object can be seen "
                                "horizontally in daylight conditions.",
                    units="m",
                    limits=ParameterLimits(upper=4500, lower=0),
                )
            case self.WIND_U_COMPONENT_10m.name:
                return ParameterData(
                    name=str(self),
                    description="U component of wind at 10m above ground level. "
                                "Defined as the horizontal speed of "
                                "the wind in the eastward direction.",
                    units="m/s",
                    limits=ParameterLimits(upper=100, lower=-100),
                )
            case self.WIND_V_COMPONENT_10m.name:
                return ParameterData(
                    name=str(self),
                    description="V component of wind at 10m above ground level. "
                                "Defined as the horizontal speed of "
                                "the wind in the northward direction.",
                    units="m/s",
                    # Non-tornadic winds are usually < 100m/s
                    limits=ParameterLimits(upper=100, lower=-100),
                )
            case self.WIND_U_COMPONENT_100m.name:
                return ParameterData(
                    name=str(self),
                    description="U component of wind at 100m above ground level. "
                                "Defined as the horizontal speed of "
                                "the wind in the eastward direction.",
                    units="m/s",
                    limits=ParameterLimits(upper=100, lower=-100),
                )
            case self.WIND_V_COMPONENT_100m.name:
                return ParameterData(
                    name=str(self),
                    description="V component of wind at 100m above ground level. "
                                "Defined as the horizontal speed of "
                                "the wind in the northward direction.",
                    units="m/s",
                    limits=ParameterLimits(upper=100, lower=-100),
                )
            case self.WIND_U_COMPONENT_200m.name:
                return ParameterData(
                    name=str(self),
                    description="U component of wind at 200m above ground level. "
                                "Defined as the horizontal speed of "
                                "the wind in the eastward direction.",
                    units="m/s",
                    limits=ParameterLimits(upper=150, lower=-150),
                )
            case self.WIND_V_COMPONENT_200m.name:
                return ParameterData(
                    name=str(self),
                    description="V component of wind at 200m above ground level. "
                                "Defined as the horizontal speed of "
                                "the wind in the northward direction.",
                    units="m/s",
                    limits=ParameterLimits(upper=150, lower=-150),
                )
            case self.SNOW_DEPTH_GL.name:
                return ParameterData(
                    name=str(self),
                    description="Depth of snow on the ground.",
                    units="m",
                    limits=ParameterLimits(upper=12, lower=0),
                )
            case self.CLOUD_COVER_HIGH.name:
                return ParameterData(
                    name=str(self),
                    description="Fraction of grid square covered by high-level cloud. "
                                "Defined as the ratio of "
                                "the area of the grid square covered by high-level (>6km) cloud "
                                "to the square's total area.",
                    units="UI",
                    limits=ParameterLimits(upper=1, lower=0),
                )
            case self.CLOUD_COVER_MEDIUM.name:
                return ParameterData(
                    name=str(self),
                    description="Fraction of grid square covered by medium-level cloud. "
                                "Defined as the ratio of "
                                "the area of the grid square covered by medium-level (2-6km) cloud "
                                "to the square's total area.",
                    units="UI",
                    limits=ParameterLimits(upper=1, lower=0),
                )
            case self.CLOUD_COVER_LOW.name:
                return ParameterData(
                    name=str(self),
                    description="Fraction of grid square covered by low-level cloud. "
                                "Defined as the ratio of "
                                "the area of the grid square covered by low-level (<2km) cloud "
                                "to the square's total area.",
                    units="UI",
                    limits=ParameterLimits(upper=1, lower=0),
                )
            case self.CLOUD_COVER_TOTAL.name:
                return ParameterData(
                    name=str(self),
                    description="Fraction of grid square covered by any cloud. "
                                "Defined as the ratio of "
                                "the area of the grid square covered by any cloud "
                                "to the square's total area.",
                    units="UI",
                    limits=ParameterLimits(upper=1, lower=0),
                )
            case self.TOTAL_PRECIPITATION_RATE_GL.name:
                return ParameterData(
                    name=str(self),
                    description="Total precipitation rate at ground level. "
                                "Defined as the rate at which liquid is deposited on the ground "
                                "including rain, snow, and hail.",
                    units="kg/m^2/s",
                    limits=ParameterLimits(upper=0.2, lower=0),
                )
            case self.DOWNWARD_ULTRAVIOLET_RADIATION_FLUX_GL.name:
                return ParameterData(
                    name=str(self),
                    description="Downward ultraviolet radiation flux at ground level. "
                                "Defined as the mean amount of "
                                "ultraviolet radiation incident on the surface "
                                "expected over the next hour.",
                    units="W/m^2",
                    limits=ParameterLimits(upper=1000, lower=0),
                )
            case self.DIRECT_SHORTWAVE_RADIATION_FLUX_GL.name:
                return ParameterData(
                    name=str(self),
                    description="Direct shortwave radiation flux at ground level. "
                                "Defined as the mean amount of "
                                "unscattered solar radiation incident on"
                                "a surface plane perpendicular to the direction of the sun "
                                "expected over the next hour.",
                    units="W/m^2",
                    limits=ParameterLimits(upper=1000, lower=0),
                )
            case _:
                # Shouldn't happen thanks to the test case in test_parameters.py
                raise ValueError(f"Unknown parameter: {self}")